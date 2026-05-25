# Code review: zigparser2

Дата: 2026-05-25

Область review: текущий `zigtest2/src/*.zig` после обновлений.

Фокус: корректность записи, читаемость, упрощение кода и риски новых изменений.

Проверка сборки: `../../zig-x86_64-linux-0.17.0-dev.263+0add2dfc4/zig build` из `zigtest2/` прошел успешно.

## Что изменилось с прошлого review

Закрыто или частично закрыто:

- `saveBatch()` теперь возвращает `!void`, а `batchSendRows()` проверяет CQL response через `recvFrameCheck()`.
- Historical pipeline теперь сохраняет `save_error` и не двигает Redis cursor, если save thread вернул ошибку.
- Realtime stats вынесены в `RtStats`.
- Добавлен `FROM_BLOCK`, `RESERVE_RPC_URL`, catchup+realtime mode.
- Добавлен экспериментальный shard-aware connect через port `19042` и bind source port.

Остались важные проблемы:

- prepared IDs все еще готовятся только на `conns[0]`, но используются на всех connections;
- `had_error: bool` пишется из многих threads без atomics;
- encoding errors все еще silently skip rows;
- `httpPostZC()` все еще может неправильно пометить ownership при fallback allocation;
- новый shard-aware режим подключает все соединения к одному target shard, что полезно для эксперимента, но опасно как production default.

## Findings

### 1. Critical: prepared statement IDs по-прежнему per-pool, не per-connection

Файлы:

- `zigtest2/src/main.zig:76-80`
- `zigtest2/src/db.zig:773-782`
- `zigtest2/src/db.zig:1114-1119`

В `main.zig` prepared statements готовятся только на первом connection pool'а:

```zig
prep_ids[p] = try db.prepareAll(pools[p].conns[0]);
```

Но `saveBatch()` рассылает BATCH через разные `CqlConn`. Для CQL prepared IDs это риск `UNPREPARED` или некорректной работы при разных connections/shards.

Рекомендация:

- хранить `PreparedIds` рядом с каждым `CqlConn`;
- готовить all statements внутри `CqlPool.init()` для каждого connection;
- удалить отдельный массив `prep_ids` из `main/pipeline/realtime`;
- `spawnTable()` должен брать per-connection `prep_id`.

Это blocker перед дальнейшими latency-бенчмарками.

### 2. Critical: data race на `had_error`

Файлы:

- `zigtest2/src/db.zig:850-855`
- `zigtest2/src/db.zig:883`
- `zigtest2/src/db.zig:926`
- `zigtest2/src/db.zig:963`
- `zigtest2/src/db.zig:995`
- `zigtest2/src/db.zig:1030`
- `zigtest2/src/db.zig:1060`
- `zigtest2/src/db.zig:1110-1125`

`saveBatch()` создает обычный `bool`, а затем адрес этого bool передается во все write threads:

```zig
var had_error: bool = false;
```

Каждый worker может писать:

```zig
wa.had_error.* = true;
```

Это data race. Рекомендация: заменить на `std.atomic.Value(bool)` или `std.atomic.Value(u32)`.

### 3. High: `recvFrameCheck()` глушит read errors

Файл:

- `zigtest2/src/db.zig:816-846`

В функции проверки CQL ack есть успешные возвраты при ошибках чтения:

```zig
tcpReadExact(fd, &header) catch return;
tcpReadExact(fd, buf[0..read_len]) catch {};
tcpReadExact(fd, discard[0..n]) catch return;
```

Network failure может превратиться в successful write. Здесь нужны `try tcpReadExact(...)` во всех местах.

### 4. High: row encoding errors все еще quietly drop rows

Файлы:

- `zigtest2/src/db.zig:873-878`
- `zigtest2/src/db.zig:900-921`
- `zigtest2/src/db.zig:943-958`
- `zigtest2/src/db.zig:980-990`
- `zigtest2/src/db.zig:1012-1025`
- `zigtest2/src/db.zig:1047-1055`
- `zigtest2/src/db.zig:1164-1250`

В worker loop остались `catch continue`; в dump encoders остались `catch return`. Если encoding/allocation падает, строка пропадает без счетчика.

Рекомендация:

- единые row encoder функции должны возвращать `!void`;
- worker должен считать dropped rows;
- `saveBatch()` должен fail, если `dropped_rows > 0`;
- dump mode не должен писать частичный файл молча.

### 5. High: `httpPostZC()` ownership bug не исправлен

Файлы:

- `zigtest2/src/rpc.zig:247-255`
- `zigtest2/src/rpc.zig:446-455`

`httpPostZC()` при наличии `result_arena` может fallback'нуться на `gpa.alloc()`, но `flatFetch()` помечает ownership только по наличию `locked_arena`:

```zig
arg.out.from_arena = arg.locked_arena != null;
```

Если allocation реально произошел через `gpa`, buffer не освободится. Верните `HttpBody { data, from_arena }` или уберите fallback.

### 6. High: shard-aware mode сейчас маршрутизирует все connections в один shard

Файлы:

- `zigtest2/build.zig:13-25`
- `zigtest2/src/db.zig:715-728`

Новый режим подключает все pool connections к одному target shard:

```zig
const src: u16 = @intCast(40000 + i * num_shards + target);
const fd = try tcpConnectBound(host, 19042, src);
```

Для эксперимента это полезно. Но это не production shard-aware routing: для production нужно выбирать shard по token/partition key.

Дополнительный риск: `tcpConnectBound()` bind'ит local address на `127.0.0.1`, даже если Scylla host не localhost.

### 7. Medium: `CqlPool.init()` все еще leaks partially initialized connections

Файл:

- `zigtest2/src/db.zig:711-738`

Если соединение падает на середине pool creation, уже созданные `CqlConn` не освобождаются. Добавьте `errdefer pool.deinit()` и аккуратно обновляйте `pool.count` только после полной инициализации connection.

### 8. Medium: `runHistorical()` принимает `to`, но progress считает `cfg.to_block`

Файлы:

- `zigtest2/src/pipeline.zig:175-185`
- `zigtest2/src/pipeline.zig:249-252`
- `zigtest2/src/pipeline.zig:293-296`

`runHistorical()` теперь принимает отдельный `to`, что хорошо для catchup до `ws_first - 1`. Но `finishPrev()` вызывается с `cfg.to_block`, не с локальным `to`.

Рекомендация:

```zig
try finishPrev(..., to, &blocks_done);
```

### 9. Medium: retry reserve RPC ограничен 64 блоками

Файл:

- `zigtest2/src/pipeline.zig:91-111`

Retry buffer фиксирован:

```zig
var retry_buf: [64]u64 = undefined;
```

Если failed blocks больше 64, остальные не попадут в retry. После этого они логируются как skipped, но batch может считаться обработанным.

Рекомендация: выделять retry buffer динамически или fail-fast при переполнении.

### 10. Medium: catchup+realtime может пропускать gaps между cursor и WS notification

Файлы:

- `zigtest2/src/realtime.zig:365-394`
- `zigtest2/src/realtime.zig:397-402`

WS newHeads event должен быть trigger/head, а не единственный номер для обработки. Если listener получит block `N+2`, код не добирает `N+1` последовательно.

Рекомендация:

- realtime phase ведет `next = cursor + 1`;
- при notification `head` обрабатывает все `next..head`;
- не перескакивает через номер без явного режима skip.

### 11. Medium: `db.zig` стал еще больше и смешивает больше ответственностей

Файл:

- `zigtest2/src/db.zig`, сейчас 1377 строк

В одном файле: TCP bind/connect, CQL OPTIONS parser, CQL protocol, value encoders, workers, dump format и Redis. После correctness fixes стоит разделить:

- `net.zig`
- `cql_protocol.zig`
- `cql_values.zig`
- `writer.zig`
- `dump.zig`
- `redis.zig`

### 12. Low: `fetch_fn` все еще confusing для `fetch_mode=0`

Файлы:

- `zigtest2/src/config.zig:146-150`
- `zigtest2/src/pipeline.zig:86-89`

`fetch_mode=0` реально обрабатывается special-case в `pipeline.zig`, а `fetch_fn` при `else` указывает на `fetchBatch3`. Лучше заменить на enum switch в одном месте.

### 13. Low: `body_ok` в `fetchBlock()` все еще не используется

Файл:

- `zigtest2/src/rpc.zig:874-891`

`body_ok` выставляется, но дальше не читается. Можно удалить.

### 14. Low: TCP helpers все еще дублируются

Файлы:

- `zigtest2/src/db.zig:16-203`
- `zigtest2/src/rpc.zig:127-184`
- `zigtest2/src/ws.zig:9-60`

После добавления `tcpConnectBound()` дублирование стало еще заметнее. Это хороший low-risk cleanup после correctness fixes.

## Рекомендуемый порядок работ

### Этап 1: blocker correctness

1. Prepared IDs per connection.
2. `had_error` заменить на atomic.
3. `recvFrameCheck()` должен `try` read errors.
4. Encoding errors считать и превращать в failed save.
5. Не двигать Redis cursor, если в batch остались failed/skipped blocks после retry.

### Этап 2: realtime/catchup semantics

1. WS notification использовать как head trigger.
2. Обрабатывать все номера `cursor+1..head`.
3. В `runHistorical()` progress считать относительно local `to`.
4. Retry reserve RPC сделать без лимита 64 или fail-fast.

### Этап 3: shard-aware как эксперимент vs production feature

1. Переименовать fixed target shard mode в experimental option.
2. Не bind'ить всегда на `127.0.0.1`.
3. Для production shard-aware добавить token/shard routing.

### Этап 4: readability refactor

1. Вынести `net.zig`.
2. Вынести Redis из `db.zig`.
3. Объединить live/dump row encoders.
4. Разделить `db.zig` на protocol/values/writer/dump.
5. Убрать `fetch_fn` ambiguity и `body_ok`.

## Главный вывод

Код стал лучше с точки зрения observability, но write path пока нельзя считать полностью надежным: prepared IDs per connection, atomic/error accounting и корректная обработка read/encoding errors нужны до новых performance conclusions.

Если выбирать один следующий patch: сделать `CqlPool` владельцем `{conn, prep_ids}` per connection и одновременно заменить `had_error` на atomic.
