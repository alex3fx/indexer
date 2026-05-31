# Обзор изменений относительно origin/master

Дата: 2026-05-29  
Коммиты: `3296d3c` → `7ef2ec9` (9 коммитов, из которых 6 затрагивают код)  
Затронутые файлы: `build.zig`, `src/db.zig`, `src/realtime.zig`, `src/rpc.zig`, `src/ws.zig`

---

## 1. build.zig — новые дефолты batch-размеров

**Суть:** Увеличены дефолтные batch-размеры для четырёх таблиц.

| Параметр | Было | Стало |
|----------|------|-------|
| `bs_blk`  | 50 | 10 |
| `bs_txs`  | 50 | 100 |
| `bs_logs` | 50 | 200 |
| `bs_itxs` | 50 | 500 |

`bs_blk` уменьшен (1 блок в батч → меньше пустых строк),  
`bs_txs/logs/itxs` увеличены — меньше CQL BATCH-фреймов на воркер при крупных блоках,  
что при async-пайплайне (см. db.zig) снижает суммарное время ответов.

Параметры `bs_cont`, `bs_cba` остались 50 (не изменились).

---

## 2. src/ws.zig — обработка фрагментированных WS-сообщений

**Проблема:** RFC 6455 разрешает разбивать сообщение на несколько фреймов (FIN=0 + CONTINUATION). Старый код их игнорировал и мог пропустить блок.

**Изменения:**

- `wsRecv` теперь возвращает поле `fin: bool` (бит FIN из первого байта заголовка).
- `nextBlockNum` буферизует фрагменты через `frag: ArrayList(u8)`:
  - `TEXT` frame с `FIN=0` → начало фрагментации, копируем в `frag`.
  - `CONT` frame с `FIN=0` → продолжаем копировать.
  - `CONT` frame с `FIN=1` → финальный фрагмент, разбираем собранное сообщение.
  - `TEXT` frame с `FIN=1` → полное сообщение, разбираем напрямую (fast path, без аллокации).
- Добавлена константа `WS_OPCODE_CONT = 0x0`.

**Корректность:** `frag` живёт на стеке `nextBlockNum`, освобождается через `defer`.  
Пинги обрабатываются до проверки FIN, не нарушая логику сборки фрагментов.

---

## 3. src/rpc.zig — RpcScheduler, chunked encoding, NullOrErrorTraces

### 3.1 RpcScheduler — глобальный rate-limit + in-flight throttle

**Проблема:** При перегрузке ноды несколько потоков одновременно получали 429 и сразу повторяли запросы — thundering herd.

**Решение:** Новый `pub const RpcScheduler` с атомарными полями:

```
in_flight[3]        — счётчик активных запросов per-method
backoff_until_ns[3] — глобальное время окончания backoff per-method
```

- `MAX_IN_FLIGHT = {32, 32, 10}` — для `trace_block` (метод 2) вдвое меньше слотов, т.к. запросы в 3–10× тяжелее.
- `waitSlot(method)` — ждёт пока: (а) истёк global backoff, (б) есть свободный in-flight слот.
- `releaseSlot(method)` — декремент после получения ответа.
- `setBackoff(method, delay_ms)` — атомарный CAS: поднимает `backoff_until_ns` только вверх.
- `pub var global_rpc_scheduler` — единственный экземпляр на процесс, шарится всеми fetch-потоками.

`MAX_RETRIES = 4`, backoff начинается с 250ms, удваивается до 8000ms.

### 3.2 Chunked Transfer Encoding

**Проблема:** Некоторые ноды отвечают с `Transfer-Encoding: chunked`, а не с `Content-Length`. Старый код не декодировал chunked → `receipts`/`traces` не парсились на mainnet.

**Новая функция `decodeChunked(data []u8) !usize`:**
- Декодирует in-place: `<hex-size>\r\n<data>\r\n ... 0\r\n\r\n`.
- Возвращает итоговую длину, данные сжимаются в начало буфера.
- Вызывается в `httpPostZC` когда заголовок содержит `transfer-encoding: chunked`.
- Аналогичная проверка добавлена в `httpPost` (для polling-режима).

**Детект статусов до аллокации body:**  
В обоих `httpPostZC` и `httpPost` добавлена проверка HTTP status до выделения буфера под body:
```
HTTP status 429 / 502 / 503 → return error.RateLimit
```
Это позволяет `RpcScheduler.setBackoff` среагировать до траты памяти.

### 3.3 flatFetch — retry loop + NullOrErrorTraces

**Было:** `flatFetch` делал один запрос, при ошибке сразу ставил `out.failed = true`.

**Стало:** Цикл `while (attempt <= MAX_RETRIES)` с:
- Экспоненциальным backoff между попытками.
- `waitSlot` / `releaseSlot` вокруг каждого HTTP-запроса.
- Параметр `method: u2` в `FlatFetchArg` — передаётся в scheduler.

**Проверка NullOrErrorTraces (только для `method == 2`):**  

Старая ошибка: `"result":null` встречается внутри trace-записей для reverted calls  
(`{"type":"call","result":null}`). Поиск по всему телу давал ложные срабатывания.

Новая логика — проверяем только верхний уровень:
```zig
const key = "\"result\":";
const idx = indexOf(data, key);
const after = trimStart(data[idx + key.len..], " ");
// Валидный trace_block всегда начинается с '['; всё остальное — ошибка
break :blk after.len == 0 or after[0] != '[';
```

Проверка на JSON-ошибку (`"error":{`) осталась без изменений.

**Управление ареной в retry:** `locked_arena` передаётся при каждой попытке — ZC-срезы должны оставаться валидными до `method_arena.deinit()`. Перед каждым повтором вызывается `la.resetLocked()` (новый метод `LockedArena`): захватывает спинлок, вызывает `arena.reset(.retain_capacity)`, освобождает. Это предотвращает накопление multi-MB буферов неудавшихся попыток в арене.

```zig
fn resetLocked(self: *LockedArena) void {
    self.lock();
    defer self.unlock();
    _ = self.arena.reset(.retain_capacity);
}
```

В `flatFetch` при `attempt > 0`:
```zig
if (arg.locked_arena) |la| la.resetLocked();
```

Ошибки 429 перехватываются до аллокации body, поэтому arena не засоряется при rate-limit ответах.

---

## 4. src/db.zig — async CQL pipeline + WorkerPool split + метрики

### 4.1 batchSendRowsNoWait — отправка без ожидания

**Новая private-функция на `CqlConn`:**
```zig
fn batchSendRowsNoWait(prep_id, n_vals, row_bufs, stream: u16) !void
```
Строит BATCH-фрейм идентично `batchSendRows`, но вызывает `sendFrameStream` с заданным stream-ID вместо `sendFrame`. Ответ не читается — вызывающий код собирает все ответы позже через `recvFrameCheck`.

### 4.2 Async CQL pipeline в pTxsWork / pLogsWork / pItxsWork

**Было:** каждый батч отправлялся и тут же ждал ответа (`batchSendRows = send + recv`).  
При N батчах — N последовательных RTT.

**Стало:** все батчи отправляются через `batchSendRowsNoWait` с stream-ID `1..n_sent`, затем в отдельном цикле собираются все ответы. Время = 1 RTT + кодирование всех батчей (вместо N × RTT).

```zig
var n_sent: u16 = 0;
while (i < rows.len) {
    // ... encode batch ...
    if (enc > 0) {
        n_sent += 1;
        batchSendRowsNoWait(..., n_sent) catch { had_error = true; return; };
    }
    i = end;
}
// Verify each response stream ID is in [1, n_sent] with no duplicates (u64 bitmask)
var seen: u64 = 0;
std.debug.assert(n_sent <= 64);
for (0..n_sent) |_| {
    const sid = recvBatchStream(conn.fd) catch { had_error = true; return; };
    if (sid < 1 or sid > n_sent) { had_error = true; return; }
    const bit = @as(u64, 1) << @intCast(sid - 1);
    if (seen & bit != 0) { had_error = true; return; }
    seen |= bit;
}
```

**`recvBatchStream`** — новая функция (заменяет `recvFrameCheck` в pipeline-воркерах): читает 9-байтовый CQL-заголовок и возвращает stream-ID из байт `[2..4]`. Тело кадра дочитывается и дискардится аналогично `recvFrameCheck`. Если stream-ID вне диапазона `[1, n_sent]` или уже встречался в этом батче — `had_error = true`, воркер завершается немедленно.

### 4.3 SaveTableMs + finish_ns — per-table метрики

Новая структура:
```zig
pub const SaveTableMs = struct {
    blk, txs, logs, itxs, cont, cba, comp: f64 = 0,
};
```

Каждый `PWorkerCtx` получил поле `finish_ns: i64 = 0`, которое устанавливается в `pWorkerLoop` после завершения работы (до decrement `pending`).

`tableMs(workers, t0)` — возвращает `max(finish_ns) - t0` по всем воркерам таблицы.

### 4.4 WorkerPool: dispatch/await split

**saveBatch разбит на два вызова:**

`saveBatchDispatch(ent)` — запускает воркеров и возвращает немедленно:
- Сохраняет `dispatch_nc`, `dispatch_t0`, `dispatch_ent` в полях WorkerPool.
- Обнуляет `finish_ns` у запускаемых воркеров.
- Устанавливает `pending` атомик и отправляет задачи через `pSend`.

`saveBatchAwait(result_ms, tbl_ms)` — ждёт завершения:
- Futex-wait пока `pending > 0`.
- Вычисляет `tbl_ms.*` через `tableMs`.
- Вызывает `writeBlockCompletions`.
- `result_ms = max(tbl_ms.blk..cba) + tbl_ms.comp` — **не** `nowNs()-dispatch_t0`, т.к. в pipeline await вызывается через ~250ms после dispatch (drip-feed интервал), что создавало артефакт.
- Сбрасывает `dispatch_ent = null`.

`saveBatch` — обёртка: dispatch + await (для нон-pipeline кода).

**Защита от двойного dispatch:** `saveBatchDispatch` начинается с `std.debug.assert(self.dispatch_ent == null)` — в debug-сборке сразу паникует если предыдущий dispatch не был awaited. В release-сборке assert убирается компилятором, поэтому защита работает только на этапе разработки.

**Дополнительные поля на WorkerPool:**
```zig
dispatch_nc:  [6]u32 = .{0, ...}
dispatch_t0:  i64    = 0
dispatch_ent: ?*Entities = null
```

---

## 5. src/realtime.zig — двухслотовый pipeline

### 5.1 BlockSlot — изолированный контейнер блока

```zig
pub const BlockSlot = struct {
    method_arenas: [3]ArenaAllocator,
    block_arena:   ArenaAllocator,
    ent:           transform.Entities,
    save_ms:       f64,
    tbl_ms:        SaveTableMs,
    // init() / deinit() / reset()
};
```
Два слота чередуются: слот N занят save-воркерами, пока в слоте N^1 выполняется fetch следующего блока.

### 5.2 fetchTransformBlock — только fetch+transform

```zig
pub fn fetchTransformBlock(io, gpa, cfg, block_num, slot, fetch_ms_out) !?f64
```
Сбрасывает арены слота, делает RPC-запрос, трансформирует. Возвращает `transform_ms` или `null` если блок недоступен. Save не вызывается — это задача вызывающего кода.

### 5.3 PipeState — стыковка итераций

```zig
const PipeState = struct {
    slot_idx, block_num: ...,
    fetch_ms, transform_ms: f64,
    t_arrival: i64,
    dispatched: bool,
};
```

### 5.4 finishSave — await + печать + Redis

```zig
fn finishSave(wpool, slot, block_num, fetch_ms, transform_ms, t_arrival, stats, cursor, redis) !void
```
- Вызывает `saveBatchAwait` (блокирует только если воркеры ещё не закончили).
- Считает `total_ms = nowNs() - t_arrival` (время от прихода блока по WS до записи в Scylla).
- Печатает строку `⚡ [N] fetch=…ms save=…ms total=…ms ...`.
- Обновляет `cursor` и `LATEST_PROCESSED_BLOCK_NUMBER` в Redis.

### 5.5 Pipeline в runRealtimeWs и runCatchupAndRealtime

Основной цикл для каждого из двух функций:
```
[блок N прибыл]
  t_arrival = nowNs()
  fetchTransformBlock(N, cur_slot)   ← перекрывается с save(N-1)
  finishSave(prev)                   ← ждёт save(N-1), лишь если он ещё не завершён
  saveBatchDispatch(cur_slot.ent)    ← запускает save(N) в фоне
  prev = { slot_idx, block_num, ... }
  slot_idx ^= 1
```

После выхода из цикла — `finishSave(prev)` для последнего блока.

**runCatchupAndRealtime:** `ws_first` (первый блок, полученный до подписки) подаётся через `next_block: ?u64`, чтобы не теряться при переходе с channel.recv() на него.

### 5.6 RtStats — расширение метрик

`BlockResult` получил поле `tbl: SaveTableMs`.  
`RtStats` добавил `sum_*/max_*` для каждой из 7 таблиц.  
Итоговый print выводит строку `save breakdown (avg/max ms): blk=…/… txs=…/… ...`.

**processBlock** (polling-режим) также переведён на `BlockSlot` + `saveBatch` с `tbl_ms`.

---

## Итоговые бенчмарки

### Localhost WSL2 (после фиксов, pool=48, catchup+realtime)

Localhost, WS pipeline, 16 блоков (коммит `7ef2ec9`, pool=48 split=1,4,8,30,3,2):

| метрика | avg | min | max |
|---------|-----|-----|-----|
| fetch   | 6.3ms | 3.3ms | 24.1ms |
| **save**| **10.9ms** | 6.9ms | 33.4ms |
| **total**| **22.9ms** | 16.1ms | 45.1ms |

save breakdown (avg/max): blk=2.8/19 txs=9.2/33 logs=10.1/33 itxs=10.0/33 cont=7.3/22 cba=4.6/29 comp=0.7/1.3

### Сервер lotos-archive-01 (smp=32, drip-feed 250ms, 100 блоков)

| Config | pool/split | batch | fetch avg | **save avg** | throughput |
|--------|-----------|-------|-----------|--------------|------------|
| W (baseline, sequential) | 48 / 1,4,8,30,3,2 | itxs=100 | ~8ms | ~9ms | **18ms** |
| **ABC (pipeline)** | 64 / 1,6,12,40,3,2 | itxs=500 logs=200 | 8.5ms | **14.6ms** | **14.6ms** |
| ABC2 (меньше batch) | 64 / 1,6,12,40,3,2 | itxs=100 logs=50 | 8.6ms | 16.7ms | 16.7ms |

**Вывод:** Pipeline даёт −3.4ms/блок (−19%) vs W за счёт overlap save(N) ∥ fetch(N+1).  
Крупные batch (itxs=500, logs=200) лучше мелких: меньше CQL-фреймов → меньше суммарной обработки Scylla, несмотря на больший размер каждого фрейма.

Метрика `total` в drip-feed тесте включает задержку ожидания следующего блока (~250ms). Реальную задержку показывает последний блок каждого прогона (`total ≈ 30–32ms`).

---

## Ранее выявленные риски (устранены в коммите `7ef2ec9`)

1. **batchSendRowsNoWait + stream ID** → **исправлено.**  
   Новая функция `recvBatchStream` возвращает stream-ID из заголовка CQL-ответа. Все три pipeline-воркера (`pTxsWork`, `pLogsWork`, `pItxsWork`) проверяют каждый stream-ID: должен быть в диапазоне `[1, n_sent]` и встречаться ровно один раз (u64 bitmask). Любое нарушение → `had_error = true`, немедленный выход.

2. **Двойной dispatch без await** → **исправлено.**  
   `saveBatchDispatch` начинается с `std.debug.assert(self.dispatch_ent == null)`. В debug-сборке двойной вызов вызывает панику немедленно; в release-сборке assert убирается, но логика realtime.zig корректна по конструкции (слот переключается только после dispatch, await старого слота вызывается до следующего dispatch того же слота).

3. **Накопление arena при retry в flatFetch** → **исправлено.**  
   `LockedArena` получил метод `resetLocked()`. В `flatFetch` при `attempt > 0` арена сбрасывается перед каждым повтором — буфер неудавшейся попытки освобождается до следующего HTTP-запроса.
