# SCYLLA.md — Подход к записи данных в ScyllaDB

## Обзор

Индексер пишет EVM-данные в ScyllaDB через **нативный CQL v4 binary protocol**,
реализованный без сторонних библиотек — только raw Linux syscalls. Основная идея:
сгруппировать как можно больше строк в один CQL round-trip, используя UNLOGGED BATCH,
и распараллелить записи по таблицам через пул персистентных потоков.

---

## Транспортный слой

### Raw TCP, никакого драйвера

```zig
// db.zig — всё IO через linux syscalls напрямую
linux.socket()  → linux.connect() → linux.write() / linux.read()
```

Нет зависимостей от `cassandra-driver`, `gocql`, `cql-cpp` и им подобных.
Это устраняет несколько классов накладных расходов:

- Нет парсинга `CassandraOptions` / конфигурации драйвера при старте
- Нет внутренних горутин/потоков драйвера, конкурирующих за CPU
- Нет абстракций `Session → Cluster → Host → Connection` — только fd

### TCP_NODELAY — критично для CQL latency

```zig
const nodelay: c_int = 1;
_ = linux.setsockopt(fd, linux.IPPROTO.TCP, linux.TCP.NODELAY, ...);
```

Без `TCP_NODELAY` ядро буферизует мелкие CQL-фреймы и ждёт заполнения MSS или
истечения таймера Nagle (~40ms на loopback). С `TCP_NODELAY` каждый фрейм уходит
немедленно — критично для latency-чувствительных маленьких запросов.

### writev для EXECUTE фреймов

```zig
// Два буфера (заголовок + тело) уходят за один syscall
fn tcpWritev(fd: i32, a: []const u8, b: []const u8) !void {
    var iov = [2]std.posix.iovec_const{ ... };
    linux.writev(fd, @ptrCast(&iov), 2);
}
```

Вместо двух `write()` — один `writev()`. Экономит один syscall per EXECUTE.

---

## Протокол CQL v4

Реализован вручную. Используемые opcodes:

| Opcode | Hex | Назначение |
|--------|-----|-----------|
| STARTUP | 0x01 | Handshake, negotiate `CQL_VERSION=3.0.0` |
| AUTH_RESPONSE | 0x0F | SASL plain: `\x00user\x00pass` |
| QUERY | 0x07 | `USE keyspace`, COUNT-запросы в verify |
| PREPARE | 0x09 | Компиляция INSERT-запроса в prepared statement |
| EXECUTE | 0x0A | Одиночный INSERT (используется только в shard-aware тестах) |
| BATCH | 0x0D | N строк за один round-trip ← **основной путь записи** |

Consistency level: **ONE** для всех операций записи. Replication factor=1 (single DC),
поэтому QUORUM = ONE в любом случае.

### Prepared statements

Все 7 INSERT запросов компилируются в prepared statements **один раз при инициализации**
каждого соединения:

```zig
fn prepareAll(conn: *CqlConn) !PreparedIds {
    return .{
        .blocks            = try conn.prepare(INSERT_BLOCKS),            // 5 полей
        .transactions      = try conn.prepare(INSERT_TXS),               // 21 поле
        .logs              = try conn.prepare(INSERT_LOGS),              // 15 полей
        .internal_txs      = try conn.prepare(INSERT_INT_TXS),           // 10 полей
        .contracts         = try conn.prepare(INSERT_CONTRACTS),         // 13 полей
        .contracts_by_addr = try conn.prepare(INSERT_CONTRACTS_BY_ADDR), // 8 полей
        .block_completions = try conn.prepare(INSERT_BLOCK_COMPLETIONS), // 6 полей
    };
}
```

Каждый prepared statement Scylla возвращает уникальный `prep_id` (16 байт).
В BATCH-фрейме каждая строка ссылается на `prep_id` вместо полного текста запроса —
экономия на парсинге и планировании запросов на стороне Scylla.

---

## UNLOGGED BATCH — ключевая оптимизация

### Почему не individual EXECUTE

TS1 (TypeScript-индексер) использует `cassandra-driver.execute()` на каждую строку:

```
1 блок ≈ 2800 строк
insertMany бьёт по 100 строк → 28 sequential Promise.allSettled
Latency: 28 × RTT_Scylla ≈ 28 × 5ms = 140ms
```

Zigparser2 с UNLOGGED BATCH:
```
100 строк → 1 BATCH frame → 1 RTT
2800 строк / 100 = 28 фреймов, отправляемых параллельно через пул соединений
Latency: max(28 фреймов / n_conns) × RTT ≈ 1-2 RTT = 5-10ms
```

**Результат: 140ms → 8-10ms save per block = 15× ускорение.**

### Формат BATCH фрейма

```
BATCH body:
  [byte]  type = 0x01 (UNLOGGED)
  [short] n_statements

  for each row:
    [byte]   kind = 0x01 (PREPARED)
    [short bytes] prep_id          ← 16 bytes
    [short]  n_values
    [bytes]  values...             ← pre-encoded binary

  [short] consistency = ONE (0x0001)
  [byte]  flags = 0x00
```

Один BATCH frame для N строк = один TCP round-trip.

### Почему UNLOGGED, а не LOGGED BATCH

- **LOGGED BATCH** гарантирует атомарность через batchlog (дополнительные записи в системную таблицу).
  Это ~2× overhead по сравнению с обычными записями.
- **UNLOGGED BATCH** — только оптимизация транспортного уровня (один round-trip), без гарантии атомарности.
  Нам атомарность не нужна — консистентность обеспечивается через `block_completions` (см. CONSISTENCY.md).

---

## Encoding строк (zero-alloc hot path)

### Буфер на воркер

Каждый воркер при старте выделяет **два ArrayList**:

```zig
var v  = workerBuf(EST_TX_ROW);         // один ряд (512B типично)
var bd = workerBuf(BS_TXS * EST_TX_ROW); // батч (50 × 512B = 25KB)
```

- `v` — временный буфер для одной строки; сбрасывается через `v.items.len = 0` (O(1), без free/alloc)
- `bd` — контигуальный буфер батча; указатели `ptrs[i]` = `bd.items[starts[i]..starts[i+1]]`

`starts[]` — массив на стеке, хранит границы каждой строки в `bd`.

Выделение памяти происходит только один раз при инициализации воркера. Горячий путь (encode → accumulate → send) — **zero heap allocation**.

### Типы значений

Каждое значение в CQL: `[int32 size][bytes...]` или `[int32 -1]` для null.

| Тип CQL | Размер | Реализация |
|---------|--------|-----------|
| `bigint` | 4+8 = 12B | `valBigint` — прямая запись i64 big-endian |
| `int` | 4+4 = 8B | `valInt32` |
| `tinyint` | 4+1 = 5B | `valTinyint` |
| `text` | 4+len | `valText` (null для пустой строки); `valTextRequired` (для обязательных) |
| `boolean` | 4+1 = 5B | `valBool` |
| `varint` | 4+1-N | `valVarint` — hex string → big-endian two's complement; in-place, zero copy |
| `list<text>` | 4+4+sum | `valListText` — используется только для `logs.rest_topics` |
| null | 4 | `valNull` — `[int32 -1]` |

### varint — специальный случай

CQL varint = big-endian two's complement переменной длины. Значения (`transaction.value`)
приходят из RPC как hex-строки (`"0x1a2b3c..."`).

```
"0x1a2b3c" → [0x1a, 0x2b, 0x3c]   (MSB < 0x80 → без prefix)
"0xffa3"   → [0x00, 0xff, 0xa3]    (MSB >= 0x80 → добавить 0x00 для знака +)
```

Алгоритм работает **в-place в буфере `list`**:
1. `ensureTotalCapacity` под worst-case (4 + 1 + len/2)
2. Декодирование hex-nibbles напрямую в `list.items[out_start..]`
3. Trim leading zeros (copyForwards)
4. Если MSB >= 0x80 — shift right + prepend 0x00 (copyBackwards)
5. Запись 4-байтового префикса длины

Нет промежуточного аллоцирования, нет копий — raw bytes пишутся сразу в финальный буфер.

---

## Пул соединений и разбиение по таблицам (split)

### CqlPool

```zig
pub const POOL_SIZE: u32 = cfg.pool_size;  // compile-time, e.g. 64

pub const CqlPool = struct {
    conns: []*CqlConn,   // POOL_SIZE соединений
    next_idx: std.atomic.Value(usize),  // для round-robin acquire()

    pub fn acquire(self: *CqlPool) *CqlConn {
        const idx = self.next_idx.fetchAdd(1, .monotonic) % self.count;
        return self.conns[idx];
    }
};
```

Каждое соединение — отдельный TCP-сокет, отдельный prepared statement id set.
`acquire()` — lock-free round-robin.

### SPLIT — распределение соединений по таблицам

```zig
// build option: -Dsplit=1,6,12,40,3,2
pub const SPLIT: [6]u32 = [1, 6, 12, 40, 3, 2];
// cumulative offsets:
const SPLIT_OFF: [7]u32 = [0, 1, 7, 19, 59, 62, 64];

// POOL_SIZE = sum(SPLIT) проверяется при компиляции
if (sum != POOL_SIZE) @compileError("split sum != pool_size");
```

Каждая таблица получает срез `conns[SPLIT_OFF[i]..SPLIT_OFF[i+1]]`:

| Таблица | Соединений | Строк/блок | Строк/conn |
|---------|-----------|-----------|-----------|
| blocks | 1 | ~1 | 1 |
| transactions | 6 | ~270 | 45 |
| logs | 12 | ~600 | 50 |
| **internal_txs** | **40** | **~1900** | **47** |
| contracts | 3 | ~20 | 7 |
| contracts_by_addr | 3 | ~20 | 7 |

Почему так много на `itxs`: это самая объёмная таблица. 40 соединений × 100 строк/батч = 4000 строк
параллельно за один round-trip. Без этого itxs становится bottleneck.

### Обоснование выбора split

Идеальный параллелизм: одно соединение обрабатывает один BATCH frame за один RTT.
При `BS_ITXS=100` строк/батч и ~1900 строках на блок:

```
1900 строк / 100 = 19 BATCH фреймов
При 40 соединениях: ceil(19/40) = 1 фрейм на соединение → максимальный параллелизм
```

Увеличение до 64+ соединений не ускорит — все фреймы уже отправляются за 1 round-trip.

---

## Per-table batch sizes (BS_*)

```zig
pub const BS_BLK:  usize = cfg.bs_blk;   // default 50, prod 1
pub const BS_TXS:  usize = cfg.bs_txs;   // default 50
pub const BS_LOGS: usize = cfg.bs_logs;  // default 50
pub const BS_ITXS: usize = cfg.bs_itxs;  // default 50, prod 100
pub const BS_CONT: usize = cfg.bs_cont;  // default 50, prod 10
pub const BS_CBA:  usize = cfg.bs_cba;   // default 50, prod 10
const BS_COMP: usize = 50; // block_completions, фиксированный
```

### Почему разные значения

**`BS_BLK=1`**: блок пишет 1 строку в таблицу blocks. BATCH из 1 строки = просто EXECUTE с меньшим overhead.
При `BS_BLK=50` воркер ждёт накопления 50 строк — это 50 блоков, нереалистично для realtime.

**`BS_ITXS=100`**: internal_txs — самые маленькие строки (~100 байт). 100 строк в одном BATCH frame
= ~10KB, хорошо укладывается в лимит 50KB. Увеличение с 50 вдвое снижает число round-trips.

**`BS_CONT=10`, `BS_CBA=10`**: contracts содержат `creation_bytecode` и `deployed_bytecode` (EVM bytecode).
Один контракт может занимать 10-100KB. При `BS_CONT=50` один BATCH frame легко превышает
50KB лимит Scylla → `code=0x2200 Batch too large` → ошибка.

### Лимит Scylla на размер BATCH

Scylla отклоняет BATCH фреймы > 50KB по умолчанию. Поэтому:

```
contracts: 50 × ~1KB avg = 50KB → на грани, иногда превышает
contracts: 10 × ~1KB avg = 10KB → безопасно
```

Для `logs.data` та же проблема: некоторые события Ethereum логируют большие объёмы данных.
`BS_LOGS=50` держим умеренным именно из-за этого.

---

## Шардирование: partition key

### Схема таблиц

Все data-таблицы (кроме `contracts_by_addresses`) используют составной primary key:

```sql
PRIMARY KEY ((chunk), block_number, ...)
```

`chunk` — partition key. Данные одного partition всегда на одном шарде Scylla.

### Два режима вычисления chunk

**Linear (default):** `chunk = block_number / RAW_CHUNK_SIZE` (default 1000)

```
block 25078000 → chunk 25078
block 25079000 → chunk 25079
1000 блоков → 2 горячих чанка → 2 горячих шарда
```

**Remap-mod:** `chunk = block_number % REMAP_MOD`

```
REMAP_MOD=24:
  block 25079000 → chunk 25079000 % 24 = 8
  block 25079001 → chunk 25079001 % 24 = 9
  ...каждые 24 блока покрывают все шарды 0..23
```

Remap обеспечивает равномерную нагрузку на **все шарды одновременно** при потоковой записи.
Без remap 1000 блоков подряд пишут только в 2 чанка → 2 шарда из N — остальные простаивают.

### Влияние на производительность (WSL2, smp=16, tmpfs)

| Режим | Горячих шардов | ms/block |
|-------|---------------|---------|
| Linear chunk_size=1000 | 2 | 15.2ms |
| **REMAP_MOD=16** | **16** | **8.75ms** |

В 1.7× быстрее — все 16 шардов равномерно принимают writes.

---

## Persistent WorkerPool (realtime-режим)

### Проблема: spawn per block

В историческом режиме `saveBatch` спаунит потоки под каждый блок:

```
saveBatch:
  spawnTable × 6 → clone() + stack mmap() + join()
  Overhead: ~1-5ms per block (syscall + TLB flush + stack allocation)
```

При скорости 18ms/block это 5-28% накладных расходов на одни thread lifecycle calls.

### Решение: WorkerPool с futex

```zig
pub const WorkerPool = struct {
    workers: []PWorkerCtx,          // POOL_SIZE контекстов
    threads: []std.Thread,          // POOL_SIZE OS-потоков (созданы один раз)
    pending: std.atomic.Value(u32), // барьер: сколько воркеров ещё работают
    had_error: std.atomic.Value(bool),
};
```

Воркеры **создаются при старте**, живут до завершения программы.
Dispatch через futex (`FUTEX_WAKE/WAIT`):

```
Worker state:  0=idle  1=work_ready  3=shutdown

Dispatch:
  pool.pending.store(n_total)    // сколько воркеров получат задание
  for each worker:
    ctx.task = task
    ctx.state.store(1, .release)
    FUTEX_WAKE(&ctx.state, 1)    // разбудить воркер

Barrier (main thread):
  while pending.load() > 0:
    FUTEX_WAIT(&pending, cur_val)  // спать до изменения

Worker done:
  ctx.state.store(0, .release)   // вернуться в idle
  if pending.fetchSub(1) == 1:   // если последний
    FUTEX_WAKE(&pool.pending, 1) // разбудить main
```

`std.Thread.Mutex` отсутствует в Zig 0.17.0-dev.263 → raw Linux futex.

### Результат

| Режим | avg | p95 | p99 |
|-------|-----|-----|-----|
| spawn per block | 21ms | 34ms | 43ms |
| **persistent workers** | **18ms** | **30ms** | **43ms** |

avg −3ms (−14%), p95 −4ms. p99 не улучшился — bottleneck сместился в Scylla flush.

---

## Shard-aware routing (экспериментально, H1)

Scylla поддерживает специальный порт **19042**, на котором маршрутизирует входящие
соединения на конкретный шард по формуле:

```
shard = source_port % num_shards
```

Реализовано через `tcpConnectBound(host, 19042, src_port)`, где
`src_port = 40000 + i * num_shards + target_shard`.

**Тест H1 показал: при single-partition workload shard-aware ухудшает результат** (+15ms save).

Причина: все 64 соединения попадают на один шард. Его seastar reactor (single-threaded)
вынужден поллить 64 FD + обрабатывать данные. В обычном режиме 64 соединения распределены
по всем шардам через TCP балансировщик ядра — I/O нагрузка распределена.

Shard-aware полезен только при **multi-partition workload** с правильным per-request routing
(одно соединение на шард, каждый запрос идёт на нужный шард).

---

## Итоги: откуда берётся выигрыш

### Сравнение с TS1 (TypeScript, cassandra-driver)

| Компонент | TS1 | zigparser2 | Δ |
|-----------|-----|-----------|---|
| Save path | individual `execute()` per row | UNLOGGED BATCH N rows | **15×** |
| Round-trips per block | 28 sequential (Promise.allSettled) | ceil(N/BS) parallel | **~30×** |
| CQL overhead | driver parse/validate per call | prepared stmt id only | ~3× |
| Queue overhead | BullMQ serialize → Redis → deserialize | нет | — |
| Save latency per block | ~128ms | ~8-10ms | **13-15×** |

### Цепочка оптимизаций по порядку внедрения

| Этап | Что сделано | Save latency | Δ |
|------|------------|-------------|---|
| Baseline (pool=32) | UNLOGGED BATCH, BATCH_SIZE=50 | ~20ms | — |
| remap-mod=32 | Все шарды активны | ~17ms | −3ms |
| pool=48, split retune | Больше параллелизма на itxs | ~17ms | −0ms |
| BATCH_SIZE=50→per-table | BS_ITXS=100, BS_CONT=10 | ~16ms | −1ms |
| persistent workers | Нет spawn overhead | ~15ms | −2ms |
| split S64 (pool=64) | 40 conn на itxs | ~15ms | 0 avg, −3ms p99 |
| **smp=24** | Больше памяти/шард, реже flush | **~15ms** | **−3ms p99** |

**Итог: ~20ms → ~15ms avg, p99 40ms → 31ms** (только Scylla-фаза, без fetch).

### Текущий bottleneck

Все оптимизации транспортного уровня исчерпаны. avg=15ms — это **Scylla memtable write latency**
на одном шарде (все тестовые блоки → один chunk → один шард).

В production с разными block ranges → разные chunks → разные шарды — запись параллелится
автоматически и avg будет ниже.

Дальнейшее снижение latency возможно через:
1. **Многошардовую нагрузку в тестах** — симулировать несколько block ranges одновременно
2. **smp sweep при реальной prod-нагрузке** — smp=24 оптимален для realtime; для historical может быть выше
3. **Compression отключить** — LZ4 compression в Scylla добавляет CPU overhead на write path
