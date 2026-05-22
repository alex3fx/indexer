# zigparser2 — Архитектура и параллелизм

EVM-блокчейн парсер на Zig 0.17. Читает блоки с JSON-RPC ноды, трансформирует, пишет в ScyllaDB + Redis.

---

## Параллелизм: где и что

### 1. HTTP fetch — N×3 потока

На каждый батч из N блоков запускается `N * 3` потоков одновременно:
- поток `b*3+0`: `eth_getBlockByNumber(b)`
- поток `b*3+1`: `eth_getBlockReceipts(b)`
- поток `b*3+2`: `trace_block(b)`

Все блоки и все методы запрашиваются **одновременно**. Для batch=16 → 48 параллельных HTTP запросов.

```
block 0: getBlock ─────────────────┐
block 0: getReceipts ──────────────┤
block 0: trace_block ──────────────┤  (все 48 в параллель)
block 1: getBlock ─────────────────┤
...                                 │
block 15: trace_block ─────────────┘  → join → parse
```

**Файл:** `rpc.zig::fetchBatchFlat()`

---

### 2. JSON parse — 3 потока

После завершения HTTP потоков — 3 параллельных parse потока:
- поток 0: парсит все block-ответы → `results[*].block`
- поток 1: парсит все receipts-ответы → `results[*].receipts`
- поток 2: парсит все traces-ответы → `results[*].traces`

Каждый поток работает со своей `method_arena` — нет синхронизации.

```
HTTP done → spawn parse_thread[0..2] → join → data ready
```

Важно для **realtime**: `fetchBlock()` тоже запускает 3 параллельных parse потока.  
Без этого parse шёл последовательно (~1.7ms vs ~1.0ms параллельно).

**Файл:** `rpc.zig::parseMethodThread()`, `fetchBatchFlat()`, `fetchBlock()`

---

### 3. CQL save — pool_size=32 потоков

`saveBatch()` разбивает 6 таблиц на группы соединений и запускает все параллельно:

| Таблица | Соединений | ~Строк/блок | Фреймов/соединение |
|---------|-----------|-------------|-------------------|
| blocks | 1 | 1 | 1 |
| transactions | 3 | ~270 | ~1 |
| logs | 6 | ~600 | ~1 |
| **internal_transactions** | **20** | **~1900** | **~1** |
| contracts | 1 | ~20 | 1 |
| contracts_by_addresses | 1 | ~20 | 1 |

Оптимум: **~100 строк = 1 UNLOGGED BATCH фрейм на соединение**.  
Все 32 потока работают параллельно → стены-clock save ≈ 1 фрейм/шард.

```
saveBatch → spawn 32 workers → each sends 1 UNLOGGED BATCH → join
             (6 tables × pool split)
```

**Файл:** `db.zig::saveBatch()`, `spawnTable()`

---

### 4. PIPELINE=N — N fetch-воркеров

В историческом режиме N батчей обрабатываются параллельно:

```
PIPELINE=2:
Round k:  [fetch(b0,b1)] ──parallel── join → [save(b0)] [save(b1)]
                                        ↕ overlap ↕
Round k+1:              [fetch(b2,b3)] ──parallel── join → [save(b2)] [save(b3)]
```

Save раунда k перекрывается с fetch раунда k+1.

**Файл:** `pipeline.zig::runHistorical()`

---

### 5. Realtime WS — слушатель в отдельном потоке

В WS realtime режиме WS listener и processBlock разделены:

```
Thread A (WS listener):
  while: conn.nextBlockNum() → write(pipe, block_num)

Thread B (main, critical path):
  while: read(pipe) → t0=now() → fetchBlock → transform → saveBatch → redis
```

**Зачем:** WS listener блокируется на чтении TCP-фрейма. Это не мешает main-потоку начать fetch немедленно после пробуждения из `read(pipe)`.

**Pipe** (OS pipe, не кольцевой буфер) обеспечивает нулевой CPU spin при ожидании.

**Файл:** `realtime.zig::wsListenerThread()`, `runRealtimeWs()`

---

## Режимы работы

| Режим | Активация | Описание |
|-------|-----------|---------|
| Historical | по умолчанию | PIPELINE=N батчей параллельно |
| Realtime polling | `REALTIME=1` | 1 блок за раз, опрос каждые POLL_MS |
| Realtime WS | `REALTIME=1 WS_URL=ws://...` | WS trigger → fetch → save |
| Dump | `DUMP_FILE=path` | пишет в файл вместо Scylla |

---

## Critical path (latency от запроса до DB write)

```
t0 = now()
    │
    ├── [spawn 3 HTTP threads] ──────────────────── 1.4ms (parallel)
    │   eth_getBlockByNumber
    │   eth_getBlockReceipts
    │   trace_block
    │
    ├── [spawn 3 parse threads] ─────────────────── 1.0ms (parallel)
    │   parseBlockRespZC
    │   parseReceiptsRespZC
    │   parseTracesRespZC
    │
    ├── [transform] ─────────────────────────────── 0.3ms (single)
    │   transformBlock (chunk = block % remap_mod)
    │
    └── [spawn 32 CQL workers] ──────────────────── 8-9ms (parallel)
        UNLOGGED BATCH × 6 tables

total = 11-12ms
```

**Файл:** `realtime.zig::processBlock()`

---

## Измеряемые метрики

| Метрика | Что измеряет | Включает |
|---------|-------------|---------|
| `fetch_ms` | HTTP запросы | HTTP только (не parse) |
| `transform_ms` | transform | `transformBlock` call |
| `save_ms` | CQL write | spawn workers + join |
| `total_ms` | wall-clock | fetch + parse + transform + save + overhead |

> `total_ms > fetch_ms + transform_ms + save_ms` на ~1ms  
> Разница = JSON parse (внутри fetchBlock, не включён в `fetch_ms`)

---

## Конфигурация сборки

```bash
ZIG=.../zig-x86_64-linux-0.17.0-dev.263+0add2dfc4/zig
$ZIG build -Doptimize=ReleaseFast \
           -Dpool_size=32 \        # CQL connections (total)
           -Dsplit="1,3,6,20,1,1"  # connections per table
```

`SPLIT` = компайл-тайм параметр, задаёт как pool_size делится по таблицам.  
Оптимум для ~3000 строк/блок: `1,3,6,20,1,1` (= ~1 BATCH фрейм на соединение).

---

## Переменные окружения

| Переменная | По умолчанию | Описание |
|-----------|------------|---------|
| `REMAP_MOD=N` | 0 | chunk = block_number % N (N = smp для оптимума) |
| `PIPELINE=N` | 1 | N параллельных fetch+save воркеров (historical) |
| `BATCH_SIZE=N` | 10 | Блоков в батче |
| `REALTIME=1` | 0 | Realtime режим |
| `POLL_MS=N` | 500 | Интервал опроса (только polling режим) |
| `WS_URL=ws://...` | "" | WebSocket URL; активирует WS режим |
| `TO_BLOCK=N` | 25079196 | Последний блок |
| `DUMP_FILE=path` | "" | Dump в файл вместо Scylla |

---

## Результаты (smp=16, tmpfs, REMAP_MOD=16)

| Режим | Конфиг | ms/block |
|-------|--------|---------|
| Historical | PIPELINE=2, BATCH=16 | **5.58ms** |
| Realtime WS | POOL=32, SPLIT=1,3,6,20,1,1 | **11.5–11.9ms** |
| TS1 historical | 5 workers, BullMQ | 27.85ms |
| TS1 realtime | BATCH_SIZE=1 | 138.7ms |
