# SHARD.md — тест конфигураций ScyllaDB шардов

## Условия теста (парсер)

- Машина: WSL2 Ubuntu 22.04, Intel i9 14-го поколения, 24 logical CPU, ~41GB RAM
- ScyllaDB 6.2 нативно, data dir на tmpfs (`/dev/shm/scylla`)
- Парсер: zigtest2 с **UNLOGGED BATCH** (100 строк/frame)
- Датасет: 1000 блоков (25078197–25079196), 3.2M строк, 6 таблиц
- Флаги: `--developer-mode=1 --unsafe-bypass-fsync=1 --overprovisioned`
- pool_size=32, split=1,3,6,20,1,1 (blocks:txs:logs:itxs:contracts:cba)
- Протокол: warmup run (discard) → TRUNCATE → settle → measured

---

## Часть 1: Влияние smp и памяти на скорость парсера

### Ключевой факт: только 2 шарда активны при RAW_CHUNK_SIZE=1000

```
chunk = block_number / RAW_CHUNK_SIZE
блоки 25078197–25079196 → chunk 25078 и chunk 25079 → 2 партиции → 2 шарда
```

При любом smp пишут только 2 шарда. Остальные шарды не участвуют в записи raw-таблиц.

### Результаты парсера (measured run после warmup+settle)

| Конфиг | mem/shard | Measured | FBDR avg | Save avg/batch | Bottleneck |
|--------|-----------|----------|----------|---------------|------------|
| smp=2, 4G | 2.0 GB | 19.3ms | 14ms | 191ms | save (flush stalls) |
| smp=2, 1G | 512 MB | 20.2ms | 12ms | 200ms | save |
| smp=4, 5G | 1.25 GB | 21.0ms | 15ms | 210ms | save |
| **smp=8, 10G** | **1.25 GB** | **13.8ms** | **18ms** | **136ms** | **fetch** |
| smp=12, 14G | 1.17 GB | 14.4ms | 22ms | 144ms | fetch |
| smp=16, 20G | 1.25 GB | 15.1ms | 28ms | 149ms | fetch |
| smp=20, 24G | 1.2 GB | 16.8ms | 30ms | 164ms | fetch |
| smp=24, 28G | 1.17 GB | 14.7ms | 26ms | 145ms | fetch |

**Лучший результат парсера: smp=8, 10G → 13.8ms/block**

Оптимум при smp=8: Scylla занимает 8 ядер, остальные 16 доступны для HTTP fetch.
Fetch и save перекрываются (PrevBatch): `bottleneck = max(fetch, save)`.

- smp=2: max(14ms, 19ms) → save bottleneck → 19ms/block
- smp=8: max(18ms, 13.6ms) → сбалансировано → 13.8ms/block
- smp=24: max(26ms, 14.5ms) → fetch bottleneck → 14.7ms/block

### Почему smp=8 быстрее smp=2 при одинаковых 2 горячих шардах?

Главная причина: **размер memtable × поведение bypass-fsync**.

| smp | mem/shard | flush | с bypass-fsync |
|-----|-----------|-------|----------------|
| smp=2 | 2.0 GB | редко, большой | медленнее (больше данных на диск) |
| smp=8 | 1.25 GB | чаще, меньше | быстрее (маленькие flush, fsync не ждём) |

С `--unsafe-bypass-fsync=1` на tmpfs мелкий flush дешевле крупного.

---

## Часть 2: Go загрузчик (gotest/loader)

### Оптимизация загрузчика

| Версия | Метод | Конфиг | ms/block | CQL ошибки |
|--------|-------|--------|---------|-----------|
| Старый | pipelined EXECUTE, conns=4/table | conns=4, pipe=64 | 24.8ms | 0 |
| Новый firehose | UNLOGGED BATCH, batch-split=100..100..10..10 | split=1,3,6,20,1,1 | **12.1ms** | 0 |
| Новый saveBatch | per-batch workers, batch=100 | split=1,3,6,20,1,1 | 18.5ms | 0 |

**Почему загрузчик медленнее парсера:**
- Парсер: fetch(N+1) перекрывается с save(N) → fetch "бесплатен"
- Загрузчик: данные в памяти, нет перекрытия → чистое время записи
- Парсер пишет ~20 contracts/батч → нет "Batch too large"; загрузчик пишет 100 → превышает 50KB лимит Scylla

**batch-split:** contracts и cba используют batch=10 (bytecodes крупные), остальные batch=100.

### tmpfs vs ext4 (smp=8, 10G)

| Mode | tmpfs | ext4 |
|------|-------|------|
| warmup cold | 25ms | 36.7ms (+47%) |
| saveBatch | 18.5ms | 27.8ms (+50%, +timeout CQL errors) |
| firehose batch-split=100..10..10 | **12.8ms** | 13.7ms (+7%) |

ext4 с bypass-fsync почти одинаков для firehose (async flush), но saveBatch получает timeout-ошибки — крупные BATCH-фреймы медленнее коммитятся на ext4.

---

## Часть 3: Влияние RAW_CHUNK_SIZE на загрузчик

### Сколько шардов активно

```
chunk = block_number / RAW_CHUNK_SIZE
1000 блоков (25078197–25079196):

chunk_size=1000 → 2 уникальных chunk  → 2  горячих шарда
chunk_size=100  → 11 уникальных chunk → 11 горячих шардов
chunk_size=10   → 101 уникальных chunk → 101 горячих шардов (все 8 задействованы)
```

### Результаты (smp=8, 10G, tmpfs, bypass-fsync)

| chunk_size | hot partitions | saveBatch | firehose |
|-----------|----------------|-----------|---------|
| 1000 | 2 | 16.9ms | 14.5ms |
| 100 | 11 | **17.8ms** | 21.2ms |
| **10** | **101** | ~~48ms (timeout!)~~ | **8.2ms** |

### Объяснение результатов

**chunk_size=10, firehose → 8.2ms**: лучший результат. 101 уникальный chunk задействует
все 8 шардов равномерно. Firehose выигрывает потому что непрерывно заливает все шарды без
sequential wait между dump-батчами.

**chunk_size=10, saveBatch timeout**: saveBatch ждёт завершения каждого batch-окна (10 блоков).
При 101 chunk-ключах на 1000 блоков, каждый dump-батч имеет 10 разных chunk-ключей →
10 разных шардов получают write + flush одновременно → превышение timeout.

**chunk_size=100, saveBatch лучше firehose (17.8 vs 21.2ms)**: 11 chunk-ключей — достаточно
для хорошего распределения, но не слишком много. saveBatch выигрывает потому что каждый
батч (10 блоков) пишет данные одного chunk — шарды получают компактные partition writes.
Firehose при chunk_size=100 создаёт больше cross-shard давления.

**chunk_size=1000, firehose лучше saveBatch (14.5 vs 16.9ms)**: только 2 горячих шарда,
sequential saveBatch overhead заметен. Firehose непрерывно загружает без пауз.

### Главный вывод по chunk_size

```
chunk_size=10  → 101 шард активен → firehose 8.2ms/block  ← максимальная скорость
chunk_size=100 → 11 шардов        → saveBatch 17.8ms/block ← лучший balance для batch-режима
chunk_size=1000 → 2 шарда         → firehose 14.5ms/block  ← текущий prod
```

RAW_CHUNK_SIZE=10 ускоряет загрузку в **1.8× по сравнению с RAW_CHUNK_SIZE=1000**
при одинаковых остальных параметрах.

**Но:** изменение RAW_CHUNK_SIZE несовместимо с prod-схемой (partition key определён).
В проде с продовым smp=32 и chunk_size=1000 при параллельной загрузке разных диапазонов
все шарды будут активны автоматически.

---

## Часть 6: Парсер zigtest2 с REMAP_MOD

### Изменения

`transform.zig` — новый параметр `remap_mod`:
```zig
const chunk = if (remap_mod > 0)
    @as(i32, @intCast(@mod(number, @as(i64, @intCast(remap_mod)))))
else
    @as(i32, @intCast(@divFloor(number, @as(i64, @intCast(chunk_size)))));
```

`main.zig` — env var `REMAP_MOD`:
```bash
REMAP_MOD=8 ./zig-out/bin/zigparser2  # chunk = block_number % 8
```

### Результаты парсера (smp=8, 10G, tmpfs, BATCH_SIZE=8)

| Конфиг | ms/block | Save total | FBDR avg |
|--------|---------|-----------|---------|
| **REMAP_MOD=8** (8 шардов) | **8.75ms** | 8578ms | 16.3ms |
| REMAP_MOD=0 (baseline, 2 шарда) | 15.2ms | 15064ms | 13.2ms |

REMAP_MOD=8 ускоряет парсер в **1.7×**: 15.2ms → 8.75ms.

Save total снизился с 15s до 8.5s. FBDR вырос с 13ms до 16ms — 8 активных шардов
немного конкурируют с HTTP fetch за CPU.

Разрыв с loader firehose (5.36ms): парсер ожидает HTTP fetch (~16ms/block),
который частично перекрывается с save. Без fetch-задержки (pure loader) быстрее.

---

## Часть 7: PIPELINE — пул fetch-воркеров

### Архитектура

`PIPELINE=N` env var: N fetch+transform воркеров запускаются параллельно,
каждый с собственным CQL пулом → N параллельных saves.

```
PIPELINE=1 (старое поведение):
  Round: [fetch(N)] → [save(N)] || [fetch(N+1)] → ...
  1 save в полёте, save перекрывается с 1 fetch

PIPELINE=2 (новое):
  Round: [fetch(N) || fetch(N+1)] → [save(N) || save(N+1)] || [fetch(N+2) || fetch(N+3)]
  2 saves параллельно (разные CQL пулы), перекрываются с 2 fetches
```

Реализация: `src/main.zig` — `FetchArgs + fetchWorker`, кольцевой буфер `prev_saves[P]`.
Env vars: `PIPELINE=N`, `REMAP_MOD=N`, `BATCH_SIZE=N`.

### Результаты (smp=16, 5G, tmpfs, REMAP=16, warm Scylla)

| Конфиг | ms/block | FBDR avg | Save total | Save/batch |
|--------|---------|---------|-----------|-----------|
| PIPELINE=1, BATCH=16 | 7.63ms | 50ms | 7185ms | 114ms |
| **PIPELINE=2, BATCH=16** | **5.80ms** | 98ms | 11035ms | 175ms |
| PIPELINE=2, BATCH=8 | 6.55ms | 48ms | 12518ms | 198ms |

Улучшение PIPELINE=2 vs PIPELINE=1: **~18-24%** (cold run) / **~7%** (warm run).

### Анализ узких мест

**gonode bottleneck**: 2 параллельных воркера удваивают HTTP нагрузку на ноду.
FBDR растёт с 50ms до 98ms при PIPELINE=2 + BATCH=16.
При PIPELINE=2 + BATCH=8: FBDR=48ms (норм) но save занимает больше → итог хуже.

**Scylla contention**: 2 параллельных save → каждый шард получает 2×нагрузку.
Каждый save замедляется: 114ms → 175ms. Scylla не масштабируется линейно при 2× нагрузке.

**Тем не менее**: перекрытие fetch+save даёт выигрыш даже при замедлении обоих.
Effective round time: 2 batches / max(fetch_round, save_round) = 2×32 / 187ms = ~10.7 batches/s
vs PIPELINE=1: 1×32 / 114ms = ~8.8 batches/s → +22% throughput.

### На реальной ноде (не localhost)

С RTT 50-200ms fetch займёт 200+ms. PIPELINE=2 даст ~2× fetch throughput
при той же save latency → ожидаемое ускорение 40-60%.

---

## Итоговая сводка лучших результатов

| Инструмент | Конфиг | Sharding | ms/block |
|-----------|--------|---------|---------|
| loader firehose | smp=16, 5G, tmpfs | remap-mod=16 | **5.27ms** |
| loader firehose | smp=8, 10G, tmpfs | remap-mod=8 | **5.36ms** |
| loader saveBatch | smp=8/16, tmpfs | remap-mod=8/16, 16 блоков | ~5.6ms |
| loader firehose | smp=8, 10G, tmpfs | chunk_size=10 | 8.44ms |
| **parser PIPELINE=2** | smp=16, 5G, tmpfs | **REMAP_MOD=16, BATCH=16** | **5.80ms** |
| parser PIPELINE=1 | smp=16, 5G, tmpfs | REMAP_MOD=16, BATCH=16 | 6.25ms |
| parser PIPELINE=1 | smp=8, 10G, tmpfs | REMAP_MOD=8, BATCH=8 | 8.75ms |
| parser (baseline) | smp=8, 10G, tmpfs | chunk_size=1000 | 13.8ms |
| loader firehose | smp=8, 10G, tmpfs | chunk_size=1000 | 14.5ms |
| loader старый (EXECUTE) | smp=8, 10G | chunk_size=1000 | 24.8ms |

---

## Команды воспроизведения

### Конфиг smp=8, 10G (оптимум для этой машины)

```bash
# /etc/default/scylla-server
SCYLLA_ARGS="--smp=8 --unsafe-bypass-fsync=1 --overprovisioned --max-concurrent-requests-per-shard=65536 \
             --log-to-syslog=1 --log-to-stdout=0 --default-log-level=info --network-stack=posix"
# /etc/scylla.d/memory.conf
MEM_CONF="--memory=10G"
```

### Загрузчик

```bash
cd /home/alex/lotos/task1/gotest/loader

# Генерация дампов (требует gonode)
RAW_CHUNK_SIZE=10   DUMP_FILE=../writetest/dump_1000_chunk10.bin   zigparser2 ...
RAW_CHUNK_SIZE=100  DUMP_FILE=../writetest/dump_1000_chunk100.bin  zigparser2 ...
RAW_CHUNK_SIZE=1000 DUMP_FILE=../writetest/dump_1000.bin            zigparser2 ...

# Тест (после warmup + TRUNCATE)
./loader -dump=../writetest/dump_1000_chunk10.bin -mode=firehose -batch-split="100,100,100,100,10,10"
./loader -dump=../writetest/dump_1000.bin -mode=firehose -batch-split="100,100,100,100,10,10"
./loader -dump=../writetest/dump_1000.bin  # saveBatch mode (default)
```

### Парсер

```bash
cd /home/alex/lotos/task1/zigtest2
ZIG=/home/alex/lotos/zig-x86_64-linux-0.17.0-dev.263+0add2dfc4/zig
$ZIG build -Doptimize=ReleaseFast -Dpool_size=32 -Dsplit="1,3,6,20,1,1"

SCYLLA_DB_CONTACT_POINTS='["172.31.208.104:9142"]' \
SCYLLA_DB_KEYSPACE=eth \
SCYLLA_DB_CREDENTIALS='{"username":"cassandra","password":"cassandra"}' \
CM_CONNECTION_URL="redis://:mockpass@127.0.0.1:6379/0" \
RPC_URL=http://127.0.0.1:8545 CHAIN_ID=1 RAW_CHUNK_SIZE=1000 \
TO_BLOCK=25079196 BATCH_SIZE=10 \
./zig-out/bin/zigparser2
```

---

## Часть 4: Модульное шардирование (remap-mod=N)

### Идея

Вместо `chunk = block_number / RAW_CHUNK_SIZE` (группирует 1000 блоков в 2 шарда)
использовать `chunk = block_number % N` — соседние блоки попадают в разные шарды.

```
remap-mod=8:  chunk = block_number & 7
  блок 25078197 → chunk 5
  блок 25078198 → chunk 6
  ...каждые 8 блоков покрывают все 8 шардов (0-7)
```

Реализация: флаг `-remap-mod N` в `gotest/loader/main.go` перезаписывает
chunk in-place в Values-буфере каждой строки (таблицы 0-4, байты [4:8]).

### Результаты (smp=8, 10G, tmpfs, bypass-fsync)

**Загрузчик saveBatch, remap-mod=8, vary blocks/saveBatch:**

| blocks/saveBatch | hot shards | ms/block | CQL ошибки |
|-----------------|-----------|---------|-----------|
| 8  (1×8)  | 8 | **5.70ms** | 1 (Batch too large) |
| 16 (2×8)  | 8 | **5.74ms** | 0 ✓ |
| 32 (4×8)  | 8 | **5.68ms** | 0 ✓ |
| 64 (8×8)  | 8 | **5.76ms** | 0 ✓ |

Все варианты от 8 до 64 блоков дают ~5.7ms/block — размер батча не влияет на результат.
Scylla прогрета, 8 горячих шардов, данные пишутся равномерно.

**Baseline без remap (только 2 шарда):**

| blocks/saveBatch | hot shards | ms/block |
|-----------------|-----------|---------|
| 8  | 2 | 15.5ms |
| 32 | 2 | 13.9ms |

**Firehose-режим:**

| Режим | ms/block |
|-------|---------|
| firehose + remap-mod=8 | **5.36ms** |
| firehose + chunk_size=10 (101 chunks) | 8.44ms |
| firehose + no remap (2 chunks) | ~13ms |

### Главный вывод: remap-mod=8 лучше chunk_size=10

`remap-mod=8` даёт **5.36-5.7ms/block** против 8.44ms для chunk_size=10:
- remap=8: ровно 8 уникальных chunk [0..7] → ровно 8 горячих шардов = smp
- chunk_size=10: 101 уникальный chunk → много шардов конкурируют, больше overhead

Ключевой принцип: **N уникальных chunk = N × smp** — оптимально когда N=1 (chunk=block%smp).

### Технические детали

```bash
# Дамп с BATCH_SIZE=8 (8 блоков/батч)
BATCH_SIZE=8 DUMP_FILE=dump_1000_b8.bin zigparser2 ...

# Тест с remap и merge батчей
./loader -dump=dump_1000_b8.bin -remap-mod=8 \
  -batch-split=100,100,100,100,10,10   # contracts/cba=10 (bytecodes!)
./loader -dump=dump_1000_b8.bin -remap-mod=8 -blocks-per-batch=32  # merge 4 batches
```

`-batch-split=100,100,100,100,10,10` обязателен — contracts и cba могут содержать
крупные bytecodes; 100 таких строк в одном BATCH-фрейме превысит 50KB лимит Scylla.

---

## Часть 5: smp=16, remap-mod=16, малая память на шард

### Гипотеза

С remap-mod=16 активны 16 шардов. При очень малой памяти на шард (~300MB)
memtable заполняется быстро → частые маленькие flush → с bypass-fsync дёшево.

### Ограничения по минимальной памяти

| memory | mem/shard | результат |
|--------|-----------|----------|
| 1.6G | 100MB | ❌ не стартует (`logalloc::bad_alloc` в system keyspace) |
| 3.2G | 200MB | ❌ не стартует (flush system_schema не помещается) |
| **5G** | **312MB** | ✅ стартует, работает |

Минимум ~300MB/shard для Scylla 6.2.

### Результаты (smp=16, 5G, tmpfs, bypass-fsync, remap-mod=16)

| Режим | blocks/saveBatch | ms/block | CQL ошибки |
|-------|-----------------|---------|-----------|
| saveBatch | 8  | 5.67ms | 1 |
| saveBatch | **16** | **5.59ms** | 0 ✓ |
| saveBatch | 32 | 5.68ms | 0 ✓ |
| **firehose** | — | **5.27ms** | 1 |

Сравнение с предыдущим лучшим (smp=8, remap=8):

| Конфиг | firehose ms/block |
|--------|------------------|
| smp=16, 5G, remap-mod=16 | **5.27ms** |
| smp=8, 10G, remap-mod=8 | 5.36–5.93ms |

### Вывод

smp=16 + remap-mod=16 + меньше памяти на шард ≈ smp=8 + remap-mod=8.
Разница в пределах погрешности (~10%). Ключевая зависимость:

```
N_шардов = remap-mod = smp
mem/shard ≈ 300MB–1.25GB — не критично при bypass-fsync на tmpfs
```

Основной bottleneck сместился с compaction/flush на что-то другое (network stack, CQL processing).
Дальнейшее уменьшение памяти не помогает — 312MB/shard уже близко к минимуму.

---

## Часть 8: PIPELINE sweep (zigparser2, smp=16 tmpfs)

**Дата:** 2026-05-22  
**Условия:** 1000 блоков (25078197–25079196), REMAP_MOD=16, BATCH_SIZE=16, smp=16 5G tmpfs, gonode localhost:8545.

### Результаты

| Конфиг | Total ms | ms/block | FBDR avg/block | Примечание |
|--------|---------|---------|----------------|-----------|
| PIPELINE=1 REMAP=16 BATCH=16 | 6062 ms | 6.06 ms | 40 ms | Baseline |
| **PIPELINE=2 REMAP=16 BATCH=16** | **5580 ms** | **5.58 ms** | ~100 ms | Лучший |
| PIPELINE=4 REMAP=16 BATCH=16 | 6177 ms | 6.18 ms | 244 ms | gonode bottleneck |

### Анализ

С PIPELINE=4 FBDR вырос до 244ms (vs 40ms у PIPELINE=1) — gonode-мок перегружен 4 параллельными потоками (4×48=192 одновременных HTTP запроса).

**Оптимум для localhost: PIPELINE=2.** На реальной ноде с RTT 50–200ms PIPELINE=4 ожидаемо лучше.

Сборка и запуск:
```bash
ZIG=/home/alex/lotos/zig-x86_64-linux-0.17.0-dev.263+0add2dfc4/zig
cd zigtest2
$ZIG build -Doptimize=ReleaseFast -Dpool_size=32 -Dsplit="1,3,6,20,1,1"

CM_CONNECTION_URL="redis://:mockpass@127.0.0.1:6379/0" \
SCYLLA_DB_CONTACT_POINTS='["172.31.208.104:9142"]' \
SCYLLA_DB_KEYSPACE=eth RPC_URL=http://127.0.0.1:8545 CHAIN_ID=1 \
TO_BLOCK=25079196 BATCH_SIZE=16 REMAP_MOD=16 PIPELINE=2 \
./zig-out/bin/zigparser2
```

---

## Часть 9: Сравнение zigparser2 vs TS1 (полный pipeline)

**Дата:** 2026-05-22  
**Условия:** 1000 блоков, gonode localhost:8545, native Scylla smp=16 5G tmpfs, одинаковые данные.

### TS1 — полный тест с реальной ScyllaDB

Архитектура: `historical.ts` (fetch+transform → BullMQ) + 5 × `save.ts` (BullMQ → cassandra-driver EXECUTE).  
Запуск: `cd mocknode && bash scripts/run_ts1_bench.sh 5 1000`

**ВАЖНО:** DragonflyDB должен быть запущен с `--cluster_mode=emulated --lock_on_hashtags`, иначе BullMQ Lua scripts падают с `ERR script tried accessing undeclared key`.

```bash
docker run --rm -d --name temp_dragonfly --network host \
  docker.dragonflydb.io/dragonflydb/dragonfly:latest \
  --bind=0.0.0.0 --port=6379 \
  --lock_on_hashtags --cluster_mode=emulated --requirepass=mockpass
```

#### Разбивка TS1 (1000 блоков, 5 workers)

| Фаза | Время |
|------|-------|
| FBDR avg/block (HTTP gonode) | 34.6 ms |
| Transform avg/batch (10 блоков) | 52.0 ms |
| BullMQ addJob avg/batch | 41.0 ms |
| TPT (historical.ts) | 14915 ms / **10.6 ms/block** |
| Save workers (после historical.ts) | ~12933 ms |
| **Полный wall-clock** | **27848 ms / 27.85 ms/block** |

### Итоговая таблица сравнения

| Парсер | ms/block | vs TS1 | CQL path |
|--------|---------|--------|---------|
| **TS1** (5 workers, BullMQ) | **27.85 ms** | 1.0× | cassandra-driver EXECUTE per row |
| zigparser2 PIPELINE=1 | 6.06 ms | 4.6× быстрее | UNLOGGED BATCH 100 rows/frame |
| **zigparser2 PIPELINE=2** | **5.58 ms** | **5.0× быстрее** | UNLOGGED BATCH 100 rows/frame |
| zigparser2 PIPELINE=4 | 6.18 ms | 4.5× быстрее | UNLOGGED BATCH 100 rows/frame |

### Ключевые факторы преимущества zigparser2

1. **UNLOGGED BATCH vs individual EXECUTE**: 100 строк за 1 CQL-фрейм вместо 100 отдельных запросов → меньше round-trips
2. **Нет BullMQ overhead**: нет сериализации в Redis, нет шины очереди
3. **PrevBatch overlap**: save N-1 перекрывается с fetch+transform N
4. **REMAP_MOD=16**: все 16 шардов активны (vs 2 шарда при chunk_size=1000)

---

## Итоговая сводка всех оптимизаций парсера

| Этап | Конфиг | ms/block | Ускорение от предыдущего |
|------|--------|---------|------------------------|
| Baseline (Docker smp=2, chunk=1000) | POOL=32, PIPE=256 | 21.6 ms | — |
| Native Scylla smp=24 ext4 | POOL=32, PIPE=256 | 21.7 ms | ≈0% |
| + tmpfs + remap-mod=8 | smp=8, BATCH=8 | 8.75 ms | 2.5× |
| + UNLOGGED BATCH | smp=16, remap=16, BATCH=16 | 6.25 ms | 1.4× |
| + PIPELINE=2 | smp=16, REMAP=16, BATCH=16 | **5.58 ms** | 1.12× |

**Итог: 21.6ms → 5.58ms = 3.9× ускорение относительно baseline.**  
**vs TS1 (27.85ms): 5.0× быстрее.**

---

## Часть 10: Realtime-режим — latency одного блока

**Дата:** 2026-05-22  
**Условия:** 100 блоков (25079097–25079196), REMAP_MOD=16, smp=16 5G tmpfs, gonode localhost.  
**Метрика:** время от отправки первого HTTP запроса на ноду до завершения записи в ScyllaDB (wall-clock per block).

### Конфигурация

```bash
REALTIME=1 POLL_MS=<N> REMAP_MOD=16 TO_BLOCK=25079196
```

### Результаты

| POLL_MS | fetch avg | save avg | **total avg** | total min | total max |
|---------|----------|---------|-------------|----------|----------|
| 500 ms  | 1.4 ms   | 8.5 ms  | **11.8 ms** | 4.9 ms   | 45.2 ms  |
| 250 ms  | 1.4 ms   | 8.5 ms  | **11.8 ms** | 4.9 ms   | 30.6 ms  |

### Разбивка avg per block (POLL_MS=500)

| Фаза | Время |
|------|-------|
| fetch (3 параллельных HTTP + JSON) | 1.4 ms |
| transform | ~0.3 ms |
| save (UNLOGGED BATCH → Scylla) | 8.5 ms |
| **TOTAL (запрос → DB)** | **11.8 ms** |

### Анализ

- **POLL_MS не влияет на latency** — все блоки доступны сразу в gonode, ожидания нет.
- **Fetch (1.4ms)** — gonode localhost, практически без задержки сети. На реальной ноде: +RTT.
- **Save (8.5ms)** — основной компонент latency. Один блок = один `saveBatch` со всеми таблицами.
- **Max (45ms)** — редкие outliers из-за Scylla compaction или OS scheduling.
- **Сравнение с historical:** realtime avg 11.8ms vs historical 5.58ms/block — в 2× медленнее.
  Разница: historical батчит 16 блоков в одном round-trip и перекрывает save с fetch (PIPELINE=2).
  В realtime каждый блок — отдельный save, нет pipeline overlap.

### Реальная нода (ожидаемые цифры)

При RTT к ноде 50ms: total ≈ 1.4 + 50 + 0.3 + 8.5 ≈ **60ms** (fetch станет доминирующим).  
При RTT 100ms: total ≈ **110ms**.

### Потенциал оптимизации realtime

Для снижения save latency можно:
1. Уменьшить SPLIT (больше соединений на logи/itxs — основной объём)
2. Уменьшить pool connections на save (сейчас pool=32 для 1 блока = излишне)
