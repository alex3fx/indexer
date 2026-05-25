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

---

## Часть 11: TS1 realtime — latency одного блока (BATCH_SIZE=1)

**Дата:** 2026-05-22  
**Условия:** 100 блоков (25079097–25079196), BATCH_SIZE=1, 1 воркер save.ts, gonode localhost, native Scylla smp=16 tmpfs.  
**Метрика:** время от начала fetch до завершения записи в ScyllaDB (TPT + save_ms per block).

### Разбивка TS1 per block (BATCH_SIZE=1)

| Фаза | avg | min | max |
|------|-----|-----|-----|
| FBDR (3×HTTP+JSON parse) | 6.4 ms | 1.0 ms | 12.0 ms |
| Transform (JS loop) | 4.2 ms | — | — |
| BullMQ addJob | 6.7 ms | — | — |
| **TPT** (fetch+xform+BullMQ) | **10.6 ms** | 3.0 ms | 20.0 ms |
| Save (cassandra-driver EXECUTE) | 127.8 ms | 23.0 ms | 273.0 ms |
| **TOTAL (fetch → DB)** | **138.7 ms** | 32.0 ms | 286.0 ms |

Wall-clock throughput: **2553ms / 100 blocks = 25.5ms/block**  
(saves перекрываются с fetch следующих блоков через BullMQ, поэтому wall-clock < per-block latency)

### Сравнение realtime: TS1 vs zigparser2

| Метрика | TS1 (BATCH_SIZE=1) | zigparser2 | Разница |
|---------|-------------------|------------|---------|
| Fetch avg | 6.4 ms | 1.4 ms | 4.6× |
| Transform avg | 4.2 ms | 0.3 ms | 14× |
| Queue overhead | 6.7 ms (BullMQ) | 0 ms | — |
| **Save avg** | **127.8 ms** (cassandra-driver) | **8.5 ms** (UNLOGGED BATCH) | **15× !** |
| **Per-block latency** | **138.7 ms** | **11.8 ms** | **11.7×** |
| Wall-clock throughput | 25.5 ms/block | 11.8 ms/block | 2.2× |

### Почему save у TS1 в 15× медленнее

TS1 использует `cassandra-driver.execute()` на каждую строку:
- 1 блок ≈ 2800 строк (270 txs + 600 logs + 1900 itxs + ...)
- `insertMany` бьёт на группы по 100 и ждёт `Promise.allSettled` последовательно
- 2800 / 100 = 28 sequential round-trips для одного блока

zigparser2 — нативный `UNLOGGED BATCH`:
- 100 строк в одном CQL-фрейме → 1 round-trip per frame
- 2800 / 100 = 28 frames, но отправляются параллельно через pool=32

**Итог:** cassandra-driver имеет фундаментальный overhead sequential Promise.allSettled на каждые 100 строк. Это даёт 127ms на блок вместо 8.5ms у zigparser2.

### Запуск теста

```bash
cd /home/alex/lotos/task1/mocknode
bash scripts/run_ts1_realtime.sh
```

---

## Часть 12: Realtime zigparser2 — drip-feed 50ms/block

**Дата:** 2026-05-22  
**Условия:** 100 блоков (25079097–25079196), REMAP_MOD=16, smp=16 tmpfs.  
**Gonode:** BLOCK_INTERVAL_MS=50 — новый блок появляется каждые 50ms (таймер с первого запроса).  
**Zigparser2:** REALTIME=1, POLL_MS=50 — опрос каждые 50ms при ожидании блока.

### Поведение

Видны пары `[JSON breakdown] 0KB` + реальные данные — парсер поймал момент когда блок ещё недоступен (null), поспал 50ms, повторил — получил данные. Drip-feed работает корректно.

### Результаты (100 блоков, poll_ms=50)

| Фаза | avg | min | max |
|------|-----|-----|-----|
| Fetch (poll + 3×HTTP) | 2.2 ms | 0.8 ms | 7.8 ms |
| Transform | ~0.3 ms | — | — |
| Save (UNLOGGED BATCH) | 11.6 ms | 3.7 ms | 46.5 ms |
| **TOTAL (poll→DB)** | **16.5 ms** | 5.3 ms | 51.2 ms |

> Fetch avg выше 1.4ms (vs 500ms-poll тест) — из-за 50ms poll retry overhead при ожидании блока.

### Сравнение poll интервалов (все тесты на gonode localhost)

| POLL_MS | Gonode режим | fetch avg | save avg | **total avg** |
|---------|------------|----------|---------|-------------|
| 500 ms | мгновенный | 1.4 ms | 8.5 ms | **11.8 ms** |
| 250 ms | мгновенный | 1.4 ms | 8.5 ms | **11.8 ms** |
| **50 ms** | **drip-feed 50ms** | **2.2 ms** | **11.6 ms** | **16.5 ms** |

Увеличение total avg с 11.8ms → 16.5ms при drip-feed объясняется:
- fetch avg +0.8ms: иногда делает один null-poll (50ms sleep) прежде чем блок готов
- save avg +3ms: небольшая Scylla нагрузка от более длинного теста

### Запуск

```bash
# 1. Gonode с drip-feed 50ms:
bash -c 'MOCK_DATA_FILE=packtest/data/blocks_fresh_100.json CHAIN_ID=1 BLOCK_INTERVAL_MS=50 \
  mocknode/gonode/gonode > /tmp/gonode.log 2>&1 &'

# 2. Парсер:
REALTIME=1 POLL_MS=50 REMAP_MOD=16 TO_BLOCK=25079196 \
  zigtest2/zig-out/bin/zigparser2
```

---

## Часть 13: WebSocket newHeads в zigparser2 (runRealtimeWs)

**Дата:** 2026-05-22  
**Условия:** 100 блоков (25079097–25079196), REMAP_MOD=16, smp=16 tmpfs.  
**Gonode:** BLOCK_INTERVAL_MS=100 — блок каждые 100ms, WS endpoint `/ws`.  
**Zigparser2:** REALTIME=1 WS_URL=ws://127.0.0.1:8545/ws.

### Архитектура WS режима

```
gonode /ws (eth_subscribe newHeads)
    │  push block header (number, hash, ...)
    ▼
ws.zig WsConn.nextBlockNum()  ← блокирует до события
    │  block_num извлечён из JSON
    ▼
processBlock(block_num)       ← 3×HTTP parallel + transform + saveBatch
    │
    ▼
Redis cursor update
```

- `ws.zig` — RFC 6455 клиент, 184 строки, raw Linux TCP, без внешних зависимостей
- `WsConn.subscribeNewHeads()` → отправляет eth_subscribe, возвращает sub_id
- `WsConn.nextBlockNum()` → блокирует на fdRead, возвращает block_num из eth_subscription event

### Результаты (100 блоков, drip-feed 100ms)

| Фаза | avg | min | max |
|------|-----|-----|-----|
| Fetch (WS wakeup + 3×HTTP) | 2.1 ms | 0.9 ms | 9.7 ms |
| Transform | ~0.3 ms | — | — |
| Save (UNLOGGED BATCH) | 11.7 ms | 4.7 ms | 55.6 ms |
| **TOTAL (WS event → DB)** | **16.4 ms** | 7.0 ms | 59.3 ms |

### Сравнение WS vs polling (все тесты drip-feed, gonode localhost)

| Режим | Gonode | POLL/interval | fetch avg | save avg | **total avg** |
|-------|--------|--------------|----------|---------|-------------|
| Polling | мгновенный | POLL_MS=500 | 1.4ms | 8.5ms | 11.8ms |
| Polling | мгновенный | POLL_MS=250 | 1.4ms | 8.5ms | 11.8ms |
| Polling | drip-feed 50ms | POLL_MS=50 | 2.2ms | 11.6ms | 16.5ms |
| **WebSocket** | **drip-feed 100ms** | **n/a** | **2.1ms** | **11.7ms** | **16.4ms** |

**Вывод:** на localhost результаты WS и polling практически идентичны.  
Преимущество WS проявится на реальной ноде с сетевой задержкой:  
polling добавляет POLL_MS/2 среднего ожидания (250ms poll → +125ms), WS — нет.

### Запуск

```bash
# Gonode с WS и drip-feed:
bash -c 'MOCK_DATA_FILE=packtest/data/blocks_fresh_100.json CHAIN_ID=1 \
  BLOCK_INTERVAL_MS=100 mocknode/gonode/gonode > /tmp/gonode.log 2>&1 &'

# Zigparser2 WS режим:
REALTIME=1 WS_URL=ws://127.0.0.1:8545/ws REMAP_MOD=16 TO_BLOCK=25079196 \
  zigtest2/zig-out/bin/zigparser2
```

### Исправление: WS с мгновенным gonode (честное сравнение с polling)

Предыдущий WS тест использовал drip-feed 100ms — Scylla "остывала" между блоками.  
Исправленный тест: instant gonode (все блоки сразу), те же условия что и polling 500/250ms.

**Баги исправлены в gonode:**
1. `minBlock` теперь всегда инициализируется при загрузке (не только при drip-feed)  
2. Буфер WS канала 64 → 1024 (100+ блоков не дропаются)  
3. `lastPushed` теперь per-subscriber (каждый клиент получает все блоки с начала подписки)

| Режим | Gonode | fetch avg | save avg | **total avg** |
|-------|--------|----------|---------|-------------|
| Polling POLL_MS=500 | мгновенный | 1.4ms | 8.5ms | **11.8ms** |
| Polling POLL_MS=250 | мгновенный | 1.4ms | 8.5ms | **11.8ms** |
| **WebSocket (исправлен)** | **мгновенный** | **1.4ms** | **8.6ms** | **12.1ms** |
| WS (drip-feed 100ms) | drip-feed | 2.1ms | 11.7ms | 16.4ms |

**Вывод:** WS не даёт деградации относительно polling (12.1ms vs 11.8ms — в пределах погрешности).  
Разница 0.3ms — это overhead одного WS frame read вместо HTTP null-ответа.  
Предыдущая «деградация» 16ms была артефактом drip-feed (Scylla простаивала между блоками).

---

## Часть 14: WS realtime — 10 блоков/сек (BLOCK_INTERVAL_MS=100)

**Дата:** 2026-05-22  
**Условия:** 100 блоков (25079097–25079196), REMAP_MOD=16, smp=16 tmpfs.  
**Gonode:** BLOCK_INTERVAL_MS=100 — новый блок каждые 100ms через WS push.  
**Zigparser2:** REALTIME=1 WS_URL=ws://127.0.0.1:8545/ws.

### Результаты

| Фаза | avg | min | max |
|------|-----|-----|-----|
| Fetch (WS wakeup + 3×HTTP) | 2.4 ms | 1.0 ms | 14.9 ms |
| Transform | ~0.3 ms | — | — |
| Save (UNLOGGED BATCH) | 11.8 ms | 4.6 ms | 44.5 ms |
| **TOTAL (WS push → DB)** | **16.8 ms** | 6.6 ms | 51.1 ms |

### Итоговое сравнение всех realtime режимов

| Режим | Gonode | fetch avg | save avg | **total avg** | Объяснение |
|-------|--------|----------|---------|-------------|-----------|
| Polling POLL_MS=500 | мгновенный | 1.4ms | 8.5ms | **11.8ms** | Scylla горячая, нет ожидания |
| Polling POLL_MS=250 | мгновенный | 1.4ms | 8.5ms | **11.8ms** | то же |
| Polling POLL_MS=50 | drip-feed 50ms | 2.2ms | 11.6ms | 16.5ms | Scylla чуть остывает |
| WS | мгновенный | 1.4ms | 8.6ms | **12.1ms** | ≈ polling, +0.3ms overhead |
| WS | drip-feed 100ms | 2.1ms | 11.7ms | 16.4ms | 89ms idle → Scylla остывает |
| **WS 10 блок/сек** | **drip-feed 100ms** | **2.4ms** | **11.8ms** | **16.8ms** | реалистичная продукция |

### Вывод

При **10 блоков/сек** (реалистичный темп для BSC/fast chains):
- Latency WS push → DB write: **16.8ms avg** (~17ms)
- Scylla save 11.8ms — доминирующий компонент
- 88ms из 100ms парсер простаивает (ждёт следующего блока)

На реальной ноде с RTT 10–50ms: total ≈ RTT + 2ms(transform) + 11ms(save) = **~25–65ms**.

WS vs polling при 10 блок/сек (POLL_MS=100):  
- Polling POLL_MS=100: среднее ожидание +50ms → total avg ≈ **67ms** (11.8 + 50 + overhead)  
- WebSocket: **16.8ms** (нет ожидания — уведомление мгновенное)  
- **WS выгоднее в ≈4× при production темпе блоков**

### 3 прогона подряд (TRUNCATE между тестами)

Каждый прогон: gonode перезапускается (сброс drip-feed таймера), TRUNCATE всех таблиц, settle 5s.

| Прогон | fetch avg | save avg | **total avg** | total min | total max |
|--------|----------|---------|-------------|----------|----------|
| Run 1 | 2.1 ms | 12.2 ms | **16.8 ms** | 7.7 ms | 43.5 ms |
| Run 2 | 2.1 ms | 11.0 ms | **15.6 ms** | 6.1 ms | 59.3 ms |
| Run 3 | 2.1 ms | 11.6 ms | **16.4 ms** | 6.2 ms | 65.9 ms |
| **Среднее** | **2.1 ms** | **11.6 ms** | **16.3 ms** | — | — |

**Fetch стабилен: 2.1ms во всех трёх прогонах.**  
**Save вариация 11.0–12.2ms** — зависит от состояния Scylla memtable после TRUNCATE.  
**Total avg 15.6–16.8ms** — воспроизводимо в пределах ±1.2ms.

---

## Часть 15: loader2 — realtime single-block write benchmark

**Дата:** 2026-05-22  
**Программа:** `gotest/loader2/` — Go, нативный CQL TCP  
**Данные:** dump_100_b1.bin — 100 блоков × 1 блок на батч, REMAP_MOD=16  
**Условия:** BLOCK_INTERVAL_MS=100 (10 блок/сек), smp=16 tmpfs, 1 соединение на таблицу  
**Метрика:** wall-clock от отправки первого CQL BATCH до получения последнего ответа (6 таблиц параллельно)

### Что делает loader2

1. Загружает dump с 1 блоком на батч в память (~124MB, 3051 rows/block avg)
2. 6 персистентных CQL соединений (по одному на таблицу)
3. Каждые 100ms: отправляет 6 UNLOGGED BATCH параллельно (по одному на таблицу)
4. Измеряет `save_ms` = wall-clock от первой отправки до последнего ответа

### Результаты (100 блоков, interval=100ms)

| Метрика | avg | min | p50 | p95 | p99 | max |
|---------|-----|-----|-----|-----|-----|-----|
| **save (все 6 таблиц)** | **24.5ms** | 4.0ms | 18.5ms | 80.9ms | 112.4ms | 112.4ms |
| itxs (доминирует) | 24.3ms | 3.9ms | 18.3ms | 80.7ms | 112.1ms | 112.1ms |
| logs | 14.5ms | 2.2ms | 11.1ms | 46.4ms | 89.0ms | 89.0ms |
| txs | 8.4ms | 1.6ms | 7.2ms | 17.6ms | 55.0ms | 55.0ms |
| blocks | 2.4ms | 0.2ms | 1.7ms | 7.2ms | 26.1ms | 26.1ms |
| contracts | 1.9ms | 0.0ms | 1.1ms | 4.8ms | 21.4ms | 21.4ms |
| cba | 2.5ms | 0.0ms | 1.6ms | 6.3ms | 47.4ms | 47.4ms |

### Анализ

**Bottleneck — internal_transactions** (itxs): avg 24.3ms, max 112ms.  
1 блок = ~1900 itxs → 19 BATCH фреймов × 100 строк. При 100ms интервале Scylla периодически делает flush (compaction outliers → p99=112ms).

**Median save: 18.5ms** — типичная запись 1 блока. p50 более репрезентативен чем avg из-за outliers.

**Сравнение с zigparser2 realtime (WS):** zigparser2 = 16.3ms avg vs loader2 = 24.5ms avg.  
Разница: zigparser2 использует PIPELINE=1 с 32 соединениями на таблицу (больше параллелизма), loader2 — 1 соединение.

### Запуск

```bash
cd gotest/loader2
# Создать dump (1 блок/батч):
BATCH_SIZE=1 REMAP_MOD=16 DUMP_FILE=dump_100_b1.bin ... zigparser2

# Запустить тест:
./loader2 -dump=dump_100_b1.bin -interval=100 -truncate
```

### loader2: sweep пула соединений (conns=1,2,4,8)

| conns/table | save avg | p50 | p95 | p99 | itxs avg |
|------------|---------|-----|-----|-----|---------|
| 1 | 21.2ms | 17.8ms | 48.8ms | 69.6ms | 21.0ms |
| **2** | **17.6ms** | **13.2ms** | 56.6ms | 94.4ms | 17.4ms |
| **4** | **13.8ms** | **11.1ms** | **33.2ms** | **53.4ms** | **13.6ms** |
| 8 | 14.6ms | 10.1ms | 50.5ms | 73.8ms | 14.4ms |

**Оптимум: 4 соединения на таблицу** → save avg 13.8ms, p50 11.1ms.

При conns=4: 1900 itxs = 19 BATCH фреймов делятся на 4 параллельных потока по ~5 фреймов. Latency ≈ 1/4 от conns=1 (21ms → 13.8ms ≈ 1.5×, не 4× — из-за накладных расходов Scylla на параллельные writes в один шард).

При conns=8: немного хуже conns=4 — contention на одном шарде при 100ms интервале.

**Итоговый минимум latency 1 блока:**
- **save avg: 13.8ms** (conns=4)
- **p50: 11.1ms**
- **min: 2.4ms** (пустые блоки без contracts/cba)

### loader2: балансировка split (как в zigparser2)

`-split=blocks,txs,logs,itxs,conts,cba` — разное число соединений на таблицу.

| split | total | save avg | p50 | p95 | p99 |
|-------|-------|---------|-----|-----|-----|
| `1,1,1,1,1,1` (uniform=1) | 6 | 21.2ms | 17.8ms | 48.8ms | 69.6ms |
| `4,4,4,4,4,4` (uniform=4) | 24 | 13.8ms | 11.1ms | 33.2ms | 53.4ms |
| **`1,3,6,20,1,1`** (zigparser2) | **32** | **9.5ms** | **8.1ms** | **23.9ms** | **43.8ms** |
| `1,3,6,32,1,1` | 44 | 12.7ms | 10.9ms | 30.8ms | 48.4ms |
| `1,5,10,32,1,1` | 50 | 12.2ms | 9.9ms | 37.1ms | 54.9ms |

**Оптимум — split=1,3,6,20,1,1 (pool_size=32, как в zigparser2).**

Почему именно 1,3,6,20:
- itxs: ~1900 строк / 20 соединений = 95 строк = **1 BATCH фрейм на соединение** → максимальный параллелизм
- logs: ~600 строк / 6 = 100 строк = 1 фрейм
- txs: ~270 строк / 3 = 90 строк = 1 фрейм

При 32 соединениях на itxs — contention на одном шарде ухудшает результат.

**Итоговый минимум latency записи 1 блока (loader2, smp=16, tmpfs):**

| Метрика | Значение |
|---------|---------|
| save avg | **9.5ms** |
| p50 | **8.1ms** |
| p95 | 23.9ms |
| min | 1.9ms (пустые блоки) |

Запуск:
```bash
./loader2 -dump=dump_100_b1.bin -interval=100 -split="1,3,6,20,1,1"
```

---

## Часть 16: Оптимизированный WS realtime — параллельный парс + WS горутина

**Дата:** 2026-05-22  
**Условия:** 100 блоков (25079097–25079196), REMAP_MOD=16, smp=16 tmpfs, instant gonode.  
**Цель:** снизить latency до ~11ms (1.4ms fetch + 1.0ms parse + 0.3ms transform + 8.6ms save).

### Оптимизации (коммит ed8aad6)

**1. `fetchBlock()` — параллельный JSON парс (rpc.zig)**

До: 3 HTTP потока параллельно → join → 3 parse вызова последовательно (~1.7ms)  
После: 3 HTTP потока параллельно → join → 3 parse потока параллельно (~1.0ms) → -0.7ms

```
HTTP done → spawn parse_thread[0..2] → join → data ready
```

Дополнительно: стековые буферы вместо gpa.alloc() → устранены 6 mmap syscalls для n=1.

**2. `runRealtimeWs()` — WS listener в отдельном потоке (realtime.zig)**

До: WS recv и processBlock в одном потоке — WS блокировал fetch.  
После: отдельный поток wsListenerThread пишет block_num в OS pipe, main читает из pipe.

```
Thread A (wsListenerThread):
  while: conn.nextBlockNum() → write(pipe, block_num)

Thread B (main, critical path):
  while: read(pipe, block_num) → t0=now() → fetchBlock → transform → saveBatch → redis
```

OS pipe: zero CPU spin (блокируется в read), нет мьютексов, нет кольцевого буфера.

### Результаты — 3 прогона подряд (instant gonode, TRUNCATE между тестами)

| Прогон | fetch avg | save avg | **total avg** | total min | total max |
|--------|----------|---------|-------------|----------|----------|
| Run 1 | 1.4 ms | 8.7 ms | **11.9 ms** | 5.4 ms | 37.2 ms |
| Run 2 | 1.4 ms | 9.0 ms | **12.4 ms** | 5.1 ms | 44.8 ms |
| Run 3 | 1.4 ms | 7.9 ms | **11.5 ms** | 4.8 ms | 32.1 ms |
| **Среднее** | **1.4 ms** | **8.5 ms** | **11.9 ms** | — | — |

> `total_ms > fetch_ms + save_ms` на ~1.5ms — разница = JSON parse (~1.0ms) + Redis write (~0.3ms) + overhead. Parse не входит в `fetch_ms` (HTTP only), но входит в `total_ms`.

### Итоговое сравнение — все WS тесты

| Версия | Gonode | fetch avg | save avg | **total avg** | Δ vs предыдущей |
|--------|--------|----------|---------|-------------|----------------|
| WS (первый тест, drip-feed) | drip-feed 100ms | 2.1ms | 11.7ms | 16.4ms | baseline |
| WS (исправлен, instant) | мгновенный | 1.4ms | 8.6ms | 12.1ms | −4.3ms |
| WS 10 блок/сек (3 прогона avg) | drip-feed 100ms | 2.1ms | 11.6ms | 16.3ms | drip-feed overhead |
| **WS оптимизирован (parallel parse + goroutine)** | **мгновенный** | **1.4ms** | **8.5ms** | **11.9ms** | **−0.2ms** |

### Critical path (wall-clock breakdown)

```
t0 = now()
    │
    ├── [spawn 3 HTTP threads] ──────────────────── 1.4ms (parallel)
    │   eth_getBlockByNumber
    │   eth_getBlockReceipts
    │   trace_block
    │
    ├── [spawn 3 parse threads] ─────────────────── ~1.0ms (parallel)
    │   parseBlockRespZC
    │   parseReceiptsRespZC
    │   parseTracesRespZC
    │
    ├── [transform] ─────────────────────────────── ~0.3ms (single)
    │
    └── [spawn 32 CQL workers] ──────────────────── 8.5ms avg (parallel)
        UNLOGGED BATCH × 6 tables

total avg = 11.9ms  (best run: 11.5ms)
```

### Вывод

Параллельный parse дал −0.7ms на `fetch_ms` (не видно — parse не в fetch_ms), но снизил `total_ms`.  
WS горутина устранила блокировку WS recv на critical path.  
Лучший результат: **11.5ms** (Run 3). Цель 11ms практически достигнута — оставшиеся 0.5ms это Redis write overhead.

**Сравнение с TS1 realtime: 138.7ms → 11.9ms = 11.7× быстрее.**

---

## Часть 17: Тест на реальном сервере — реальный диск, smp=32

**Дата:** 2026-05-24  
**Сервер:** lotos-archive-01, 48 ядер, 256 GB RAM, Ubuntu  
**Scylla:** Docker 6.2, smp=32, memory=128G, `developer-mode=1`, **реальный диск** (без tmpfs, без bypass-fsync)  
**Gonode:** instant (все 100 блоков сразу), локально на том же сервере  
**Парсер:** zigparser2, REALTIME=1, WS_URL=ws://127.0.0.1:8545/ws  
**Чанкование:** chunk = block_number / 1000 (TS-совместимое, REMAP_MOD не задан)  
**pool_size=32, split=1,3,6,20,1,1**

### Результаты — 2 прогона (TRUNCATE между тестами)

| Прогон | fetch avg | save avg | **total avg** | total min | total max |
|--------|----------|---------|-------------|----------|----------|
| Run 1 | 3.4 ms | 18.2 ms | **26.8 ms** | 10.4 ms | 49.5 ms |
| Run 2 | 4.0 ms | 20.6 ms | **30.2 ms** | 13.1 ms | 57.3 ms |
| **Среднее** | **3.7 ms** | **19.4 ms** | **28.5 ms** | — | — |

### Анализ

**fetch 3.4–4.0ms** (vs 1.4ms на tmpfs):  
Gonode локальный, min=1.4–1.6ms совпадает с WSL-тестами. Среднее выше из-за Go GC пауз на 48-ядерном сервере.

**save 18–20ms** (vs 8.5ms на tmpfs):  
Реальный диск + fsync — в **2.1–2.4× медленнее** tmpfs. Ожидаемо для продакшна.

**total gap** (`total − fetch − save ≈ 5–6ms` vs ~1.5ms локально):  
JSON парс медленнее на другом CPU (page cache gonode файла не прогрет после рестарта), Redis write overhead, другая архитектура.

### Сравнение сред

| Среда | Конфиг | save avg | total avg | Примечание |
|-------|--------|---------|---------|-----------|
| WSL2 tmpfs | smp=16, bypass-fsync | 8.5 ms | **11.9 ms** | RAM-диск, разработка |
| **Сервер реальный диск** | **smp=32, fsync** | **19.4 ms** | **28.5 ms** | **продакшн-конфиг** |
| Разница | | 2.3× | 2.4× | — |

### Прогноз с реальной нодой

На реальной ноде (RTT ~1–5ms для collocated):
- fetch: ~2–5ms (RTT × 1, 3 запроса параллельно)
- save: ~19ms (диск, стабильно)
- total: **~25–30ms**

WS vs polling при реальном темпе блоков (mainnet ~12 сек/блок):  
Polling добавляет POLL_MS/2 ожидания → WS всегда быстрее на production.

---

## Часть 18: pool=48 на реальном сервере — влияние pool_size на save latency

**Дата:** 2026-05-25  
**Сервер:** lotos-archive-01, 48 ядер, 256 GB RAM  
**Scylla:** Docker 6.2, smp=32, memory=128G, `developer-mode=1`, **реальный диск**  
**Парсер:** zigparser2_p48, REALTIME=1, WS_URL=ws://127.0.0.1:8545/ws  
**Чанкование:** chunk = block_number / 1000 (TS-совместимое)  
**pool_size=48, split=1,4,8,30,3,2**

### Результаты — 1 прогон

| Прогон | fetch avg | save avg | **total avg** | total min | total max |
|--------|----------|---------|-------------|----------|----------|
| Run 1 | 3.3 ms | 17.0 ms | **25.3 ms** | 12.9 ms | 47.0 ms |

### Сравнение pool=32 vs pool=48 (реальный диск, smp=32)

| pool_size | split | save avg | total avg | Δ save |
|-----------|-------|---------|---------|--------|
| 32 | 1,3,6,20,1,1 | 19.4 ms (avg 2 run) | 28.5 ms | — |
| **48** | **1,4,8,30,3,2** | **17.0 ms** | **25.3 ms** | **−2.4 ms** |

### Анализ

Увеличение pool_size с 32 до 48 дало **−2.4ms save** (~12% улучшение).  
Bottleneck остаётся тот же: все 100 тестовых блоков в одном chunk 25079 → одна Scylla shard.  
Дополнительные соединения позволяют чуть эффективнее параллелить батчи по таблицам (split распределяет по 6 таблицам).

На продакшне с разными чанками эффект от pool=48 будет выше — разные шарды смогут обслуживаться параллельно.
