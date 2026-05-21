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

## Итоговая сводка лучших результатов

| Инструмент | Конфиг | sharding | ms/block |
|-----------|--------|---------|---------|
| **loader firehose** | smp=16, 5G, tmpfs | **remap-mod=16** | **5.27ms** |
| **loader firehose** | smp=8, 10G, tmpfs | **remap-mod=8** | **5.36ms** |
| loader saveBatch | smp=8/16, tmpfs | remap-mod=8/16, 16 блоков | ~5.6ms |
| loader firehose | smp=8, 10G, tmpfs | chunk_size=10 (101 chunks) | 8.44ms |
| **zigtest2 parser** | smp=8, 10G, tmpfs | **REMAP_MOD=8** | **8.75ms** |
| zigtest2 parser | smp=8, 10G, tmpfs | chunk_size=1000 (2 chunks) | 13.8ms |
| loader firehose | smp=8, 10G, tmpfs | chunk_size=1000 | 14.5ms |
| loader старый (pipelined EXECUTE) | smp=8, 10G | chunk_size=1000 | 24.8ms |

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

## Путь к 5ms/block на текущей машине

Loader с chunk_size=10 уже даёт 8.2ms/block. Для парсера (chunk_size=1000 в проде):

| Вариант | Ожидаемый результат | Статус |
|---------|--------------------|----|
| loader firehose + chunk_size=10 | **8.2ms** ✓ | Готово |
| Параллельная загрузка N disjoint chunk-диапазонов | N×ускорение | Реализуемо |
| Production машина (smp=32, 128GB) | все 32 шарда активны при prod chunk_size | Прод среда |
