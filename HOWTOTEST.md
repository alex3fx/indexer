# HOWTOTEST — Руководство по запуску бенчмарков

Сравниваются два парсера EVM-блокчейна:

| Парсер | Стек | Файлы |
|--------|------|-------|
| **zigparser2** | Zig 0.17, нативный CQL, UNLOGGED BATCH | `zigtest2/` |
| **TS1** | Bun + TypeScript, cassandra-driver, BullMQ | `packtest/` (benchmark) / `input/.../parser/` (исходник) |

---

## 1. Настройка окружения

### 1.1 Zig-компилятор

```bash
ZIG=/home/alex/lotos/zig-x86_64-linux-0.17.0-dev.263+0add2dfc4/zig
$ZIG version   # должно вывести 0.17.0-dev.263+0add2dfc4
```

### 1.2 ScyllaDB (нативный WSL2, tmpfs)

ScyllaDB установлен нативно в WSL2. Данные хранятся в `/dev/shm/scylla/data` (tmpfs — RAM-диск).

**Конфигурация** (`/etc/default/scylla-server`):
```
SCYLLA_ARGS="--log-to-syslog=1 --log-to-stdout=0 --default-log-level=info \
  --network-stack=posix --smp=16 --unsafe-bypass-fsync=1 --overprovisioned \
  --max-concurrent-requests-per-shard=65536"
```

Важно: `smp=16` требует ≥5 GB RAM. Без `--unsafe-bypass-fsync=1` на tmpfs будет медленнее.

**Запуск / остановка:**
```bash
sudo systemctl start scylla-server
sudo systemctl stop scylla-server
sudo systemctl is-active scylla-server   # проверка
nodetool status                          # статус кластера
```

**Адрес для подключения:** `172.31.208.104:9142`  
(Находим текущий IP: `hostname -I | awk '{print $1}'`)

**Создание схемы** (после каждого рестарта WSL — tmpfs очищается):
```bash
cqlsh 172.31.208.104 9142 -f /mnt/c/Users/Public/zig20260521/schema.cql
# Проверка:
cqlsh 172.31.208.104 9142 -e "USE eth; DESCRIBE TABLES;"
```

### 1.3 Redis / DragonflyDB

Используется для хранения курсора `LATEST_PROCESSED_BLOCK_NUMBER`.

**Запуск DragonflyDB в Docker (host network):**
```bash
docker run --rm -d --name temp_dragonfly \
  --network host \
  docker.dragonflydb.io/dragonflydb/dragonfly:latest \
  --bind=0.0.0.0 --port=6379 --requirepass=mockpass

# Проверка:
nc -z 127.0.0.1 6379 && echo "OK"
```

**Установить начальный курсор** (блоки 25078197–25079196 в данных):
```bash
docker exec temp_dragonfly redis-cli -a mockpass SET LATEST_PROCESSED_BLOCK_NUMBER 25078196
```

### 1.4 Mock RPC-нода (gonode)

Отдаёт блоки из JSON-файла. Поддерживает `eth_getBlockByNumber`, `eth_getBlockReceipts`, `trace_block`.

**Запуск:**
```bash
MOCK_DATA_FILE=/home/alex/lotos/task1/packtest/data/blocks_fresh_1000.json \
CHAIN_ID=1 \
  /home/alex/lotos/task1/mocknode/gonode/gonode > /tmp/gonode.log 2>&1 &

# Ждём загрузки (файл 3.2 GB, ~40 секунд):
until nc -z 127.0.0.1 8545; do sleep 3; done && echo "gonode ready"
```

**Диапазон блоков в данных:** 25078197–25079196 (1000 блоков).

### 1.5 Bun (для TS1)

```bash
curl -fsSL https://bun.sh/install | bash
# или:
~/.bun/bin/bun --version
```

---

## 2. Бенчмарк zigparser2

### 2.1 Сборка

```bash
ZIG=/home/alex/lotos/zig-x86_64-linux-0.17.0-dev.263+0add2dfc4/zig
cd /home/alex/lotos/task1/zigtest2

$ZIG build -Doptimize=ReleaseFast -Dpool_size=32 -Dsplit="1,3,6,20,1,1"
```

### 2.2 Процедура бенчмарка

1. Запустить Scylla, Redis, gonode (см. раздел 1)
2. **Warmup run** (discarded):
```bash
docker exec temp_dragonfly redis-cli -a mockpass SET LATEST_PROCESSED_BLOCK_NUMBER 25079090
cqlsh 172.31.208.104 9142 << 'EOF'
USE eth;
TRUNCATE blocks; TRUNCATE transactions; TRUNCATE logs;
TRUNCATE internal_transactions; TRUNCATE contracts; TRUNCATE contracts_by_addresses;
EOF
sleep 6   # дать ScyllaDB прийти в себя после truncate

CM_CONNECTION_URL="redis://:mockpass@127.0.0.1:6379/0" \
SCYLLA_DB_CONTACT_POINTS='["172.31.208.104:9142"]' \
SCYLLA_DB_KEYSPACE=eth RPC_URL=http://127.0.0.1:8545 CHAIN_ID=1 \
TO_BLOCK=25079106 BATCH_SIZE=16 REMAP_MOD=16 PIPELINE=2 \
./zig-out/bin/zigparser2
```

3. **Замер** (TRUNCATE + settle + run):
```bash
cqlsh 172.31.208.104 9142 << 'EOF'
USE eth;
TRUNCATE blocks; TRUNCATE transactions; TRUNCATE logs;
TRUNCATE internal_transactions; TRUNCATE contracts; TRUNCATE contracts_by_addresses;
EOF
sleep 6

docker exec temp_dragonfly redis-cli -a mockpass SET LATEST_PROCESSED_BLOCK_NUMBER 25078196

CM_CONNECTION_URL="redis://:mockpass@127.0.0.1:6379/0" \
SCYLLA_DB_CONTACT_POINTS='["172.31.208.104:9142"]' \
SCYLLA_DB_KEYSPACE=eth RPC_URL=http://127.0.0.1:8545 CHAIN_ID=1 \
TO_BLOCK=25079196 BATCH_SIZE=16 REMAP_MOD=16 PIPELINE=2 \
./zig-out/bin/zigparser2
```

### 2.3 Ключевые env-переменные

| Переменная | Значение | Описание |
|-----------|---------|---------|
| `PIPELINE=N` | 1/2/4 | Параллельных fetch-воркеров (оптимум=2 для localhost) |
| `REMAP_MOD=N` | 16 | `chunk = block % N`, N=smp для оптимума |
| `BATCH_SIZE=N` | 16 | Блоков в батче |
| `TO_BLOCK=N` | 25079196 | Последний блок |

### 2.4 Текущие лучшие результаты (smp=16, tmpfs, 1000 блоков)

| Конфигурация | ms/block | FBDR avg |
|-------------|---------|---------|
| PIPELINE=4 REMAP=16 BATCH=16 | 6.18 ms | 244ms (gonode перегружен) |
| **PIPELINE=2 REMAP=16 BATCH=16** | **5.58 ms** | ~100ms |
| PIPELINE=1 REMAP=16 BATCH=16 | 6.06 ms | 40ms |

> **Примечание:** С PIPELINE=4 gonode-мок перегружен (FBDR=244ms vs 40ms у PIPELINE=1).  
> На реальной ноде с RTT 50-200ms PIPELINE=4 ожидаемо лучше.

---

## 3. Бенчмарк TS1 (packtest)

packtest — профилировщик TS1 с мок-данными. Измеряет **TPT** (fetch+transform),  
без реальных запросов к Scylla (BullMQ и Scylla заменены моками).

### 3.1 Запуск

```bash
cd /home/alex/lotos/task1/packtest

# 1000 блоков из директории (экономит RAM):
MOCK_DATA_DIR=data/blocks_fresh_1000_dir \
CHAIN_ID=1 RAW_CHUNK_SIZE=1000 \
  bun run ./src/entrypoints/collecting/historical.ts
# или:
bun run run:fresh:1000
```

### 3.2 Результат (1000 блоков)

```
TPT total:       ~4700 ms
TPT avg/block:   ~4.7 ms
FBDR avg:        ~0.2 ms  (чтение файла, не HTTP)
```

### 3.3 Сравнение с zigparser2

| Метрика | TS1 (packtest, файл) | TS1 (полный, gonode+Scylla) | zigparser2 PIPELINE=2 |
|---------|---------------------|---------------------------|----------------------|
| ms/block | ~4.7 ms (только transform) | **27.85 ms** (fetch+save) | **5.58 ms** (fetch+save) |
| Fetch из ноды | ❌ файл | ✅ HTTP gonode | ✅ HTTP gonode |
| Запись в ScyllaDB | ❌ мок | ✅ cassandra-driver EXECUTE | ✅ нативный CQL UNLOGGED BATCH |
| Измерено на | — | 2026-05-22, native Scylla smp=16 tmpfs | 2026-05-22, native Scylla smp=16 tmpfs |

**Итог: zigparser2 быстрее TS1 в 27.85 / 5.58 ≈ 5.0x**

---

## 4. Полный TS1 с реальной ScyllaDB

Исходник: `mocknode/src/entrypoints/`  
Два процесса: `historical.ts` (продюсер) + `save.ts` (консьюмер, N воркеров).

### 4.1 Готовый скрипт

```bash
cd /home/alex/lotos/task1/mocknode

# DragonflyDB ДОЛЖЕН стартовать с флагами для BullMQ:
docker stop temp_dragonfly 2>/dev/null || true
docker run --rm -d --name temp_dragonfly --network host \
  docker.dragonflydb.io/dragonflydb/dragonfly:latest \
  --bind=0.0.0.0 --port=6379 \
  --lock_on_hashtags --cluster_mode=emulated \
  --requirepass=mockpass

# Запустить тест (5 воркеров, 1000 блоков):
bash scripts/run_ts1_bench.sh 5 1000

# Или 100 блоков:
bash scripts/run_ts1_bench.sh 5 100
```

### 4.2 Требования

- DragonflyDB на `127.0.0.1:6379` с `--cluster_mode=emulated --lock_on_hashtags` (для BullMQ Lua scripts)
- gonode на `127.0.0.1:8545`
- ScyllaDB на `172.31.208.104:9142`
- `bun` установлен: `~/.bun/bin/bun`

### 4.3 Результаты (2026-05-22, native Scylla smp=16 tmpfs)

```
Total wall-clock : 27848 ms  (1000 blocks)
ms/block         : 27.85 ms
Workers          : 5
historical.ts    : 14915 ms (fetch+transform+BullMQ)
save workers     : 12933 ms (cassandra-driver INSERT после historical.ts)
```

| TS1 компонент | Время |
|--------------|-------|
| FBDR avg/block (HTTP gonode) | 34.6 ms |
| Transform avg/batch | 52.0 ms |
| BullMQ addJob avg | 41.0 ms |
| TPT total (historical.ts) | 14915 ms / 10.6 ms per block |
| Save total (5 workers async) | ~12900 ms |
| **Полный wall-clock** | **27848 ms / 27.85 ms/block** |

---

## 5. Realtime-тест (1 блок за раз)

### 5.1 zigparser2 realtime

```bash
cd /home/alex/lotos/task1/zigtest2
# REALTIME=1 — обрабатывает по 1 блоку, цикл до TO_BLOCK
CM_CONNECTION_URL="redis://:mockpass@127.0.0.1:6379/0" \
SCYLLA_DB_CONTACT_POINTS='["172.31.208.104:9142"]' \
SCYLLA_DB_KEYSPACE=eth RPC_URL=http://127.0.0.1:8545 CHAIN_ID=1 \
TO_BLOCK=25079196 REMAP_MOD=16 REALTIME=1 POLL_MS=500 \
./zig-out/bin/zigparser2
```

Результаты (100 блоков, REMAP_MOD=16, smp=16 tmpfs, gonode localhost):

| Фаза | avg | min | max |
|------|-----|-----|-----|
| Fetch (3×HTTP parallel) | 1.4 ms | 0.6 ms | 7.6 ms |
| Transform | 0.3 ms | — | — |
| Save (UNLOGGED BATCH) | 8.5 ms | 3.4 ms | 40.2 ms |
| **TOTAL (запрос → DB)** | **11.8 ms** | 4.9 ms | 45.2 ms |

> POLL_MS не влияет на latency, когда блок уже готов — используется только при ожидании нового блока.

### 5.2 TS1 realtime (BATCH_SIZE=1)

```bash
cd /home/alex/lotos/task1/mocknode
# DragonflyDB с --cluster_mode=emulated --lock_on_hashtags обязателен для BullMQ
bash scripts/run_ts1_realtime.sh
```

Результаты (100 блоков, BATCH_SIZE=1, 1 воркер, gonode localhost):

| Фаза | avg | min | max |
|------|-----|-----|-----|
| FBDR (3×HTTP + JSON parse) | 6.4 ms | 1.0 ms | 12.0 ms |
| Transform (JS loop) | 4.2 ms | — | — |
| BullMQ addJob | 6.7 ms | — | — |
| **TPT** (fetch+transform+BullMQ) | **10.6 ms** | 3.0 ms | 20.0 ms |
| Save (cassandra-driver EXECUTE) | 127.8 ms | 23.0 ms | 273.0 ms |
| **TOTAL (запрос → DB)** | **138.7 ms** | 32.0 ms | 286.0 ms |

Wall-clock throughput: 25.5 ms/block (save перекрывается через BullMQ)

### 5.3 Realtime: сравнение

| Метрика | TS1 (BATCH_SIZE=1) | zigparser2 | Разница |
|---------|-------------------|------------|---------|
| Fetch avg | 6.4 ms | 1.4 ms | 4.6× |
| Transform avg | 4.2 ms | 0.3 ms | 14× |
| Queue overhead | 6.7 ms (BullMQ) | 0 ms | — |
| **Save avg** | **127.8 ms** | **8.5 ms** | **15×** |
| **Per-block latency** | **138.7 ms** | **11.8 ms** | **11.7×** |
| Wall-clock throughput | 25.5 ms/block | 11.8 ms/block | 2.2× |

**Почему save в 15× медленнее у TS1:**  
cassandra-driver делает `Promise.allSettled` на каждые 100 строк — это **28 sequential round-trips** для ~2800 строк одного блока. zigparser2 отправляет те же 28 UNLOGGED BATCH фреймов **параллельно** через pool=32.

---

## 6. Итоговое сравнение парсеров

Все измерения: gonode HTTP, native ScyllaDB smp=16 tmpfs, 2026-05-22.

### Historical (1000 блоков)

| Парсер | Конфигурация | ms/block | vs TS1 |
|--------|-------------|---------|--------|
| TS1 | 5 workers, BullMQ, cassandra-driver | **27.85 ms** | 1.0× |
| zigparser2 PIPELINE=1 | REMAP=16, BATCH=16 | 6.06 ms | 4.6× |
| **zigparser2 PIPELINE=2** | **REMAP=16, BATCH=16** | **5.58 ms** | **5.0×** |
| zigparser2 PIPELINE=4 | REMAP=16, BATCH=16 | 6.18 ms | 4.5× |

### Realtime (1 блок за раз, 100 блоков)

| Парсер | Per-block latency | Wall-clock throughput |
|--------|-----------------|----------------------|
| TS1 (BATCH_SIZE=1, 1 worker) | 138.7 ms | 25.5 ms/block |
| **zigparser2** (REALTIME=1) | **11.8 ms** | **11.8 ms/block** |
| **Speedup** | **11.7×** | **2.2×** |

**Ключевое отличие CQL path:**
- TS1: `cassandra-driver.execute()` → 28 sequential `Promise.allSettled` × 100 строк
- zigparser2: `UNLOGGED BATCH` → 28 параллельных фреймов через pool=32

---

## 7. Валидация данных

После записи — сверить с prod CSV:

```bash
python3 /mnt/c/Users/Public/zig20260521/validate_db.py
```

---

## 8. Типичные проблемы

| Симптом | Причина | Решение |
|---------|---------|---------|
| `NoStartBlock` у zigparser2 | Redis пустой или нет ключа | `SET LATEST_PROCESSED_BLOCK_NUMBER 25078196` |
| Scylla не стартует | Недостаточно RAM для smp=16 | Нужно ≥5 GB free (при smp=8 хватит 2.5 GB) |
| gonode не отвечает | Файл 3.2 GB ещё грузится | Ждать `nc -z 127.0.0.1 8545` |
| `batch too large` в TS1 | bytecodes > 50 KB в contracts | Уменьшить batch для contracts/cba до 10 |
| После рестарта WSL schema пропала | tmpfs очищается | Пересоздать: `cqlsh -f schema.cql` |
| PIPELINE=4 медленнее PIPELINE=2 | gonode перегружен на localhost | Нормально; на реальной ноде PIPELINE=4 лучше |
| BullMQ Lua script error | DragonflyDB без `--cluster_mode=emulated` | Перезапустить с флагами из раздела 4.1 |
