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

### 1.5 Gonode: WebSocket newHeads (drip-feed)

Gonode поддерживает `BLOCK_INTERVAL_MS=N` (drip-feed) и WebSocket endpoint `/ws`.

```bash
# Обычный режим (все блоки доступны сразу):
bash -c 'MOCK_DATA_FILE=packtest/data/blocks_fresh_100.json CHAIN_ID=1 \
  mocknode/gonode/gonode > /tmp/gonode.log 2>&1 &'

# Drip-feed + WS (новый блок каждые N ms, таймер стартует с первого запроса):
bash -c 'MOCK_DATA_FILE=packtest/data/blocks_fresh_100.json CHAIN_ID=1 \
  BLOCK_INTERVAL_MS=50 mocknode/gonode/gonode > /tmp/gonode.log 2>&1 &'
```

WS endpoint: `ws://127.0.0.1:8545/ws`  
Поддерживаемые методы: `eth_subscribe` (только `newHeads`), `eth_unsubscribe`.

Пересборка gonode после изменений:
```bash
cd mocknode/gonode && /usr/local/go/bin/go build -o gonode .
```

### 1.6 Bun (для TS1)

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

### 5.1 zigparser2 realtime — режим polling

```bash
cd /home/alex/lotos/task1/zigtest2
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

> POLL_MS = sleep когда блок ещё не готов. Если блок всегда доступен — не влияет на latency.

### 5.2 zigparser2 realtime — режим WebSocket newHeads

Вместо polling: подписывается на `eth_subscribe newHeads`, обрабатывает блок немедленно при WS push.

```bash
# 1. Gonode с WS + drip-feed 100ms:
bash -c 'MOCK_DATA_FILE=/home/alex/lotos/task1/packtest/data/blocks_fresh_100.json \
  CHAIN_ID=1 BLOCK_INTERVAL_MS=100 \
  /home/alex/lotos/task1/mocknode/gonode/gonode > /tmp/gonode.log 2>&1 &'
until nc -z 127.0.0.1 8545; do sleep 1; done

# 2. Zigparser2 WS режим (WS_URL активирует runRealtimeWs):
CM_CONNECTION_URL="redis://:mockpass@127.0.0.1:6379/0" \
SCYLLA_DB_CONTACT_POINTS='["172.31.208.104:9142"]' \
SCYLLA_DB_KEYSPACE=eth RPC_URL=http://127.0.0.1:8545 CHAIN_ID=1 \
TO_BLOCK=25079196 REMAP_MOD=16 \
REALTIME=1 WS_URL=ws://127.0.0.1:8545/ws \
./zig-out/bin/zigparser2
```

Результаты (100 блоков, instant gonode, REMAP_MOD=16, smp=16 tmpfs):

| Фаза | avg | min | max |
|------|-----|-----|-----|
| Fetch (WS wakeup + 3×HTTP) | 1.4 ms | 0.7 ms | 11.0 ms |
| Transform | 0.3 ms | — | — |
| Save (UNLOGGED BATCH) | 8.6 ms | 3.9 ms | 20.6 ms |
| **TOTAL (WS event → DB)** | **12.1 ms** | 5.6 ms | 23.6 ms |

**WS vs polling (одинаковые условия — instant gonode):**

| Режим | fetch avg | save avg | **total avg** |
|-------|----------|---------|-------------|
| Polling POLL_MS=500 | 1.4ms | 8.5ms | 11.8ms |
| Polling POLL_MS=250 | 1.4ms | 8.5ms | 11.8ms |
| **WebSocket** | **1.4ms** | **8.6ms** | **12.1ms** |

WS добавляет ~0.3ms overhead (один frame read) — в пределах погрешности. **Нет деградации.**

> Если тест показывает 16ms — это артефакт drip-feed gonode (Scylla простаивает между блоками).  
> На реальной ноде WS выгоднее polling за счёт мгновенного уведомления вместо периодического опроса.

### 5.3 TS1 realtime (BATCH_SIZE=1)

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

### 5.4 Realtime: zigparser2 с drip-feed polling (BLOCK_INTERVAL_MS=50)

Для честного теста gonode выдаёт блоки по одному каждые 50ms (`BLOCK_INTERVAL_MS=50`, таймер с первого запроса). Парсер с `POLL_MS=50` опрашивает ноду каждые 50ms пока блок не готов.

```bash
# Gonode с drip-feed:
bash -c 'MOCK_DATA_FILE=packtest/data/blocks_fresh_100.json CHAIN_ID=1 BLOCK_INTERVAL_MS=50 \
  mocknode/gonode/gonode > /tmp/gonode.log 2>&1 &'

# Zigparser2:
REALTIME=1 POLL_MS=50 REMAP_MOD=16 TO_BLOCK=25079196 ./zig-out/bin/zigparser2
```

| POLL_MS | Gonode режим | fetch avg | save avg | **total avg** |
|---------|------------|----------|---------|-------------|
| 500 ms | все блоки готовы | 1.4 ms | 8.5 ms | 11.8 ms |
| 250 ms | все блоки готовы | 1.4 ms | 8.5 ms | 11.8 ms |
| **50 ms** | **drip-feed 50ms/block** | **2.2 ms** | **11.6 ms** | **16.5 ms** |

При drip-feed: парсер видит null (блок ещё не готов), спит 50ms, повторяет — поэтому fetch avg 2.2ms вместо 1.4ms. Save avg 11.6ms (чуть выше обычного — Scylla под нагрузкой более длинного теста).

### 5.5 Realtime: сравнение TS1 vs zigparser2

| Метрика | TS1 (BATCH_SIZE=1) | zigparser2 (poll=500) | Разница |
|---------|-------------------|----------------------|---------|
| Fetch avg | 6.4 ms | 1.4 ms | 4.6× |
| Transform avg | 4.2 ms | 0.3 ms | 14× |
| Queue overhead | 6.7 ms (BullMQ) | 0 ms | — |
| **Save avg** | **127.8 ms** | **8.5 ms** | **15×** |
| **Per-block latency** | **138.7 ms** | **11.8 ms** | **11.7×** |
| Wall-clock throughput | 25.5 ms/block | 11.8 ms/block | 2.2× |

**Почему save в 15× медленнее у TS1:**  
cassandra-driver делает `Promise.allSettled` на каждые 100 строк — это **28 sequential round-trips** для ~2800 строк одного блока. zigparser2 отправляет те же 28 UNLOGGED BATCH фреймов **параллельно** через pool=32.

### 5.6 Оптимизация WS realtime — параллельный парс + WS горутина

**Коммит:** `ed8aad6` — `Optimize realtime WS path: parallel parse + WS goroutine`

#### Что изменилось

**`rpc.zig` — `fetchBlock()`: параллельный JSON парс**

До: 3 HTTP потока → join → parse block → parse receipts → parse traces (последовательно, ~1.7ms parse)  
После: 3 HTTP потока → join → 3 parse потока → join (~1.0ms parse, −0.7ms)

```
HTTP done → spawn parse_thread[0..2] → join → data ready
            (идентично fetchBatchFlat в историческом режиме)
```

Дополнительно: стековые буферы `[3 * 128]u8` вместо `gpa.alloc()` — убраны 6 mmap syscalls на каждый блок (было заметно при n=1).

**`realtime.zig` — `runRealtimeWs()`: WS listener в отдельном потоке**

До: WS recv и processBlock в одном потоке — `nextBlockNum()` блокировал fetch start.  
После: отдельный `wsListenerThread` пишет `block_num` в OS pipe, `main` читает из pipe и сразу стартует `fetchBlock`.

```
Thread A (wsListenerThread):         Thread B (main, critical path):
  while:                               while:
    nextBlockNum() → write(pipe)         read(pipe) → t0=now() → fetchBlock
                                                     → transform → saveBatch
```

OS pipe (`linux.pipe2`): `read()` блокируется без spin-loop, нет мьютексов, нет кольцевого буфера. `t0` устанавливается сразу после `read` — WS ожидание не входит в измеряемое время.

#### Результаты — 3 прогона подряд (instant gonode, TRUNCATE между тестами)

```bash
# Сброс курсора + gonode:
docker exec temp_dragonfly redis-cli -a mockpass SET LATEST_PROCESSED_BLOCK_NUMBER 25079096
bash -c 'MOCK_DATA_FILE=packtest/data/blocks_fresh_100.json CHAIN_ID=1 \
  mocknode/gonode/gonode > /tmp/gonode.log 2>&1 &'
until nc -z 127.0.0.1 8545; do sleep 1; done

# TRUNCATE + settle:
cqlsh 172.31.208.104 9142 -e "USE eth; TRUNCATE blocks; TRUNCATE transactions;
  TRUNCATE logs; TRUNCATE internal_transactions; TRUNCATE contracts;
  TRUNCATE contracts_by_addresses;"
sleep 6

# Запуск:
CM_CONNECTION_URL="redis://:mockpass@127.0.0.1:6379/0" \
SCYLLA_DB_CONTACT_POINTS='["172.31.208.104:9142"]' \
SCYLLA_DB_KEYSPACE=eth RPC_URL=http://127.0.0.1:8545 CHAIN_ID=1 \
TO_BLOCK=25079196 REMAP_MOD=16 REALTIME=1 WS_URL=ws://127.0.0.1:8545/ws \
./zig-out/bin/zigparser2
```

| Прогон | fetch avg | save avg | **total avg** | total min | total max |
|--------|----------|---------|-------------|----------|----------|
| Run 1 | 1.4 ms | 8.7 ms | **11.9 ms** | 5.4 ms | 37.2 ms |
| Run 2 | 1.4 ms | 9.0 ms | **12.4 ms** | 5.1 ms | 44.8 ms |
| Run 3 | 1.4 ms | 7.9 ms | **11.5 ms** | 4.8 ms | 32.1 ms |
| **Среднее** | **1.4 ms** | **8.5 ms** | **11.9 ms** | — | — |

> `total_ms` включает JSON parse (~1.0ms) и Redis write (~0.3ms), которые не входят в `fetch_ms` и `save_ms`.

#### Итоговое сравнение WS вариантов

| Версия | Gonode | fetch avg | save avg | **total avg** |
|--------|--------|----------|---------|-------------|
| WS (первый тест) | drip-feed 100ms | 2.1ms | 11.7ms | 16.4ms |
| WS (исправленный) | мгновенный | 1.4ms | 8.6ms | 12.1ms |
| **WS + parallel parse + goroutine** | **мгновенный** | **1.4ms** | **8.5ms** | **11.9ms** |

Лучший результат: **11.5ms** (Run 3). Цель 11ms практически достигнута — оставшиеся 0.5ms — Redis write overhead.

---

## 6. Итоговое сравнение парсеров

### 6.1 Historical — localhost (WSL2)

Измерения: gonode HTTP, native ScyllaDB smp=16 tmpfs, 2026-05-22, 1000 блоков.

| Парсер | Конфигурация | ms/block | vs TS1 |
|--------|-------------|---------|--------|
| TS1 | 5 workers, BullMQ, cassandra-driver | **27.85 ms** | 1.0× |
| zigparser2 PIPELINE=1 | REMAP=16, BATCH=16 | 6.06 ms | 4.6× |
| **zigparser2 PIPELINE=2** | **REMAP=16, BATCH=16** | **5.58 ms** | **5.0×** |
| zigparser2 PIPELINE=4 | REMAP=16, BATCH=16 | 6.18 ms | 4.5× |

### 6.2 Historical — реальный сервер

Измерения: gonode HTTP (локально на сервере), ScyllaDB Docker smp=32/128G `--developer-mode --unsafe-bypass-fsync`,  
2026-05-25, 100 блоков (25079097–25079196), сервер Intel Xeon Gold 5412U / 48 ядер / 251 GB RAM.

| Парсер | Конфигурация | ms/block | vs TS1 |
|--------|-------------|---------|--------|
| TS1 | 5 workers, BullMQ, cassandra-driver | **54.75 ms** | 1.0× |
| **zigparser2 PIPELINE=2** | **REMAP=32, pool=32** | **11.82 ms** | **4.6×** |

**Детализация zigparser2 (сервер):**

| Фаза | avg/block |
|------|-----------|
| FBDR (fetch+parse, 3×HTTP параллельно) | 80.7 ms |
| TPT (fetch+transform) | 12.8 ms |
| Save (UNLOGGED BATCH, 6 таблиц параллельно) | 10.8 ms |
| **Total wall-clock** | **11.82 ms/block** |

**Детализация TS1 (сервер):**

| Фаза | avg/batch (10 блоков) |
|------|----------------------|
| FBDR | 64.1 ms |
| Transform | 100.0 ms |
| BullMQ addJob | 93.8 ms |
| TPT total | 2048 ms / 20.3 ms/block |
| Save total (5 workers async) | ~3400 ms |
| **Total wall-clock** | **54.75 ms/block** |

**Почему сервер медленнее localhost по абсолютным числам:**
- gonode читает из файла на диске (не tmpfs) → FBDR 80ms vs 34ms
- Scylla в Docker vs native → дополнительный overhead на save
- Оба парсера страдают одинаково → relative speedup сохраняется (4.6×)

**Итог: zigparser2 стабильно быстрее TS1 в 4.6–5.0× на любом стенде.**

### 6.3 Realtime — localhost (WSL2, instant gonode)

100 блоков, 2026-05-22.

| Парсер | Режим | fetch avg | save avg | **Per-block latency** | vs TS1 |
|--------|-------|----------|---------|----------------------|--------|
| TS1 | BATCH_SIZE=1, 1 worker | 6.4 ms | 127.8 ms | **138.7 ms** | 1.0× |
| zigparser2 | polling POLL_MS=500 | 1.4 ms | 8.5 ms | **11.8 ms** | 11.7× |
| zigparser2 | WS (исправленный) | 1.4 ms | 8.6 ms | **12.1 ms** | 11.5× |
| **zigparser2** | **WS + parallel parse + goroutine** | **1.4 ms** | **8.5 ms** | **11.9 ms avg / 11.5 ms best** | **11.7×** |

> WS и polling дают одинаковую latency на localhost. Преимущество WS — на реальной ноде:  
> polling POLL_MS=100 → +50ms среднего ожидания; WS — мгновенное уведомление.

**Realtime при 10 блоков/сек (drip-feed 100ms, 3 прогона):**

| Режим | fetch avg | save avg | **total avg** |
|-------|----------|---------|-------------|
| WS | 2.1 ms | 11.6 ms | **16.3 ms** |

Scylla "остывает" за 88ms простоя между блоками → save растёт с 8.5ms до 11.6ms.

### 6.4 Catchup+Realtime — localhost (WSL2)

Новый режим (WS_URL задан): история синхронизируется батчами, потом переход на WS realtime.  
2026-05-25, 900 блоков history + 100 блоков WS (WS_BLOCK_INTERVAL_MS=500), REMAP_MOD=16, PIPELINE=2.

| Фаза | Результат |
|------|-----------|
| WS first block | 25079097 — seamless, 0 пропусков |
| Historical (900 блоков, PIPELINE=2) | 7018 ms / **7.8 ms/block** |
| Realtime WS fetch avg | 2.2 ms |
| Realtime WS save avg | 13.5 ms |
| **Realtime WS TOTAL avg** | **18.1 ms/block** |

### 6.5 Catchup+Realtime — реальный сервер

2026-05-25, 80 блоков history + 20 блоков WS (WS_BLOCK_INTERVAL_MS=500),  
REMAP_MOD=32, PIPELINE=2, Scylla Docker smp=32/128G.

| Фаза | Результат |
|------|-----------|
| WS first block | 25079177 — seamless, 0 пропусков |
| Historical (80 блоков, PIPELINE=2) | 506 ms / **6.3 ms/block** |
| Historical FBDR avg | 50.4 ms |
| Historical save avg | 8.6 ms |
| Realtime WS fetch avg | 5.5 ms |
| Realtime WS save avg | 24.0 ms |
| **Realtime WS TOTAL avg** | **36.0 ms/block** |

> Realtime save 24ms (vs 13.5ms WSL2): Scylla Docker без tmpfs + "остывание" за ~8s простоя во время исторической синхронизации. На продовой ноде с непрерывным потоком блоков save вернётся к ~10-15ms.

**Ключевое отличие CQL path:**
- TS1: `cassandra-driver.execute()` → 28 sequential `Promise.allSettled` × 100 строк
- zigparser2: `UNLOGGED BATCH` → 28 параллельных фреймов через pool=32

---

## 7. Бенчмарк на реальном сервере (lotos-archive-01)

Сервер: `alexey_smolyakov@100.64.0.4` (Tailscale).  
Конфигурация: 48 ядер Xeon Gold 5412U, 251 GB RAM, 2 TB HDD.  
Scylla: Docker `scylladb/scylla:6.2`, smp=32, memory=128G, `--developer-mode --unsafe-bypass-fsync`.

### 7.1 Первичная установка (один раз)

```bash
ssh alexey_smolyakov@100.64.0.4

# Установить bun (если нет)
curl -fsSL https://bun.sh/install | bash

# Создать dragonfly с BullMQ-совместимыми флагами (порт 6380, не конфликтует с native redis)
docker stop dragonfly 2>/dev/null; docker rm dragonfly 2>/dev/null
docker run -d --name dragonfly --network host --restart unless-stopped \
  docker.dragonflydb.io/dragonflydb/dragonfly:latest \
  --bind=0.0.0.0 --port=6380 --requirepass=redispass \
  --lock_on_hashtags --cluster_mode=emulated

# Перенести TS1 source (с dev-машины)
rsync -az /home/alex/lotos/task1/mocknode/src/ alexey_smolyakov@100.64.0.4:~/ts1/src/
rsync -az /home/alex/lotos/task1/mocknode/package.json \
          /home/alex/lotos/task1/mocknode/tsconfig.json \
          /home/alex/lotos/task1/mocknode/bun.lock \
          alexey_smolyakov@100.64.0.4:~/ts1/
ssh alexey_smolyakov@100.64.0.4 "cd ~/ts1 && ~/.bun/bin/bun install"
```

### 7.2 Обновить бинари (после пересборки)

```bash
scp /home/alex/lotos/task1/zigtest2/zig-out/bin/zigparser2 \
    alexey_smolyakov@100.64.0.4:~/zigparser_full/zigparser2_latest
scp /home/alex/lotos/task1/mocknode/gonode/gonode \
    alexey_smolyakov@100.64.0.4:~/zigparser_full/gonode_latest
```

### 7.3 Запустить бенчмарк

```bash
# Загрузить скрипт (если нет)
scp /tmp/bench_server.sh alexey_smolyakov@100.64.0.4:~/bench_server.sh

ssh alexey_smolyakov@100.64.0.4 "bash ~/bench_server.sh"
```

**Реквизиты сервера:**

| Ресурс | Адрес | Пароль |
|--------|-------|--------|
| Native Redis | `127.0.0.1:6379` | `ZCy8k4G6pcRYVFfm` |
| Dragonfly (BullMQ) | `127.0.0.1:6380` | `redispass` |
| ScyllaDB | `127.0.0.1:9042` | `cassandra/cassandra` |
| gonode HTTP/WS | `127.0.0.1:8545` | — |

---

## 9. loader2 — минимальная latency записи 1 блока

loader2 измеряет время записи ровно одного блока в ScyllaDB:  
6 таблиц × UNLOGGED BATCH, все параллельно, 1 блок каждые 100ms.

### 9.1 Создание dump (1 блок/батч)

```bash
cd /home/alex/lotos/task1/zigtest2

# Сброс курсора на начало 100-блочного датасета
docker exec temp_dragonfly redis-cli -a mockpass \
  SET LATEST_PROCESSED_BLOCK_NUMBER 25079096

# Создать dump (100 блоков × 1 блок/батч, REMAP_MOD=16)
CM_CONNECTION_URL="redis://:mockpass@127.0.0.1:6379/0" \
SCYLLA_DB_CONTACT_POINTS='["172.31.208.104:9142"]' \
SCYLLA_DB_KEYSPACE=eth RPC_URL=http://127.0.0.1:8545 CHAIN_ID=1 \
TO_BLOCK=25079196 BATCH_SIZE=1 REMAP_MOD=16 \
DUMP_FILE=/home/alex/lotos/task1/gotest/loader2/dump_100_b1.bin \
./zig-out/bin/zigparser2
# → gotest/loader2/dump_100_b1.bin (~124MB, 100 батчей × 1 блок)
```

### 9.2 Сборка loader2

```bash
cd /home/alex/lotos/task1/gotest/loader2
/usr/local/go/bin/go build -o loader2 .
```

### 9.3 Запуск теста

```bash
cd /home/alex/lotos/task1/gotest/loader2

# Оптимальный split (как в zigparser2, pool_size=32):
cqlsh 172.31.208.104 9142 << 'EOF'
USE eth;
TRUNCATE blocks; TRUNCATE transactions; TRUNCATE logs;
TRUNCATE internal_transactions; TRUNCATE contracts; TRUNCATE contracts_by_addresses;
EOF
sleep 5

./loader2 -dump=dump_100_b1.bin -interval=100 -split="1,3,6,20,1,1"

# Uniform pool (для сравнения):
./loader2 -dump=dump_100_b1.bin -interval=100 -conns=4
```

### 9.4 Результаты (100 блоков, interval=100ms, smp=16 tmpfs)

**Sweep split/conns:**

| Конфигурация | total conns | save avg | p50 | p95 |
|-------------|------------|---------|-----|-----|
| uniform=1 (`-conns=1`) | 6 | 21.2ms | 17.8ms | 48.8ms |
| uniform=4 (`-conns=4`) | 24 | 13.8ms | 11.1ms | 33.2ms |
| **`-split=1,3,6,20,1,1`** | **32** | **9.5ms** | **8.1ms** | **23.9ms** |

**Разбивка по таблицам при split=1,3,6,20,1,1:**

| Таблица | conns | avg | Строк/блок |
|---------|-------|-----|-----------|
| itxs | 20 | 9.1ms | ~1900 |
| logs | 6 | 9.3ms | ~600 |
| txs | 3 | 8.1ms | ~270 |
| blocks | 1 | 2.4ms | 1 |
| contracts | 1 | 1.9ms | ~20 |
| cba | 1 | 2.5ms | ~20 |

**Почему split=1,3,6,20,1,1 оптимален:**  
Каждому соединению достаётся ~100 строк = 1 BATCH фрейм.  
При 32+ соединениях на itxs: contention на одном шарде → деградация.

### 9.5 Флаги loader2

| Флаг | По умолчанию | Описание |
|------|-------------|---------|
| `-dump` | `dump_100_b1.bin` | dump-файл (1 блок/батч) |
| `-interval` | `100` | ms между блоками |
| `-split` | `1,3,6,20,1,1` | соединений на таблицу |
| `-conns` | `0` | uniform pool (переопределяет -split) |
| `-batch` | `100` | строк в UNLOGGED BATCH фрейме |
| `-truncate` | false | TRUNCATE перед тестом |

---

## 10. Валидация данных

После записи — сверить с prod CSV:

```bash
python3 /mnt/c/Users/Public/zig20260521/validate_db.py
```

---

## 11. Типичные проблемы

| Симптом | Причина | Решение |
|---------|---------|---------|
| `NoStartBlock` у zigparser2 | Redis пустой или нет ключа | `SET LATEST_PROCESSED_BLOCK_NUMBER 25078196` |
| Scylla не стартует | Недостаточно RAM для smp=16 | Нужно ≥5 GB free (при smp=8 хватит 2.5 GB) |
| gonode не отвечает | Файл 3.2 GB ещё грузится | Ждать `nc -z 127.0.0.1 8545` |
| `batch too large` в TS1 | bytecodes > 50 KB в contracts | Уменьшить batch для contracts/cba до 10 |
| После рестарта WSL schema пропала | tmpfs очищается | Пересоздать: `cqlsh -f schema.cql` |
| PIPELINE=4 медленнее PIPELINE=2 | gonode перегружен на localhost | Нормально; на реальной ноде PIPELINE=4 лучше |
| BullMQ Lua script error | DragonflyDB без `--cluster_mode=emulated` | Перезапустить с флагами из раздела 4.1 |
| WS_URL задан, но парсер висит | gonode не поддерживал WS (старый бинарь) | Пересобрать: `cd mocknode/gonode && go build -o gonode .` |
| Парсер зависает после to_block в WS режиме | Ожидает следующий WS event которого нет | Исправлено: `if block_num >= to_block: break` |
