# Контекст проекта — ETH ERC-20 индексер

_Последнее обновление: 2026-06-26 (v2 — dual-node setup)_

## Что это

Ветка `alex_erc20` — EVM-индексер с поддержкой ERC-20 токенов (сеть Ethereum, chain_id=1). Базируется
на основном индексере (`dev` ветка), добавляет:

- Обнаружение ERC-20 токенов (по паттернам логов Transfer/Approval)
- Bloom filter для дедупликации токен-адресов в окне аккумуляции
- Multicall3-резолюция `name()`/`symbol()`/`decimals()`/`totalSupply()` батчами по всему accum-окну
- 4 новые таблицы Scylla: `erc20_tokens`, `erc20_total_supplies`, `erc20_owners`, `erc20_self_destructed`
- Auto-reconnect + health-check для Scylla/Redis соединений
- Retry CQL Unprepared (0x2500) — re-prepare + повтор, без фатального падения

## 🔧 ШПАРГАЛКА — критические операции

**Тест-сервер:** `ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4`
**ETH-нода 1:** `http://100.64.0.7:8545` (RETH, архивная, trace_block, диапазон 0→12.7M)
**ETH-нода 2:** `http://100.64.0.60:8545` (RETH, архивная, trace_block, диапазон 12.7M→HEAD)
**Публичный backup:** `https://ethereum-rpc.publicnode.com`
**Redis:** `redis://:ZCy8k4G6pcRYVFfm@127.0.0.1:6379/<DB>` (DB=1 — нода .7, DB=2 — нода .60, DB=3+ — тесты)
**Scylla:** `127.0.0.1:9042`, keyspace=`eth`, user=`cassandra`, pass=`cassandra`
**GrayLog:** ingestion `144.76.108.185:12201` (GELF/TCP)

### Проверить прогресс (realtime)
```bash
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "redis-cli -u 'redis://:ZCy8k4G6pcRYVFfm@127.0.0.1:6379/0' GET LATEST_PROCESSED_BLOCK_NUMBER"
```

### Сборка и деплой
```bash
# Сборка (локально)
cd /home/alex/lotos/task1/devindexer/indexer-erc20
/home/alex/lotos/zig-x86_64-linux-0.17.0-dev.263+0add2dfc4/zig build \
  -p .zig/build --cache-dir .zig/.cache -Doptimize=ReleaseFast
# Бинарь: .zig/build/bin/raw

# Деплой
scp -i ~/.ssh/id_ed25519 .zig/build/bin/raw alexey_smolyakov@100.64.0.4:~/raw_erc20_vN
```

Имя `raw_erc20_vN` — инкрементировать N при каждом деплое.

### Запуск (через скрипт)
```bash
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 "cat > /tmp/start_erc20.sh << 'SCRIPT'
#!/bin/bash
export MODE=production
export EVM_CHAIN_ID=1
export CM_CONNECTION_URL='redis://:ZCy8k4G6pcRYVFfm@127.0.0.1:6379/0'
export SCYLLA_DB_HOST=127.0.0.1
export SCYLLA_DB_PORT=9042
export SCYLLA_DB_KEYSPACE=eth
export SCYLLA_DB_USERNAME=cassandra
export SCYLLA_DB_PASSWORD=cassandra
exec /home/alexey_smolyakov/raw_erc20_vN
SCRIPT
chmod +x /tmp/start_erc20.sh"

ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "nohup /tmp/start_erc20.sh > ~/erc20_realtime.log 2>&1 &"
```

### Исторический прогон (с --from / --to)
```bash
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 "cat > /tmp/start_hist.sh << 'SCRIPT'
#!/bin/bash
export MODE=production
export EVM_CHAIN_ID=1
export CM_CONNECTION_URL='redis://:ZCy8k4G6pcRYVFfm@127.0.0.1:6379/<DB>'
export SCYLLA_DB_HOST=127.0.0.1
export SCYLLA_DB_PORT=9042
export SCYLLA_DB_KEYSPACE=eth
export SCYLLA_DB_USERNAME=cassandra
export SCYLLA_DB_PASSWORD=cassandra
exec /home/alexey_smolyakov/raw_erc20_vN --from=<FROM> --to=<TO>
SCRIPT
chmod +x /tmp/start_hist.sh"

ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "nohup /tmp/start_hist.sh > ~/erc20_hist_<FROM>_<TO>.log 2>&1 &"
```

## Dual-node запуск (v7 binary)

Два экземпляра на `100.64.0.4`, каждый со своей ETH-нодой и диапазоном блоков.

### Деплой бинаря v7

```bash
# Локально
cd /home/alex/lotos/task1/devindexer/indexer-erc20
/home/alex/lotos/zig-x86_64-linux-0.17.0-dev.263+0add2dfc4/zig build \
  -p .zig/build --cache-dir .zig/.cache -Doptimize=ReleaseFast

# Убедиться что v6 остановлен
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 "pkill -f raw_erc20_v6 || true; sleep 1"

# Деплой
scp -i ~/.ssh/id_ed25519 .zig/build/bin/raw alexey_smolyakov@100.64.0.4:~/raw_erc20_v7
```

### Запуск dual-node (каждый в своём tmux pane)

**Шаг 1** — скопировать скрипты запуска на сервер:
```bash
scp -i ~/.ssh/id_ed25519 \
  tools/run_eth_07.sh tools/run_eth_60.sh \
  alexey_smolyakov@100.64.0.4:~/
```

**Шаг 2** — **отредактировать `YOUR_REDIS_PASSWORD`** в обоих скриптах на `ZCy8k4G6pcRYVFfm`.

**Шаг 3** — запуск (два раздельных SSH, иначе второй может не стартовать):
```bash
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "nohup ~/run_eth_07.sh > ~/eth_07_wrap.log 2>&1 &"

ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "nohup ~/run_eth_60.sh > ~/eth_60_wrap.log 2>&1 &"
```

### Мониторинг прогресса

```bash
# Прогресс нода .7
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "grep -oP 'Accum \d+→\K\d+' ~/eth_index_07.log | tail -5"

# Прогресс нода .60
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "grep -oP 'Accum \d+→\K\d+' ~/eth_index_60.log | tail -5"

# Живы ли процессы
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 "pgrep -a raw_erc20"
```

### Фоллбек схема (три уровня)

```
primary (локальная нода) → backup1 (соседняя нода) → backup2 (publicnode) → пометить пропущенным
```

Нода `.7`: primary=.7, backup1=.60, backup2=publicnode  
Нода `.60`: primary=.60, backup1=.7, backup2=publicnode

---

## Текущий статус (2026-06-26)

### Что сделано и проверено

| Фича | Статус | Коммит |
|------|--------|--------|
| ERC-20 обнаружение + 4 таблицы | ✅ готово | 84f0b2c |
| Multicall3 батчинг по всему окну | ✅ готово | 6fa2f4c |
| Auto-reconnect Scylla/Redis | ✅ готово | 45ce0dc |
| Sanitize non-UTF8 в name/symbol | ✅ готово | eb0fa4b |
| CQL Unprepared retry fix | ✅ проверено 3 дня | 0f31aa3 |
| Bytecode dedup (bloom + 3 таблицы) | ✅ готово, верифицировано | ae06c1c |
| PRIMARY_RPC_HTTPS/WSS env override | ✅ готово | (текущий) |
| Три уровня RPC фоллбека (backup1/backup2) | ✅ готово | (текущий) |
| Dual-node run scripts (07/60) | ✅ готово | (текущий) |

### Unprepared fix — детали

Scylla под нагрузкой вытесняет cached prepared statements. Наш клиент ловил `0x2500` как fatal.
Исправление: `Prepared{id, query}` — ID в паре с исходным запросом. `batchSendRows()` при получении
`error.CqlUnprepared` делает re-PREPARE на том же соединении и повторяет.

Проверка: 3-дневный прогон (25,360,000 → 25,394,654, 34,810 блоков), 12 Unprepared-событий —
все восстановлены прозрачно, ни одного краша. Процесс дополз до head цепи и тейлил новые блоки.

### Throughput

| Зона цепи | blk/s | Примечание |
|-----------|-------|------------|
| Sparse (0-5M, ранние блоки) | ~49+ | мало tx/logs |
| Dense (24-25M) | ~49 | sustained, /storage NVMe |
| Near-head (25.3M+) | ~9.5 | 2060 itx/блок, высокая плотность |
| Burst-тест 100-1000 блоков | 40-983 | ОБМАНЧИВО: не учитывает compaction backpressure |

**Sustained-скорость (не burst)** — единственная валидная метрика для оценки времени. Burst-тесты
короткие (<1 мин) и не успевают столкнуться с compaction backpressure.

Оценка полной синхронизации (genesis → 25.36M, ~49 blk/s в dense зоне): **~6 дней**.
У head (~9.5 blk/s) — существенно медленнее, но это небольшой хвост по объёму данных.

### Диск — статус

- Scylla пишет на `/storage/scylla-eth` — выделенный LVM 20.96 TiB на 3× enterprise NVMe
- Свободно ~20 TB, оценка на всю цепь ~13.5 TB (запас ~6.5 TB — не огромный, следить с первого дня)
- Подробнее: `docs/FULLCHAIN_PREP.md` разделы 3 и 3.1

### Что не запущено

- **Полная историческая индексация** (genesis → head) — **НЕ ЗАПУЩЕНА**. Это следующий шаг.
  Перед запуском — прочитать `docs/FULLCHAIN_PREP.md` полностью (особенно чеклист п.5).

## Открытые задачи

1. **Запуск full-chain индексации** — genesis → head на `/storage/scylla-eth`.
   Ключевые риски: диск (~6.5 TB запаса), время (~6+ дней), нет мониторинга как у Polygon (нет аналога `dynamic_tuner.py`).
   Предварительно: sustained-тест в зоне 25.3M (head) чтобы уточнить скорость там.

2. **Портирование operational tooling из ветки `polygon`**:
   - `tools/dynamic_tuner.py` — авто-тюнинг FETCH_WORKERS
   - `tools/find_missing_blocks.py` — поиск пропусков
   - `tools/backfill_spans.sh` — backfill конкретных диапазонов
   - `tools/monitor_pol_v3.py` → нужна ETH-версия `monitor_eth.py`

3. **Merge ветки `polygon`** — 30 operational коммитов с bug fixes (silent skip-record loss,
   WSS reconnect giving up, task #4 HTTPS backup RPC, task #7 give-up cycle counter).

4. **Верификация ERC-20 данных** — для первого 1-5M блоков после запуска, сверка с RPC ground truth.

## Infra — ETH

- **Нода .7:** `100.64.0.7:8545` — RETH v2.3.0, архивная, `trace_block`, диапазон `[0, 12_700_000]`
- **Нода .60:** `100.64.0.60:8545` — RETH v2.3.0, архивная, `trace_block`, диапазон `[12_700_001, HEAD]`
- **Publicnode backup:** `https://ethereum-rpc.publicnode.com` — третий уровень фоллбека
- **Worker count:** `8` в run-скриптах (тестовый дефолт; для sustained prod — `64` оптимум для .7)
- **SAVE_EVERY:** `100`, **SCYLLA_CHUNK_BUCKETS:** `64`
- **Redis изоляция:** нода .7 → DB=1, нода .60 → DB=2 (разные курсоры, не конфликтуют)

## Документация

| Файл | Содержание |
|------|-----------|
| `docs/FULLCHAIN_PREP.md` | Воркер-каунт, оценки времени, диск, чеклист, валидация |
| `docs/ERC20_BENCHMARK.md` | Детальные бенчмарки ERC-20 (burst/sustained, по зонам цепи) |
| `docs/SCYLLA_STORAGE_MIGRATION.md` | Как и зачем переехали на /storage, детали LVM |
| `CONTEXTDEV.md` | Ветки, сборка, деплой, форматы логов, Redis cursor, мониторинг (детально) |
| `scripts/db/models/lookups/` | CQL схемы для 4 ERC-20 таблиц |
