# Контекст проекта — ETH ERC-20 индексер

_Последнее обновление: 2026-07-03 (step 7 + chain_id guard + auto-restart API — готово к интеграции с watcher)_

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

### Запуск realtime (через готовый скрипт на сервере)
```bash
# Realtime — запустить через run_eth60_realtime.sh (уже содержит все env-переменные)
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "tmux new-session -d -s eth60 '~/run_eth60_realtime.sh >> ~/eth_index_60.log 2>&1'"
# Текущий бинарь: ~/raw_erc20_v15
# Следующий деплой: ~/raw_erc20_v16, v17, ...
```

### Запуск (через временный скрипт)
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
exec /home/alexey_smolyakov/raw_erc20_v15
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

## Dual-node запуск через тюнер (v9 binary, v3 chunk scheme)

Два экземпляра на `100.64.0.4`, каждый со своей ETH-нодой и диапазоном блоков. Управляются
`dynamic_tuner_eth.py` — он сам подбирает FETCH_WORKERS через AIMD, перезапускает при краше,
резюмирует по последнему `[watermark]` в лог-файле.

### Деплой бинаря v9 (если нужен новый)

```bash
cd /home/alex/lotos/task1/devindexer/indexer-erc20
/home/alex/lotos/zig-x86_64-linux-0.17.0-dev.263+0add2dfc4/zig build \
  -p .zig/build --cache-dir .zig/.cache -Doptimize=ReleaseFast

# Остановить работающие инстанции перед заменой бинаря
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "tmux kill-session -t eth07; tmux kill-session -t eth60; pkill -f raw_erc20 || true; sleep 2"

scp -i ~/.ssh/id_ed25519 .zig/build/bin/raw alexey_smolyakov@100.64.0.4:~/raw_erc20_v9
```

### Запуск тюнера (обе ноды)

```bash
# Деплой скриптов (если обновлены локально)
scp -i ~/.ssh/id_ed25519 \
  tools/dynamic_tuner_eth.py tools/run_tuner_07.sh tools/run_tuner_60.sh \
  alexey_smolyakov@100.64.0.4:~/
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "chmod +x ~/run_tuner_07.sh ~/run_tuner_60.sh"

# Старт (два отдельных SSH)
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "tmux new-session -d -s eth07 '~/run_tuner_07.sh >> ~/eth_tuner_07.log 2>&1'"

ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "tmux new-session -d -s eth60 '~/run_tuner_60.sh >> ~/eth_tuner_60.log 2>&1'"
```

### Мониторинг прогресса

```bash
# Статус тюнеров (blk/s, FETCH_WORKERS, load1)
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "grep -E 'steady:|probe FETCH' ~/eth_tuner_07.log | tail -10"

# Прогресс нода .7
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "grep -oP 'Accum \d+→\K\d+' ~/eth_index_07.log | tail -5"

# Прогресс нода .60
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "grep -oP 'Accum \d+→\K\d+' ~/eth_index_60.log | tail -5"

# Живы ли процессы и tmux-сессии
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 "tmux ls && pgrep -a raw_erc20"

# Остановить всё
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "tmux kill-session -t eth07; tmux kill-session -t eth60"
```

### Фоллбек схема (три уровня)

```
primary (локальная нода) → backup1 (соседняя нода) → backup2 (publicnode) → пометить пропущенным
```

Нода `.7`: primary=.7, backup1=.60, backup2=publicnode  
Нода `.60`: primary=.60, backup1=.7, backup2=publicnode

---

## Текущий статус (2026-07-03)

### Что сделано и проверено

| Фича | Статус | Коммит |
|------|--------|--------|
| ERC-20 обнаружение + 4 таблицы | ✅ готово | 84f0b2c |
| Multicall3 батчинг по всему окну | ✅ готово | 6fa2f4c |
| Auto-reconnect Scylla/Redis | ✅ готово | 45ce0dc |
| Sanitize non-UTF8 в name/symbol | ✅ готово | eb0fa4b |
| CQL Unprepared retry fix | ✅ проверено 3 дня | 0f31aa3 |
| Bytecode dedup (bloom + 3 таблицы) | ✅ готово, верифицировано | ae06c1c |
| PRIMARY_RPC_HTTPS/WSS env override | ✅ готово | bd7da48 |
| Три уровня RPC фоллбека (backup1/backup2) | ✅ готово | bd7da48 |
| Dual-node run scripts (07/60) | ✅ готово | bd7da48 |
| Dynamic tuner (AIMD FETCH_WORKERS) | ✅ запущено | bd7da48 |
| v3 chunk scheme (LANES=24, ERA=12000) | ✅ активно с 2026-06-27 | bd7da48 |
| **Bytecode store v2 pipeline integration** | ✅ завершено (binary v15) | 743102f |
| **bytecode_api v3** (`/contract`,`/same`,`/verify`, chain_id guard) | ✅ задеплоен на :8080 | fd31abf |
| **Historical backfill v2 таблиц 0→25422776** | ✅ завершён 2026-07-02 | 743102f |
| **Step 7: pending_verifications auto-trigger** | ✅ реализовано + integration tested | fd31abf |
| **bytecode_api: auto-restart + chain_id guard** | ✅ готово | fd31abf |
| **VERIFICATION_API.md** (watcher integration docs) | ✅ создан | 15d25b3 |

### v2 таблицы — состояние данных (2026-07-03, realtime ~25.45M)

| Таблица | Строки | Описание |
|---------|--------|----------|
| `bytecode_store_v2` | 117,558+ | Уникальных bytecode-сигнатур |
| `contracts_by_address_v2` | 2,300,982+ | Событий деплоя (address × block_number) |
| `addresses_by_bytecode` | 2,268,670+ | Уникальных пар (bytecode, address) — для /same |
| `collision_registry_v2` | 0 | SHA256-коллизий нет |
| `pending_verifications` | 0 | Чистое состояние (тестовая запись удалена при auto-trigger) |
| `source_store` | 0 | Верифицированных источников нет |

Разница 32,312 между `contracts_by_address_v2` и `addresses_by_bytecode` — структурная (не баг):
один адрес, задеплоенный N раз с одним bytecode → N строк в первой таблице, 1 строка во второй.

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

### Full-chain индексация — ЗАПУЩЕНА (2026-06-27)

**TRUNCATE + RESTART выполнен 2026-06-27.** Причина: обнаружено смешение двух несовместимых схем
чанков в БД (LANES=64 от основных прогонов + LANES=24 от ранних тестов → hot partition проблема).
Решение: TRUNCATE 14 таблиц eth.*, flush Redis DB1+DB2, рестарт с v3-схемой.

- **Нода .7** (eth07 tmux): `raw_erc20_v9`, блоки `0 → 12,700,000`, SCYLLA_CHUNK_BUCKETS=24, SCYLLA_CHUNK_ERA=12000
- **Нода .60** (eth60 tmux): `raw_erc20_v9`, блоки `12,700,001 → HEAD`, те же параметры
- Управляется `dynamic_tuner_eth.py` (AIMD, auto-restart)
- Схема v3: `chunk = (block % 24) + 24 * (block // 12000)` — 500 блоков/партицию, 24 партиции/эпоха

**Следить:** диск `/storage/scylla-eth` (свободно ~20 TB, нужно ~13.5 TB).

## Открытые задачи

1. **Мониторинг full-chain прогона** — проверять прогресс обеих нод, диск, скорость.
   `grep -oP 'Accum \d+→\K\d+' ~/eth_index_07.log | tail -5` на сервере.

2. **Верификация данных** — `find_missing_blocks.py` уже прогонялся (2026-06-29: найдено 6,032,
   backfill завершён). Повторить после завершения полного прогона, особенно для нодa .07 (0→12.7M).

3. **Портирование operational tooling из ветки `polygon`**:
   - `tools/monitor_pol_v3.py` → нужна ETH-версия `monitor_eth.py`
   - Merge bug fixes: silent skip-record loss, WSS reconnect giving up, give-up cycle counter

4. **Merge ветки `polygon`** — 30 operational коммитов с bug fixes (silent skip-record loss,
   WSS reconnect giving up, task #4 HTTPS backup RPC, task #7 give-up cycle counter).

5. **Верификация ERC-20 данных** — для первого 1-5M блоков, сверка с RPC ground truth.

6. ~~**pending_verifications wiring**~~ — **ГОТОВО** (binary v15, fd31abf).

7. **bytecode_api — repair backfill v2 таблиц** — если понадобится дозалить пропущенные
   контракты в `addresses_by_bytecode` (пока не нужно).

### bytecode_api v3 — endpoints (100.64.0.4:8080)

Параметр `?chain_id=` обязателен (или отсутствует — тогда предполагается `1`).
Если `chain_id != "1"` → HTTP 501. Подробная документация: `VERIFICATION_API.md`.

```
GET  /health                            → "ok"
GET  /contract?address=0x{addr}&chain_id=1  → деплой-инфо + bytecode_id + verified статус + ABI
GET  /same?address=0x{addr}&chain_id=1      → все адреса с тем же deployed bytecode
POST /same?chain_id=1  {"address"/"bytecode": "0x..."}  → то же (рекомендуется для watcher)
POST /verify?chain_id=1 {"address","abi","source"} → верифицирует; status: verified|already_verified|pending
```

**Pending-trigger:** если `/verify` вызван до индексации — пишет в `pending_verifications`.
Zig-индексер (`processOneContract` → `applyPendingVerification`) при деплое контракта
автоматически применяет pending-верификацию и удаляет запись.

**Auto-restart:** `~/run_bytecode_api.sh` содержит `while true` loop — при падении рестартует через 5с.
Запущен в tmux-сессии `bytecode_api` на 100.64.0.4. Binary: `~/bytecode_api_v3`.
Исходник: `tools/bytecode_api/main.go`.

## Infra — ETH

- **Нода .7:** `100.64.0.7:8545` — RETH v2.3.0, архивная, `trace_block`, диапазон `[0, 12_700_000]`
- **Нода .60:** `100.64.0.60:8545` — RETH v2.3.0, архивная, `trace_block`, диапазон `[12_700_001, HEAD]`
- **Publicnode backup:** `https://ethereum-rpc.publicnode.com` — третий уровень фоллбека
- **Worker count:** динамический (`dynamic_tuner_eth.py` AIMD); бенчмарк .7: оптимум W=64 (204.9 blk/s)
- **SAVE_EVERY:** `100`, **SCYLLA_CHUNK_BUCKETS:** `24`, **SCYLLA_CHUNK_ERA:** `12000`
- **Схема чанков (v3):** `chunk = (block % 24) + 24 * (block // 12000)` — 500 блоков/партицию max, 24 партиции/эпоха (spread по 24 Scylla шардам). Аналог Polygon v3.
- **Redis изоляция:** нода .7 → DB=1, нода .60 → DB=2 (разные курсоры, не конфликтуют)

## Документация

| Файл | Содержание |
|------|-----------|
| `VERIFICATION_API.md` | Watcher integration: все endpoints, chain_id, pending-flow, curl-примеры |
| `docs/FULLCHAIN_PREP.md` | Воркер-каунт, оценки времени, диск, чеклист, валидация |
| `docs/ERC20_BENCHMARK.md` | Детальные бенчмарки ERC-20 (burst/sustained, по зонам цепи) |
| `docs/SCYLLA_STORAGE_MIGRATION.md` | Как и зачем переехали на /storage, детали LVM |
| `CONTEXTDEV.md` | Ветки, сборка, деплой, форматы логов, Redis cursor, мониторинг (детально) |
| `scripts/db/models/lookups/` | CQL схемы для 4 ERC-20 таблиц |
