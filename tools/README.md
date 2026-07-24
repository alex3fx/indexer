# Tools

Supporting scripts for running and operating the dual-node historical indexer. Full env-var
reference and step-by-step launch instructions: **`docs/HOWTOSTART.md`**.

All scripts read credentials from the environment — `SCYLLA_DB_PASSWORD` is required (no
hardcoded default anywhere in this directory); `GRAFANA_USER`/`GRAFANA_PASS` are optional
(only needed for the tuner's load-aware signal).

## `dynamic_tuner_eth.py` + `run_tuner_07.sh` / `run_tuner_60.sh` — ETH tuner (active)

Bootstrap-probes FETCH_WORKERS (W=16/32/48/64 for node .07; W=32/48/64/96 for node .60), then
runs a continuous AIMD control loop driven by the RPC node's `node_load1` from Grafana/Prometheus.
Restarts the indexer on crash, resumes from `[watermark] N` in the log, sends alerts to GrayLog.

`run_tuner_07.sh` / `run_tuner_60.sh` are credential wrappers — credentials live in the file, not
on the command line (not visible in `ps aux`). Deployed to `~/` on `100.64.0.4`.

```bash
# Start (tmux sessions eth07 / eth60 on 100.64.0.4)
tmux new-session -d -s eth07 '~/run_tuner_07.sh >> ~/eth_tuner_07.log 2>&1'
tmux new-session -d -s eth60 '~/run_tuner_60.sh >> ~/eth_tuner_60.log 2>&1'

# Status
tail -f ~/eth_tuner_07.log
tail -f ~/eth_tuner_60.log
grep 'steady:' ~/eth_tuner_07.log | tail -5   # blk/s and FETCH_WORKERS

# Stop
tmux kill-session -t eth07
tmux kill-session -t eth60
```

Hardware: node .07 = 48 cores (157.90.65.123), node .60 = 96 cores (100.64.0.60).
Load target: 0.85–0.95 of core count. Cooldown between adjustments: 90s.
**Status:** running (2026-06-27), requires binary v10+, supersedes `run_eth_07.sh`/`run_eth_60.sh`.
**v3 chunk scheme active:** `SCYLLA_CHUNK_BUCKETS=24`, `SCYLLA_CHUNK_ERA=12000`
(`chunk = (block % 24) + 24 * (block // 12000)` — 500 blk/partition, 24 partitions/era).
Switched from LANES=64 flat (hot partition risk) via TRUNCATE+RESTART on 2026-06-27.
**Realtime mode (2026-06-29):** `TO_BLOCK=0` in `run_tuner_60.sh` → binary launched without `--to`,
catches up historically, enters WSS realtime loop. Per-block `log.info` → GrayLog every ~12s.

---

## `run_eth_07.sh` / `run_eth_60.sh` — static wrapper scripts (superseded)

Simple auto-restart loops with fixed FETCH_WORKERS=64. Superseded by `dynamic_tuner_eth.py`
which probes and adjusts FETCH_WORKERS automatically. Kept for reference / manual override.

**Status:** superseded (2026-06-26), requires binary v8+. Do NOT use — chunk scheme was LANES=64 flat (incompatible with current v3 scheme)

---

## `dynamic_tuner.py` — Polygon indexer tuner (not for ETH)

Polygon-specific (chain_id=137, nodes .62/.63, keyspace=pol). See the Polygon indexer repo
(`/home/alex/lotos/task1/devindexer/indexer`) for usage. Use `dynamic_tuner_eth.py` for ETH.

```bash
export SCYLLA_DB_PASSWORD='...'
export GRAFANA_USER='...' GRAFANA_PASS='...'   # optional
python3 dynamic_tuner.py <node_id> <rpc_url> <from_block_if_no_log_yet> <to_block>
```

## `monitor_pol_v3.py` — heartbeat/alert monitor

Tails the node log files, sends GELF heartbeats (block height, blk/s, ETA, alive status) to
GrayLog periodically, and fires immediate alerts on `[ALERT]`/`[WARNING]` log lines. Seeds its
read position at the current end-of-file on startup, so restarting the monitor never re-sends
old alerts as new.

```bash
python3 monitor_pol_v3.py
```

## `find_missing_blocks.py` — integrity scanner

Scans every `(lane, era)` chunk partition in a block range and prints the exact missing block
numbers (one per line) — an arithmetic-sequence check per lane, not `ALLOW FILTERING`, so it
doesn't under-sample like naive chunk-number enumeration does.

```bash
export SCYLLA_DB_USERNAME=cassandra SCYLLA_DB_PASSWORD='...' SCYLLA_DB_KEYSPACE=eth
python3.12 find_missing_blocks.py FROM_BLOCK TO_BLOCK [LANES=24] [ERA=12000] [TABLE=blocks] [CONCURRENCY=128]
```

Run this after any historical sync range is claimed complete, and after any manual backfill,
before trusting the data as gap-free. Output: one missing block number per line on stdout;
summary ("Total missing: N") to stderr. Requires `python3.12` (cassandra-driver installed there).

**2026-06-29 run**: found 6,032 missing blocks across 235 regions in 0→25.4M.
Results saved to `~/missing_blocks_eth.txt` on 100.64.0.4.

## `make_spans_eth.py` — groups missing blocks into re-index spans

Takes `find_missing_blocks.py` output (one block number per line) and groups consecutive missing
blocks into contiguous `FROM TO` spans for backfill. Splits output at block 12,700,000 (the
.07/.60 RPC split point) into two separate files.

```bash
python3 make_spans_eth.py <missing_blocks.txt> [gap_tolerance=500] [padding=10]
# produces: missing_blocks_eth_spans_07.txt and missing_blocks_eth_spans_60.txt
```

**2026-06-29**: 6,032 missing blocks → 232 spans (134 for .07, 98 for .60).

## `backfill_spans_eth.sh` — targeted re-sync for known gaps (ETH)

Re-runs the ETH indexer binary across each `FROM TO` span from a file. Idempotent — primary key
is `chunk + block_number`, so re-covering already-good blocks at span edges is safe.
Uses `SCYLLA_CHUNK_BUCKETS=24 SCYLLA_CHUNK_ERA=12000 EVM_CHAIN_ID=1 SCYLLA_DB_KEYSPACE=eth`.

```bash
export SCYLLA_DB_USERNAME=cassandra SCYLLA_DB_PASSWORD='...' REDIS_PASSWORD='...'
./backfill_spans_eth.sh <node: 07|60> <rpc_url> <spans_file> <log_file> [fetch_workers=10] [span_timeout=3600]
```

Deployed to `~/` on 100.64.0.4 along with `run_backfill_07.sh` / `run_backfill_60.sh` wrappers.
Running in tmux sessions `backfill07` / `backfill60` as of 2026-06-29.

**Status:** active (2026-06-29) — backfilling 6,032 missing blocks from overload period.
Logs: `~/eth_backfill_07.log`, `~/eth_backfill_60.log`.

## `run_eth60_realtime.sh` — direct realtime launch (no tuner)

Direct launch of eth60 without tuner — fixed `FETCH_WORKERS=64`, no AIMD, no probing.
Binary started without `--to`: discovers HEAD from WSS, catches up historically, enters
WSS realtime loop (`log.info` per block → GrayLog every ~12s).

```bash
# On 100.64.0.4, in tmux session eth60:
tmux new-session -d -s eth60 '~/run_eth60_realtime.sh'
# Resume: watermark auto-detected from eth_index_60.log
```

**Status:** active (2026-07-04), binary v18 on 100.64.0.4. Auto-restart loop added 2026-07-04
(process had no restart on CqlError crash). Launched when tuner is not needed.
For AIMD-managed realtime, prefer `run_tuner_60.sh` with `TO_BLOCK=0`.

---

## `run_test_single_block.sh` — one-shot integration test for pending_verifications trigger

Runs binary v15 on block 12622799 (node .07), Redis DB=9 (isolated cursor). Used 2026-07-02 to
verify step 7 (pending_verifications auto-trigger): confirmed `applied pending verification` fires,
`bytecode_store_v2.verified` set to true, pending row deleted.

**Status:** test-only (2026-07-02), do not use in production.

---

## `backfill_deployed_bytecode/` — backfill bytecode_hash для исторических контрактов (2026-07-04)

Заполняет `bytecode_hash`/`bytecode_seq` в `contracts_by_address_v2` для строк где эти поля NULL
(результат `merge_old_to_v2` UPDATE-only записей). Для каждой null-строки: вызывает
`eth_getCode(address, block_number)` через JSON-RPC, вычисляет SHA256, пишет в `bytecode_store_v2`,
обновляет `contracts_by_address_v2`, добавляет в `addresses_by_bytecode`.

- **Идемпотентен:** `INSERT bytecode_store_v2 IF NOT EXISTS`, `UPDATE contracts_by_address_v2` — безопасен при перезапуске.
- **Пропускает:** строки где bytecode_hash уже заполнен, контракты с пустым bytecode (`0x`).
- **Retry:** 4 попытки для RPC, 5 попыток для Scylla с exponential backoff.

```bash
~/backfill_deployed_bytecode_v1 \
  --rpc=http://100.64.0.60:8545 \
  --host=127.0.0.1 --pass=cassandra \
  --rpc-workers=40 --rpc-batch=100 \
  --db-workers=256 --page-size=5000 --log-every=500000
```

**Build:**
```bash
cd tools/backfill_deployed_bytecode
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o backfill_deployed_bytecode_v1 .
scp backfill_deployed_bytecode_v1 alexey_smolyakov@100.64.0.4:~/backfill_deployed_bytecode_v1
```

**Status:** запущен 2026-07-04 на 100.64.0.4, tmux-сессия `backfill_deployed`.
Скорость: ~11,000 строк/с. Ожидаемое время: ~85-90 мин для ~57M строк.
Лог: `~/backfill_deployed.log`. **Не закрыт bytecode deployer/tx_hash** (те поля null в merge_old_to_v2 строках — отдельная задача).

---

## `count_ghost_rows/` — count null tx_hash rows in contracts_by_address_v2 (2026-07-07)

Считает ghost-строки (reverted CREATE2 из snap-таблицы) — строки с `tx_hash = null`.
CQL не поддерживает фильтрацию по NULL, поэтому полный параллельный скан по 256 токен-сегментам.

**Зачем:** Наша БД содержит ~1,039,573 лишних строк vs Etherscan. Инструмент считает точное число.
После — нужен отдельный инструмент для удаления ghost-строк.

```bash
cd tools/count_ghost_rows
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 \
  /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o count_ghost_rows .
scp count_ghost_rows alexey_smolyakov@100.64.0.4:~/count_ghost_rows

# Run (фоново, ~1 час):
nohup ~/count_ghost_rows --host 127.0.0.1 --pass cassandra --segments 256 --page-size 2000 \
  >> ~/count_ghost_rows.log 2>&1 &
tail -f ~/count_ghost_rows.log
```

**Status:** завершён 2026-07-07 10:48 на 100.64.0.4, 27m12s, 0 ошибок.
**Результат:** ghost=102,640 / success=102,759,056 / total=102,861,696
Лог: `~/count_ghost_rows.log`.

---

## `check_tx_status/` — проверка статуса tx для контрактов (2026-07-07)

Выбирает N контрактов из каждого token-сегмента, для каждого смотрит статус транзакции
в таблице `transactions`. Проверяет гипотезу о контрактах из failed-транзакций.

```bash
cd tools/check_tx_status
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 \
  /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o check_tx_status .
scp check_tx_status alexey_smolyakov@100.64.0.4:~/check_tx_status

~/check_tx_status --host 127.0.0.1 --pass cassandra --segments 64 --per-seg 300
```

**Status:** запущен 2026-07-07, 64×300=9900 контрактов.
**Результат:** 0.26% контрактов из failed-tx (status=0), 26 примеров с адресами.

---

## `count_distinct_addrs/` — count unique contract addresses (2026-07-07)

Считает DISTINCT адреса (уникальные партиции) в `contracts_by_address_v2` через
`SELECT DISTINCT address`. Отличается от `count_ghost_rows` который считает строки —
нужен для понимания: сколько у нас уникальных адресов vs Etherscan.

```bash
cd tools/count_distinct_addrs
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 \
  /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o count_distinct_addrs .
scp count_distinct_addrs alexey_smolyakov@100.64.0.4:~/count_distinct_addrs

nohup ~/count_distinct_addrs --host 127.0.0.1 --pass cassandra --segments 256 --page-size 2000 \
  >> ~/count_distinct_addrs.log 2>&1 &
```

**Status:** завершён 2026-07-07, 26m46s, 0 ошибок.
**Результат:** 101,959,039 уникальных адресов (Etherscan: 101,821,894, разница: +137,145).
Лог: `~/count_distinct_addrs.log`.

---

## `delete_phantom_ghost/` — delete phantom and ghost rows from contracts_by_address_v2 (2026-07-13)

Удаляет:
- **Phantom rows** — все строки для адресов из `--phantom-file` (вывод `find_phantom_addrs`)
- **Ghost rows** — строки с `tx_hash=null` (full token-range scan, 256 сегментов)

По умолчанию `--dry-run=true` — выводит что будет удалено без реального удаления.
Все удаления — с retry+backoff (5 попыток). На ошибку после 5 попыток: логирует (не замалчивает).

```bash
# Build:
cd tools/delete_phantom_ghost
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 \
  /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o delete_phantom_ghost .
scp delete_phantom_ghost alexey_smolyakov@100.64.0.4:~/delete_phantom_ghost

# Dry run (безопасно, логирует что будет удалено):
~/delete_phantom_ghost --pass=cassandra \
  --phantom-file=~/phantom_addresses.txt \
  --segments=256 --dry-run=true

# Live run (реальное удаление):
nohup ~/delete_phantom_ghost --pass=cassandra \
  --phantom-file=~/phantom_addresses.txt \
  --segments=256 --dry-run=false \
  >> ~/delete_phantom_ghost.log 2>&1 &
tail -f ~/delete_phantom_ghost.log
```

**Status:** DONE 2026-07-13. Удалено: phantom=167,164 строк, ghost=102,640 строк, errors=0.
Бинарь `~/delete_phantom_ghost` на 100.64.0.4.

---

## `find_phantom_addrs/` — classify unique addresses as real/phantom/ghost-only (2026-07-07)

Полный скан `contracts_by_address_v2` по 256 token-сегментам. Для каждого уникального адреса
смотрит статус транзакции из нашей таблицы `eth.transactions` (без запросов к ноде, без Etherscan).

Классификация:
- **real** — хотя бы одна транзакция-деплой имеет `status=1` (реальный контракт на мейннете)
- **phantom** — все строки с `tx_hash` имеют `status=0` (deployed из failed-tx, на мейннете нет)
- **ghost-only** — все строки `tx_hash=null` (reverted CREATE2 snap-записи без tx)

Phantom и ghost-only адреса пишутся в выходной файл. Инструмент для независимой проверки —
даёт честный count из нашей БД, не из сравнения с Etherscan.

```bash
cd tools/find_phantom_addrs
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 \
  /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o find_phantom_addrs .
scp find_phantom_addrs alexey_smolyakov@100.64.0.4:~/find_phantom_addrs

# Quick sanity test (4 segments):
~/find_phantom_addrs --host 127.0.0.1 --pass 'pass' --segments 4 --page-size 200 --out /tmp/phantom_test.txt

# Full run (~60-90 min):
nohup ~/find_phantom_addrs --host 127.0.0.1 --pass 'pass' \
  --segments 256 --page-size 1000 --out ~/phantom_addresses.txt \
  > ~/find_phantom.log 2>&1 &
tail -f ~/find_phantom.log
```

Output file format: `address\tphantom\tblock_number\ttx_hash` или `address\tghost-only`
Progress logs every 30s. Errors in tx lookup → treated as real (never false-phantom).

**v1 Status:** завершён 2026-07-07 → 2026-07-08 (30h25m). Результат: 112,560 phantom адресов, 167,164 строк.
Лог: `~/find_phantom.log`. Вывод: `~/phantom_addresses.txt`.
**БАГ в v1:** при первой реальной строке адреса (status=1) — `break`, фантомные строки того же адреса (status=0) не записывались. CREATE2-редеплои (failed attempt → successful deployment, один адрес, разные block_number) — фантомные строки выжили.

**ФИКС v2 (2026-07-18):** `break` → `continue` в `classifyAddress`. Phantom-строки всегда пишутся, даже если адрес имеет реальные строки. Бинарь: `find_phantom_addrs_v2`.

```bash
# Build v2:
cd tools/find_phantom_addrs
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 \
  /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o find_phantom_addrs_v2 .
scp find_phantom_addrs_v2 alexey_smolyakov@100.64.0.4:~/find_phantom_addrs_v2

# Full run:
nohup ~/find_phantom_addrs_v2 --pass=cassandra \
  --segments=256 --page-size=1000 --out=/home/alexey_smolyakov/phantom_v2.txt \
  > /home/alexey_smolyakov/find_phantom_v2.log 2>&1 &
tail -f /home/alexey_smolyakov/find_phantom_v2.log
```

**v2 Status:** запущен 2026-07-18 15:14, PID=242853. ~102M строк, ожидаемый результат ~225k phantom строк (missed-redeployment phantoms).

---

## `check_gap_phantoms/` — удаление phantom строк из gap-периода (2026-07-14)

Проверяет и удаляет phantom строки, попавшие в `contracts_by_address_v2` за период когда
v19 (без фикса трансформера) работал после чистки базы (2026-07-13) до деплоя v20 (2026-07-14).
Блоки 25,459,261–25,525,803.

**Стратегия:**
- Phase 1: собирает хэши failed-tx из chunk-партиционированной таблицы `transactions` (168 чанков, быстро)
- Phase 2: полный token-range scan `contracts_by_address_v2` (256 сегментов, ~3 мин), фильтр по block_number и tx_hash

```bash
# Build:
cd tools/check_gap_phantoms
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 \
  /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o check_gap_phantoms .
scp check_gap_phantoms alexey_smolyakov@100.64.0.4:~/check_gap_phantoms

# Dry run:
~/check_gap_phantoms --pass=cassandra --dry-run=true

# Live run:
~/check_gap_phantoms --pass=cassandra --dry-run=false
```

**Status:** DONE 2026-07-14. 614 phantom строк найдено и удалено, errors=0.
Бинарь `~/check_gap_phantoms` на 100.64.0.4.
Логи: `~/check_gap_phantoms.log` (dry-run), `~/check_gap_phantoms_live.log` (live).

---

## `bytecode_api/` — HTTP service: bytecode lookup, clone-search, verification (active, v7)

Go HTTP service для интеграции с watcher. Читает из v2-таблиц (`contracts_by_address_v2`,
`addresses_by_bytecode` — 256 бакетов параллельно), пишет при верификации.
Фильтрует CREATE2-редеплои: `/same` spot-проверяет текущий байткод каждого адреса.

**Auth:** все endpoints (кроме `/health`) требуют header `Api-Access-Key: <key>`. Без ключа — 401.

**Endpoints (v7):**

```
GET   /health                                     → "ok" (без авторизации)
GET   /contract?address=0x{addr}                  → {address, block_number, block_timestamp(ms),
                                                     tx_hash, contract_creator, contract_factory,
                                                     verified, verified_at(ms), programming_language,
                                                     abi, deployed_bytecode, creation_bytecode, source}
QUERY /same  {"address"|"deployed_bytecode"|"creation_bytecode": "...", "limit":N, "offset":N}
             → {hash, seq, total, offset, limit, addresses}
POST  /verify {"address","abi","source","programming_language","verified_at"(ms, optional)}
             → {status, verified_at(ms), ...}
```

`/same`: метод QUERY (RFC draft-ietf-httpbis-safe-method-with-body) — безопасный+идемпотентный с телом.
GET и POST → 405. Тело до 4 MB.

`/verify`: `abi` и `source` **обязательны вместе** (одно без другого → 400). Оба plain-text.
`verified_at` (ms) — опционально; если не передан, используется `time.Now()`. Передавать при историческом импорте.
`block_timestamp` — int64 в миллисекундах (не строки).
Если адрес не найден — пишет в `pending_verifications`, возвращает `status:pending`.

Полная документация: **`VERIFICATION_API.md`** в корне репо.

**Source:** `tools/bytecode_api/main.go` + `go.mod`/`go.sum`

**Build** (локально через Go 1.26.4):

```bash
cd tools/bytecode_api
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 \
  /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o bytecode_api_v7 .
scp bytecode_api_v7 alexey_smolyakov@100.64.0.4:~/bytecode_api_v7
```

**Deployed:** `100.64.0.4:8080`, binary `~/bytecode_api_v7`, run script `~/run_bytecode_api.sh`
(user: cassandra — нужен для записи при `/verify`)

```bash
# Start (auto-restart loop, уже запущен через nohup на 100.64.0.4):
nohup bash ~/run_bytecode_api.sh >> ~/bytecode_api.log 2>&1 &

# Test:
KEY="Api-Access-Key: 354bf5a9-a29a-4879-9f3d-d3c09c6a610a"
curl http://100.64.0.4:8080/health
curl -H "$KEY" -H "Content-Type: application/json" \
  -X QUERY http://100.64.0.4:8080/same \
  -d '{"address":"0x219e497a09202a3534f653e63faaeab6689c1d22","limit":10}'
```

**Status:** running (2026-07-06), binary `bytecode_api_v7`. Port 8080 bound to `0.0.0.0`.
Scylla auth: user `cassandra` (read+write для `/verify`), password in `run_bytecode_api.sh`.
Auto-restart: run script содержит `while true` loop — при падении рестартует через 5с.

**Changelog:**
- **v7 (2026-07-06):** `/same` → метод QUERY вместо GET+POST, убраны GET-варианты; убрано поле `count` из ответа; `/verify` принимает опциональный `verified_at` (ms) для исторического импорта.
- **v6 (2026-07-04):** полный ETHSCAN-совместимый `/contract` (block_timestamp ms, contract_creator, contract_factory, programming_language, abi array, deployed_bytecode hex, creation_bytecode hex, source plain text); `/verify` принимает programming_language; `/same` pagination (limit/offset, total/count); creation_bytecode wired к addresses_by_creation_bytecode; verified_at → int64 ms везде.
- **v5 (2026-07-04):** авторизация `Api-Access-Key`, `/verify` требует abi+source вместе, `bytecode` → `deployed_bytecode` в `/same`, `/contract` возвращает оба bytecode hash.
- **v4 (2026-07-03):** удалён `/bytecode` endpoint (читал старую таблицу `contracts_by_addresses`).
- **v3 (2026-07-02):** pending_verifications, source_store chunked, chain_id guard.

---

## `backfill_deployer/` — backfill deployer + tx_hash для исторических контрактов (2026-07-04)

Заполняет `deployer` и `tx_hash` в `contracts_by_address_v2` для строк где эти поля NULL.

**Подход — два прохода, не перечитывать всю историю:**
- **Phase 1:** Full scan contracts_by_address_v2 → собирает SET уникальных `block_number` где deployer IS NULL (только int64, ~80MB).
- **Phase 2:** Для каждого уникального блока: `trace_block(blockNum)` → фильтруем `type=create` → UPDATE contracts_by_address_v2 SET deployer, tx_hash.

**Ключевая оптимизация:** trace_block вызывается ТОЛЬКО для блоков из null-deployer множества, не для всех 25M блоков.

```bash
# Estimate (phase 1 only, no writes):
~/backfill_deployer_v1 --rpc=http://100.64.0.60:8545 --host=127.0.0.1 --pass=cassandra \
  --phase1-only

# Full run:
~/backfill_deployer_v1 --rpc=http://100.64.0.60:8545 --host=127.0.0.1 --pass=cassandra \
  --trace-workers=100 --db-workers=256 --page-size=5000 --log-every=500000
```

**Build:**
```bash
cd tools/backfill_deployer
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o backfill_deployer_v1 .
scp backfill_deployer_v1 alexey_smolyakov@100.64.0.4:~/backfill_deployer_v1
```

**Status:** Phase 1 запущена 2026-07-04 (phase1-only). Результат: 10,976,928 уникальных блоков, 102.6M строк в таблице.
Заменён `backfill_deployer_v2` (trace_filter approach, 100x быстрее).

**Не закрывает:** `creation_hash` для старых строк (требует аналогичного подхода через trace_block).

---

## `backfill_deployer_v2/` — backfill deployer + tx_hash через trace_filter (2026-07-04, АКТИВЕН)

Заполняет `deployer` и `tx_hash` в `contracts_by_address_v2` для строк где эти поля NULL.

**Стратегия:** перебирает блоки 0→25421495 диапазонами по 100 блоков, вызывает `trace_filter(fromBlock, toBlock)`, фильтрует `type=create`, делает UPDATE для каждого найденного deployment.

**Vs backfill_deployer_v1:** 254K вызовов trace_filter вместо 11M trace_block → **100x быстрее** (~8 мин вместо 3-6 часов).
trace_filter по диапазонам работает на этой ноде (Erigon). Фильтрация action type — на стороне клиента.
Skips reverted creates (`error` field non-empty → контракт не задеплоен).

```bash
~/backfill_deployer_v2 \
  --rpc=http://100.64.0.60:8545 \
  --host=127.0.0.1 --pass=cassandra \
  --from=0 --to=25421495 \
  --range=100 --workers=150 --db-workers=256
```

**Build:**
```bash
cd tools/backfill_deployer_v2
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o backfill_deployer_v2 .
scp backfill_deployer_v2 alexey_smolyakov@100.64.0.4:~/backfill_deployer_v2
```

**Status:** запущен 2026-07-04, tmux-сессия `backfill_deployer`. Log: `~/backfill_deployer_v2.log`.
ETA: ~8 мин. Идемпотентен — повторный запуск безопасен.

---

## `backfill_creation_bc/` — заполнение `addresses_by_creation_bytecode` (2026-07-04)

Единоразовый backfill нового обратного индекса creation bytecode для всех уже
проиндексированных контрактов. Читает `contracts_by_address_v2` (full token-range scan),
пишет в `addresses_by_creation_bytecode`. Строки с нулевым хешем (до v18 / precompiles) — пропускаются.
Идемпотентен: INSERT по PK = upsert.

```bash
~/backfill_creation_bc_v2 --host=127.0.0.1 --pass=cassandra --workers=128 --log-every=50000
```

**Build:**
```bash
cd tools/backfill_creation_bc
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o backfill_creation_bc_v2 .
scp backfill_creation_bc_v2 alexey_smolyakov@100.64.0.4:~/backfill_creation_bc_v2
```

**Status:** выполнен 2026-07-04. v1 завис на drain loop (баг: `scanned` включает скипнутые строки).
Исправлено в v2 — sync.WaitGroup вместо счётчика. Данные записаны полностью (v1 успел дозаписать
через воркеры). ~430k строк записано из ~25M строк contracts_by_address_v2 (~2.3% имели
creation_hash).

---

## `merge_old_to_v2/` — одноразовый backfill старых таблиц в v2 (2026-07-03)

Заполняет новые поля в v2-таблицах из старых таблиц:
- **Stream A:** `contracts` → `contracts_by_address_v2` (поля: `contract_factory`, `block_timestamp_s`, `block_timestamp_ms`, `creation_method`, `transaction_index`, `trace_index`; только блоки < 25422404)
- **Stream B:** `bytecode_store` → `bytecode_store_v2` (поле: `first_seen_block`; seq=0)

Причина: v16 индексер заполняет эти поля для новых блоков, но блоки 0→25422404 проиндексированы v13-v15 без этих полей.

```bash
# На сервере (127.0.0.1):
./merge_old_to_v2 --host 127.0.0.1 --user cassandra --pass cassandra --stream both
# Или отдельно:
./merge_old_to_v2 --host 127.0.0.1 --user cassandra --pass cassandra --stream a
./merge_old_to_v2 --host 127.0.0.1 --user cassandra --pass cassandra --stream b
```

**Build:**
```bash
cd tools/merge_old_to_v2
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o merge_old_to_v2_linux .
scp merge_old_to_v2_linux alexey_smolyakov@100.64.0.4:~/merge_old_to_v2
```

**Status:** запущен 2026-07-03 на 100.64.0.4, tmux-сессия `backfill`.
~50M строк (Stream A) + ~2.5M строк (Stream B), 32 воркера, 8 retry с backoff.
Результат: 0 ошибок (Stream A), Stream B в ожидании после A.

---

## `count_null_deployer/` — count null-deployer rows in contracts_by_address_v2 (2026-07-04)

Full paginated scan of `contracts_by_address_v2`, counts rows where `deployer` is null/empty.
Uses paginated iterator (no ALLOW FILTERING), rate ~42K rows/s, ~40 min for 102M rows.

```bash
~/count_null_deployer --host=127.0.0.1 --pass=cassandra [--workers=1] [--page-size=5000]
```

**Build:**
```bash
cd tools/count_null_deployer
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o count_null_deployer_linux .
scp count_null_deployer_linux alexey_smolyakov@100.64.0.4:~/count_null_deployer
```

**Status:** utility (2026-07-04), used for before/after verification of deployer backfill runs.
Results: pre-snap 74.9M nulls (72.93%) → post-snap 365,187 (0.36%) → target ~0%.

---

## `backfill_deployer_from_snap/` — backfill deployer из восстановленного снэпшота (2026-07-04)

Читает `creator`+`tx_hash`+`block_number` из восстановленной таблицы `contracts_by_addresses`
(старая schema, PK=address) и делает UPDATE в `contracts_by_address_v2`.

Снэпшот `contracts_by_addresses` восстановлен через `nodetool refresh` из архива бэкапа
(107 GB, 1181 файлов), занял ~5 мин. После бэкфила: 101,643,606 обновлено, 0 ошибок за 51m39s.

```bash
~/backfill_deployer_from_snap --host=127.0.0.1 --pass=cassandra \
  --workers=64 --page-size=5000
```

**Build:**
```bash
cd tools/backfill_deployer_from_snap
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o backfill_deployer_from_snap_linux .
scp backfill_deployer_from_snap_linux alexey_smolyakov@100.64.0.4:~/backfill_deployer_from_snap
```

**Status:** выполнен 2026-07-04. Снизил nulls с 74.9M до 365K. Восстановленную таблицу
`contracts_by_addresses` можно дропнуть после завершения deployer_main + финальной верификации.

---

## `backfill_deployer_targeted/` — targeted deployer backfill via trace_filter (2026-07-05)

Двухфазный инструмент — **быстрее deployer_main** (~2-4x): вместо перебора всех 226K чанков
вызывает trace_filter только для тех, где реально есть null deployer строки.

**Phase 1 (~40 min):** полный paginated scan contracts_by_address_v2 → собирает null-строки
в set `(address, block_number)` + уникальные 100-блочные чанки.

**Phase 2:** trace_filter только для найденных чанков → сопоставляет по (address, block_number)
→ UPDATE deployer + tx_hash. Идемпотентен, безопасен параллельно с deployer_main.

Точит на eth07 (100.64.0.7), чтобы не конкурировать с deployer_main на eth60.

```bash
~/backfill_deployer_targeted \
  --rpc=http://100.64.0.7:8545 \
  --host=127.0.0.1 --pass=cassandra \
  --rpc-workers=30 --db-workers=64
```

**Build:**
```bash
cd tools/backfill_deployer_targeted
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o backfill_deployer_targeted_linux .
scp backfill_deployer_targeted_linux alexey_smolyakov@100.64.0.4:~/backfill_deployer_targeted
```

**Status:** ~~запущен 2026-07-05 параллельно с deployer_main~~ **KILLED 2026-07-06** — писал
`trace.action.from` как deployer (не EOA, а адрес фабрики для factory-деплоев).
Заменён на `backfill_restore_from_snap` + `backfill_deployer_v4` с правильной семантикой.

---

---

## `backfill_restore_from_snap/` — restore deployer + contract_factory from snap table (2026-07-06)

Восстанавливает `deployer` (EOA) + `contract_factory` + `tx_hash` в `contracts_by_address_v2`
из архивной таблицы `eth.contracts_by_addresses` (PK=address, старая snapshot-схема).

**Семантика** (из `historical.ts`):
- `deployer` = `creator` = `tx.from_address` (всегда EOA, никогда не адрес фабрики)
- `contract_factory` = `trace.action.from` только если отличается от EOA; NULL для прямых деплоев

**Зачем:** `backfill_deployer_v2` (deployer_main) и `backfill_deployer_targeted` писали
неправильный deployer (`trace.action.from` вместо EOA). Этот инструмент корректирует оба поля
из авторитетного источника — snap-таблицы.

**Два UPDATE-запроса** (in-place, last-write-wins):
```sql
-- когда factory != null:
UPDATE eth.contracts_by_address_v2 SET deployer=?, tx_hash=?, contract_factory=? WHERE address=? AND block_number=?
-- когда factory null:
UPDATE eth.contracts_by_address_v2 SET deployer=?, tx_hash=? WHERE address=? AND block_number=?
```

**Ограничение:** снэп-таблица PK=(address) — одна строка на адрес. Адреса задеплоенные N>1 раз
(CREATE2+selfdestruct редеплои) покрыты только для одного block_number. Остальные null-строки
обрабатывает `backfill_deployer_v4`.

```bash
~/backfill_restore_from_snap --host=127.0.0.1 --pass=cassandra --workers=128 --page-size=5000 >> ~/backfill_restore_from_snap.log 2>&1
```

**Build:**
```bash
cd tools/backfill_restore_from_snap
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o backfill_restore_from_snap .
scp backfill_restore_from_snap alexey_smolyakov@100.64.0.4:~/backfill_restore_from_snap
```

**Status:** запущен 2026-07-06 на 100.64.0.4. Скорость ~33k rows/s.
Лог: `~/backfill_restore_from_snap.log`. ETA ~51 мин для 101.6M строк.
~93% строк имеют contract_factory (withFactory counter).

---

## `backfill_deployer_v4/` — fix remaining null-deployer rows after snap restore (2026-07-06)

Трёхфазный инструмент для null-deployer строк, которые НЕТ в snap-таблице
(адреса задеплоенные N>1 раз — CREATE2+selfdestruct редеплои с другим block_number).

**Phase 1:** Paginated scan `contracts_by_address_v2` → собирает `(address, block_number, tx_hash)`
где deployer IS NULL. Группирует в 100-блочные окна для trace_filter.

**Phase 2a (tx lookup):** Для строк где tx_hash известен — читает `eth.transactions` напрямую
(`chunk = (block % 24) + 24 * (block // 12000)`) → берёт `from_address` как EOA.

**Phase 2b (trace_filter):** Для строк без tx_hash — вызывает `trace_filter` по 100-блочным
окнам, ищет CREATE-трейсы по `(address, block_number)`.

**Phase 3:** `deployer` = EOA; если `action.from != EOA` → `contract_factory = action.from`;
пишет UPDATE в `contracts_by_address_v2`.

```bash
~/backfill_deployer_v4 \
  --host=127.0.0.1 --pass=cassandra \
  --rpc=http://100.64.0.60:8545 \
  --rpc-workers=30 --tx-workers=64 --db-workers=64 >> ~/backfill_deployer_v4.log 2>&1
```

**Build:**
```bash
cd tools/backfill_deployer_v4
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o backfill_deployer_v4 .
scp backfill_deployer_v4 alexey_smolyakov@100.64.0.4:~/backfill_deployer_v4
```

**Status:** готов (2026-07-06), запустить после завершения `backfill_restore_from_snap`.
Лог будет: `~/backfill_deployer_v4.log`.
Запуск запланирован автоматически через watcher на `100.64.0.4` после завершения restore.

---

## `backfill_creation_from_snap/` — restore creation_bytecode from snap table (2026-07-06)

Восстанавливает `creation_bytecode` из архивной таблицы `eth.contracts_by_addresses` (snap)
в `bytecode_store_v2` (kind=1) и обновляет `contracts_by_address_v2.creation_hash`.

**Причина:** поле `creation_hash` / `creation_bytecode` в `/contract` API было null для всех
контрактов до блока ~25.4M — старые инструменты (`backfill_creation_bc_v2`) читали из
`bytecode_store` v1, а не из snap-таблицы. Snap-таблица содержит `creation_bytecode text`
(0x-prefixed hex) примерно для 90% строк.

**Логика на строку:**
1. hex-decode `creation_bytecode` (strip `0x`)
2. sha256 → `bytecode_store_v2.hash`
3. keccak256 → `bytecode_store_v2.check_hash`
4. `INSERT INTO bytecode_store_v2 (..., kind=1) IF NOT EXISTS`
5. `UPDATE contracts_by_address_v2 SET creation_hash=hash, creation_seq=0 WHERE address=? AND block_number=?`

Идемпотентен. `IF NOT EXISTS` не перезаписывает уже верифицированные строки в store.

```bash
# Run (на 100.64.0.4)
~/backfill_creation_from_snap --host=127.0.0.1 --pass=cassandra --workers=64 --page-size=200 >> ~/backfill_creation_from_snap.log 2>&1

# Build (локально)
cd tools/backfill_creation_from_snap
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o backfill_creation_from_snap .
scp backfill_creation_from_snap alexey_smolyakov@100.64.0.4:~/backfill_creation_from_snap
```

**Status:** задеплоен (2026-07-06), запускается автоматически через watcher после завершения
`backfill_restore_from_snap`. Лог: `~/backfill_creation_from_snap.log`.
Ожидаемые счётчики: `scanned≈101.6M`, `skipped≈10%`, `inserted_new + already_had ≈ 90%`.

---

## `backfill_spans.sh` — targeted re-sync for known gaps (Polygon)

**Polygon-specific** (chain_id=137, BUCKETS=64, ERA=32000, keyspace=pol). For ETH use
`backfill_spans_eth.sh` above.

```bash
export SCYLLA_DB_PASSWORD='...'
./backfill_spans.sh <node_id> <rpc_url> <spans_file> <log_file> [fetch_workers=8] [span_timeout=3600]
```

---

## `count_rows_fast/` — count total rows in contracts_by_address_v2 (2026-07-14)

Считает общее число строк через `SELECT COUNT(*)` по 256 токен-сегментам с bounded pool 16 воркеров.
Обходит зависание `SELECT DISTINCT` с 104 SSTable через серверное агрегирование.
Даёт события деплоя (не уникальные адреса): unique ≈ total - ~330k редеплоев.

```bash
cd tools/count_rows_fast
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 \
  /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o count_rows_fast .
scp count_rows_fast alexey_smolyakov@100.64.0.4:~/count_rows_fast

~/count_rows_fast --pass=cassandra --segments=256 --workers=16
```

**Status:** DONE 2026-07-14, бинарь `~/count_rows_fast` на 100.64.0.4.
**Результат:** total=103,021,077 строк, errors=0, elapsed=69.6s.

---

## `count_creates_by_block/` — count rows in contracts_by_address_v2 filtered by block range (2026-07-18)

Считает строки в `contracts_by_address_v2` с фильтром по `block_number` через token-range сегменты + ALLOW FILTERING.
Используется для разбивки total rows на до/после Byzantium (block 4,370,000) для сравнения с Dune `ethereum.traces type='create'`.

```bash
cd tools/count_creates_by_block
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 \
  /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o count_creates_by_block .
scp count_creates_by_block alexey_smolyakov@100.64.0.4:~/count_creates_by_block

# Pre-Byzantium (blocks 0 – 4,369,999):
~/count_creates_by_block --pass=cassandra --segments=256 --workers=16 --block-lo=0 --block-hi=4369999
```

**Status:** DONE 2026-07-18, бинарь `~/count_creates_by_block` на 100.64.0.4.
**Результат (pre-Byz):** 2,103,120 строк, errors=0, elapsed=82.2s.
**Примечание:** post-Byz run (4370000–25559944) через ALLOW FILTERING не проходит за 180s — 86 сегментов таймаутятся.
Вместо этого post-Byz = total (103,209,367) − pre-Byz (2,103,120) = 101,106,247.

---

## `count_distinct_addrs_v3/` — count unique addresses via sequential scan (2026-07-14)

Считает уникальные адреса через итерирование всех строк (как COUNT(*), но с подсчётом смен адреса).
Более медленный аналог count_rows_fast, но возвращает точное число уникальных партиций.
`GROUP BY address` с timeout 120s не работает при 104 SSTable — только sequential scan.

```bash
cd tools/count_distinct_addrs_v3
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 \
  /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o count_distinct_addrs_v3 .
scp count_distinct_addrs_v3 alexey_smolyakov@100.64.0.4:~/count_distinct_addrs_v3

~/count_distinct_addrs_v3 --pass=cassandra --segments=256 --workers=16
```

**Status:** DONE 2026-07-14, бинарь `~/count_distinct_addrs_v3` на 100.64.0.4.
**Результат:** unique=102,274,193 адреса, errors=0, elapsed=290.5s.

---

## `erc20_stats/` — статистика по erc20_tokens: классификация + метод-флаги + комбинации (2026-07-14)

Один проход по `eth.erc20_tokens`, считает в памяти:
- distribution по `is_fully/partially/minimally/not_following_standard`
- per-flag true/false/null для 5 method-флагов + `is_standard_decimals`
- все 32 комбинации method-флагов (5-bit паттерн) для partial-токенов и для всех токенов

```bash
cd tools/erc20_stats
GOPATH=/home/alex/go GOOS=linux GOARCH=amd64 \
  /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o erc20_stats .
scp erc20_stats alexey_smolyakov@100.64.0.4:~/erc20_stats

~/erc20_stats --pass=cassandra
```

**Status:** DONE 2026-07-14, бинарь `~/erc20_stats` на 100.64.0.4, 97.6s, 0 ошибок.
**Результат:** total=4,149,594 / fully=360,579 (8.69%) / partially=3,789,015 (91.31%).
Топ partial-комбинации: 11000 (balanceOf+transfer, 65.3%), 01000 (transfer only, 27.6%), 11111 (all 5, 9.9%).

---

## `count_rows_checkpoint/` — count rows in chunk-partitioned tables up to checkpoint (2026-07-14)

Считает строки в таблицах blocks, transactions, logs, internal_transactions через `SELECT COUNT(*)` 
per chunk с фильтром `block_number <= checkpoint`. Итерирует все 51072 chunks (eras 0-2127, lanes 0-23).
16 воркеров (64 вызывали cascading timeouts). 5 ретраев на chunk.
Blocks используют колонку `number`, остальные — `block_number`.

```bash
cd tools/count_rows_checkpoint
/home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o count_checkpoint .
scp count_checkpoint alexey_smolyakov@100.64.0.4:~/count_checkpoint

~/count_rows_checkpoint --pass=cassandra --checkpoint=25531491 --workers=16 > ~/count_checkpoint.log 2>&1 &
```

**Status:** DONE 2026-07-14/15 (два прогона, PID 1993231 на 100.64.0.4).
**Результаты @ checkpoint=25531491:**
- blocks: 25,531,492 rows, 0 errors, 77s
- transactions: 3,601,596,013 rows, 0 errors, ~52min
- logs: 7,095,672,307 rows, **56 errors** (недооценка ~33.7M строк — см. find_logs_gap), ~90min
- internal_transactions: 16,266,473,447 rows, **761 errors** (значительная недооценка near-head), 6h8m

**Интерпретация errors:** При 56 ошибках на logs (~600k строк/чанк × 56 ≈ 33M недосчитано).
При 761 ошибке на internal_transactions — реальное число выше 16.27B. Счёт ненадёжен, нужен повтор.

---

## `sum_block_completions/` — sum tx/log/itx counts from block_completions (2026-07-15)

Суммирует `tx_count`, `log_count`, `itx_count` из таблицы `block_completions` по всем chunks
до checkpoint. Быстрая верификация: block_completions записывает сколько строк трансформер
передал на запись — если отличается от фактического числа строк в таблице, значит были потери.

```bash
cd tools/sum_block_completions
/home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o sum_block_completions .
scp sum_block_completions alexey_smolyakov@100.64.0.4:~/sum_block_completions

~/sum_block_completions --pass=cassandra --checkpoint=25531491 --workers=16
```

**Status:** DONE 2026-07-15, 65s, 0 ошибок.
**Результаты @ checkpoint=25531491:**
- tx_count sum:  3,600,832,918
- log_count sum: 7,137,137,737
- itx_count sum: 17,232,935,546

**Интерпретация:** `itx_count` = все трейсы где `from != null AND value != null` (включая value=0x0).
Это больше чем Dune's `ethereum.traces WHERE value > 0` (3.08B) — разница = zero-value internal calls.

---

## `sum_bc_totals/` — SUM(tx_count, log_count, itx_count, contract_count) из block_completions (2026-07-16)

Параллельный сканер: суммирует 4 счётчика из `block_completions` для заданного диапазона блоков.
В отличие от `sum_block_completions` поддерживает `--from`/`--to` для фиксации снапшота при
работающем реалтайм-индексере и добавляет `contract_count`.

```bash
cd tools/sum_bc_totals
/home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o sum_bc_totals .
scp sum_bc_totals alexey_smolyakov@100.64.0.4:~/sum_bc_totals

~/sum_bc_totals --pass=cassandra --from=0 --to=25541725
```

**Status:** DONE 2026-07-16, бинарь `~/sum_bc_totals` на 100.64.0.4, 10.7s, 0 ошибок.
**Результаты @ snapshot block=25,541,725:**
- BC rows (blocks):    25,541,726
- SUM tx_count:         3,605,658,862
- SUM log_count:        7,145,901,310
- SUM itx_count:       17,257,656,847
- SUM contract_count:    103,328,516

---

## `find_logs_gap/` — block-level comparison: block_completions vs logs table (2026-07-15)

Для каждого chunk (51,072 chunks @ checkpoint=25,531,491): читает ожидаемое число логов из
`block_completions.log_count`, читает фактическое через `SELECT COUNT(*) GROUP BY block_number`
из `logs`, сравнивает per block. Пишет в TSV-файл блоки где actual < expected.

**Зачем:** `count_rows_checkpoint` дал logs=7.096B vs block_completions=7.137B (−33.7M).
Нужно проверить — это реальные потери данных или артефакт 56 ошибок COUNT(*).

**Алгоритм:** 2 параллельных Scylla-запроса на chunk, 16 воркеров, 5 ретраев (300ms→30s backoff),
60s timeout per query. Fallback: если GROUP BY таймаутит — полный row scan.

```bash
cd tools/find_logs_gap
/home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o find_logs_gap .
scp find_logs_gap alexey_smolyakov@100.64.0.4:~/find_logs_gap

# ВАЖНО: использовать абсолютный путь для --out (Go не раскрывает ~)
nohup ~/find_logs_gap \
  --pass=cassandra --checkpoint=25531491 \
  --workers=16 \
  --out=/home/alexey_smolyakov/missing_logs.tsv \
  > ~/find_logs_gap.log 2>&1 &
tail -f ~/find_logs_gap.log
```

**Status:** DONE 2026-07-15, PID 2049159, 3h38m (13,050s), 51,072/51,072 chunks.
**Результат: 0 блоков с missing logs, 0 ошибок** — таблица logs полная.
Разрыв −33.7M в count_rows_checkpoint был артефактом 56 ошибок (missed chunks × ~600k строк).
Примечание: файл `missing_logs.tsv` не создан — запуск был с `--out ~/missing_logs.tsv`, Go не
раскрывает `~`. При следующем запуске использовать абсолютный путь.

---

## `src/pipeline/verifier.zig` — BC completeness verifier (integrated in indexer)

**Дата:** 2026-07-16  
**Статус:** реализован и задеплоен как `raw_erc20_v21`  
**Описание:** встроенная в индексер фаза верификации BC (block_completions) между исторической
индексацией и realtime-режимом. Не отдельный инструмент, а часть основного flow.

**Flow:**
1. После завершения исторической синхронизации (и только при переходе в realtime — без `--to`)
2. Загружает `verified_eras` из Scylla — уже верифицированные эры пропускаются
3. Для каждой не-верифицированной эры: сканирует 24 чанка `block_completions`, собирает список
   отсутствующих блоков (те, у кого нет BC-записи = не дописаны до конца при предыдущем краше)
4. Реиндексирует каждый отсутствующий блок через `fetchParseTransform` + `saveBlock` (primary → backup1 → backup2)
5. Отмечает эру как верифицированную в `verified_eras` (era, verified_at_ms, missing_found, missing_reindexed)
6. Если блок не удалось реиндексировать — эра не отмечается, при следующем рестарте повторится
7. Если после всего цикла остались ошибки — возвращает `error.VerificationIncomplete`, indexer
   **не входит** в realtime (data integrity first)

**Scylla таблица:** `eth.verified_eras` — создана 2026-07-16:
```cql
CREATE TABLE IF NOT EXISTS eth.verified_eras (
  era               bigint,
  verified_at_ms    bigint,
  missing_found     int,
  missing_reindexed int,
  PRIMARY KEY (era)
);
```

**Новые prepared statements в `src/db/pool.zig`:**
- `bcScan`: `SELECT block_number FROM block_completions WHERE chunk=? AND block_number>=? AND block_number<=?`
- `verifiedErasAll`: `SELECT era FROM verified_eras`
- `verifiedErasInsert`: `INSERT INTO verified_eras (era,...) VALUES (?,?,?,?)`

**Запуск:** автоматически в составе `raw_erc20_v21` (скрипт `~/run_eth60_v21.sh` на 100.64.0.4).
Бинарь: `raw_erc20_v21` на 100.64.0.4, скрипт: `~/run_eth60_v21.sh`.

**После верификации:** сверка с Dune для финальной валидации данных.

## `retro_reorg_scan/` — ретроспективная проверка реоргов (2026-07-21, рабочий)

Сканирует диапазон блоков, проиндексированных в realtime без сохранения block_hash (т.е. v21 и
ранее, блоки 25,422,404–25,580,592). Для каждого блока с `block_hash IS NULL` (pre-v22) вызывает
`eth_getBlockByNumber` через RPC, сравнивает canonical `tx_count` с сохранённым в Scylla. При
расхождении → запись в `eth.forked_blocks` (block_hash="retro", reorg_group_id="retro_scan_N").

**Алгоритм:** tx_count mismatch = реорг: блок, который мы проиндексировали, находился на другом
форке от того, что сейчас считается каноническим. Ложные срабатывания крайне редки на PoS.

```bash
# Сборка
cd tools/retro_reorg_scan
/home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o retro_reorg_scan .

# Dry-run (только вывод, без записи в БД)
./retro_reorg_scan --pass=cassandra --dry-run

# Production run
./retro_reorg_scan --pass=cassandra

# Деплой на сервер (cross-compile)
GOOS=linux GOARCH=amd64 go build -o retro_reorg_scan_linux .
scp -i ~/.ssh/id_ed25519 retro_reorg_scan_linux alexey_smolyakov@100.64.0.4:~/retro_reorg_scan
```

Флаги: `--from` (def 25422404), `--to` (def 25580592), `--rpc` (def http://100.64.0.60:8545),
`--workers` (def 32), `--dry-run`, `--host/--port/--user/--pass` (Scylla).

**Развёрнут:** `~/retro_reorg_scan` на 100.64.0.4.

---

## `reorg_scanner/` — полный ретроскан реоргов (2026-07-22, рабочий)

Unified reorg detection tool для поиска всех реоргов в диапазоне `[from, to]`.
Два метода обнаружения:
- **FAST (method=hash):** для v22-блоков с `block_hash != null` в `block_completions` — сравнивает сохранённый hash с canonical hash из `eth_getBlockByNumber`. Один дешёвый RPC-вызов.
- **SLOW (method=tx):** для v21-блоков без `block_hash` — сравнивает множество tx hash'ей из `eth.transactions` с canonical tx hash set из RPC.

**Параметры:**
- `--from 25422404` — начальный блок (вкл.)
- `--to 0` — конечный блок (0 = читать из Redis cursor `LATEST_PROCESSED_BLOCK_NUMBER`)
- `--redis redis://:pass@host:port/db` — URL Redis для чтения курсора
- `--workers 16` — параллельные воркеры
- `--output path/to/log` — файл результатов (default: `reorg_scan_FROM_TO_TIMESTAMP.log`)
- `--rpc`, `--host/--port/--user/--pass` — параметры RPC и Scylla

**Изменений в БД не вносит** — только чтение + log-файл.

```bash
# Сборка (cross-compile)
cd tools/reorg_scanner
GOOS=linux GOARCH=amd64 \
  /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o reorg_scanner .
scp -i ~/.ssh/id_ed25519 reorg_scanner alexey_smolyakov@100.64.0.4:~/reorg_scanner

# Запуск (курсор из Redis)
nohup ~/reorg_scanner --from=25422404 --to=0 --workers=16 \
  '--redis=redis://:ZCy8k4G6pcRYVFfm@127.0.0.1:6379/2' \
  --output=~/reorg_scan.log > ~/reorg_scan_stdout.log 2>&1 &
```

**Первый запуск 2026-07-22:**
- Диапазон: 25,422,404–25,587,207 (164,804 блока, 360 чанков)
- Время: ~76 секунд, 16 воркеров, 0 ошибок
- Результат: **17 реоргов** (method=tx: 10, method=hash: 7)
- Лог сохранён: `tools/reorg_scanner/reorg_scan_25422404_25587207.log`
- Бинарь: `~/reorg_scanner` на 100.64.0.4

---

## `verify_inner_phantom/` — verify inner-phantom CREATEs are NOT in Scylla (2026-07-24)

Go-инструмент для верификации фикса трансформера (v26): inner-phantom CREATE-трейсы (вложенные
CREATE в sub-call, который впоследствии ревертнулся) не должны попадать в `contracts_by_address_v2`.

```bash
cd tools/verify_inner_phantom
GOOS=linux GOARCH=amd64 /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o verify_inner_phantom .
scp verify_inner_phantom alexey_smolyakov@100.64.0.4:~/verify_inner_phantom
# На сервере:
~/verify_inner_phantom --host=127.0.0.1 --port=9042 --user=cassandra --pass=cassandra \
  --keyspace=eth --from=25600296 --to=25600500
```

**Результат 2026-07-24:** 205 блоков, **309 inner-phantom адресов — 0 в DB** (PASS).
Бинарь `~/verify_inner_phantom` на 100.64.0.4.

---

## `find_itx_discrepancy/` — 3-way trace comparison: reth vs Geth vs Erigon (2026-07-23)

Python-скрипт `find_discrepancy.py` — запрашивает `trace_block` на нашем reth-узле для
сэмпла из 26 блоков (1 на миллион-эру в диапазоне 0–25M) и применяет фильтр BC-трансформера.
Используется для сравнения с Geth (Dune) и Erigon (QuikNode).

```bash
# Запуск на сервере
python3 ~/find_discrepancy.py > ~/find_discrepancy.log
```

**Статус:** DONE 2026-07-23. Скрипт на сервере: `~/find_discrepancy.py`.
Результаты: `~/find_discrepancy.log` (26 блоков, 0 ошибок).
Дополнено Geth-данными из Dune Q8081241 и Erigon-данными из QuikNode (6 блоков).

**Главный результат:** reth ≡ Erigon для 5/6 блоков; Geth > reth=Erigon (precompile internal depth>0);
reth > Erigon=Geth только на 10,366,004 (+37, CALL-to-EOA reth-артефакт). Детали — DUNE_CHECK.md §8.
