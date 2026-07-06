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

## `bytecode_api/` — HTTP service: bytecode lookup, clone-search, verification (active, v6)

Go HTTP service для интеграции с watcher. Читает из v2-таблиц (`contracts_by_address_v2`,
`addresses_by_bytecode` — 256 бакетов параллельно), пишет при верификации.
Фильтрует CREATE2-редеплои: `/same` spot-проверяет текущий байткод каждого адреса.

**Auth:** все endpoints (кроме `/health`) требуют header `Api-Access-Key: <key>`. Без ключа — 401.

**Endpoints (v6):**

```
GET  /health                                      → "ok" (без авторизации)
GET  /contract?address=0x{addr}                   → {address, block_number, block_timestamp(ms),
                                                     tx_hash, contract_creator, contract_factory,
                                                     verified, verified_at(ms), programming_language,
                                                     abi, deployed_bytecode, creation_bytecode, source}
GET  /same?address=0x{addr}[&limit=N&offset=N]    → {hash, seq, total, count, offset, limit, addresses}
GET  /same?deployed_bytecode=0x{hex}[&limit&offset]
GET  /same?creation_bytecode=0x{hex}[&limit&offset]→ via addresses_by_creation_bytecode
POST /same  {"address"|"deployed_bytecode"|"creation_bytecode": "...", "limit":N, "offset":N}
POST /verify {"address","abi","source","programming_language"} → {status, verified_at(ms), ...}
```

`/verify`: `abi` и `source` **обязательны вместе** (одно без другого → 400). Оба plain-text.
`verified_at` и `block_timestamp` — int64 в миллисекундах (не строки).
Если адрес не найден — пишет в `pending_verifications`, возвращает `status:pending`.

Полная документация: **`VERIFICATION_API.md`** в корне репо.

**Source:** `tools/bytecode_api/main.go` + `go.mod`/`go.sum`

**Build** (локально через Go 1.26.4):

```bash
cd tools/bytecode_api
GOOS=linux GOARCH=amd64 /home/alex/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go build -o bytecode_api_v6 .
scp bytecode_api_v5 alexey_smolyakov@100.64.0.4:~/bytecode_api_v5
```

**Deployed:** `100.64.0.4:8080`, binary `~/bytecode_api_v5`, run script `~/run_bytecode_api.sh`
(user: cassandra — нужен для записи при `/verify`)

```bash
# Start (tmux session bytecode_api на 100.64.0.4):
tmux new-session -d -s bytecode_api '~/run_bytecode_api.sh >> ~/bytecode_api.log 2>&1'

# Test:
KEY="Api-Access-Key: 354bf5a9-a29a-4879-9f3d-d3c09c6a610a"
curl http://100.64.0.4:8080/health
curl -H "$KEY" "http://100.64.0.4:8080/same?address=0x219e497a09202a3534f653e63faaeab6689c1d22"

# Stop:
tmux kill-session -t bytecode_api
```

**Status:** running (2026-07-04), binary `bytecode_api_v6`. Port 8080 bound to `0.0.0.0`.
Scylla auth: user `cassandra` (read+write для `/verify`), password in `run_bytecode_api.sh`.
Auto-restart: run script содержит `while true` loop — при падении рестартует через 5с.

**Changelog:**
- **v6 (2026-07-04):** полный ETHSCAN-совместимый `/contract` (block_timestamp ms, contract_creator, contract_factory, programming_language, abi array, deployed_bytecode hex, creation_bytecode hex, source plain text); `/verify` принимает programming_language; `/same` pagination (limit/offset, total/count); creation_bytecode wired к addresses_by_creation_bytecode; verified_at → int64 ms везде.
- **v5 (2026-07-04):** авторизация `Api-Access-Key`, `/verify` требует abi+source вместе, `bytecode` → `deployed_bytecode` в `/same`, `creation_bytecode` возвращает 501, `/contract` возвращает оба bytecode hash.
- **v4 (2026-07-03):** удалён `/bytecode` endpoint (читал старую таблицу `contracts_by_addresses`).
- **v3 (2026-07-02):** pending_verifications, source_store chunked, chain_id guard.
Использовать `/contract` вместо `/bytecode`.

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
