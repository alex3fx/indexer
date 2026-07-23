# Деплой индексера на сервер 100.64.0.4

## Сборка

```bash
cd /home/alex/lotos/task1/devindexer/indexer-erc20
/home/alex/lotos/zig-x86_64-linux-0.17.0-dev.263+0add2dfc4/zig build \
  -p .zig/build --cache-dir .zig/.cache -Doptimize=ReleaseFast
# бинарь: .zig/build/bin/raw
```

Только `-Doptimize=ReleaseFast` — Debug вешает pool workers (Zig 0.17-dev).

## Версионирование

Текущая версия: `raw_erc20_v22`. Следующую называть `raw_erc20_v23`, `v24` и т.д.

## Процедура обновления

### 1. Скопировать бинарь

```bash
scp -i ~/.ssh/id_ed25519 \
  .zig/build/bin/raw \
  alexey_smolyakov@100.64.0.4:~/raw_erc20_vNN
```

`Text file busy` при scp → процесс с тем же именем ещё жив, останови его сначала.

### 2. Создать wrapper-скрипт

Скопировать `~/run_eth60_v22.sh` на сервере, заменить `BIN` и `LOG`:

```bash
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4
cp ~/run_eth60_v22.sh ~/run_eth60_vNN.sh
# отредактировать BIN и LOG внутри
chmod +x ~/run_eth60_vNN.sh
```

### 3. Найти wrapper предыдущей версии и бинарь

```bash
ps aux | grep -E 'run_eth60|raw_erc' | grep -v grep
# пример:
#   PID1  bash /home/alexey_smolyakov/run_eth60_v22.sh   ← wrapper
#   PID2  /home/alexey_smolyakov/raw_erc20_v22 ...       ← бинарь
```

Нужно убить оба: сначала wrapper (иначе перезапустит бинарь), потом бинарь.

```bash
kill <PID_WRAPPER>   # убиваем wrapper первым
kill <PID_BINARY>    # бинарь больше не перезапустится
# подождать и проверить:
sleep 3 && ps aux | grep raw_erc | grep -v grep
```

### 4. Запустить новый wrapper

**Важно:** два `nohup cmd &` в одной ssh-команде — второй иногда не стартует.
Запускать **отдельной** ssh-командой:

```bash
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "nohup ~/run_eth60_vNN.sh >> /dev/null 2>&1 &"
```

### 5. Проверить запуск

```bash
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "sleep 5 && ps aux | grep -E 'run_eth60|raw_erc' | grep -v grep"
# ожидается: wrapper vNN + бинарь vNN
```

```bash
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "tail -20 ~/eth_index_60_vNN.log"
# ожидается: лог блоков (историч. catch-up или [rt] realtime)
```

### 6. Проверить Redis cursor

```bash
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "redis-cli -u 'redis://:ZCy8k4G6pcRYVFfm@127.0.0.1:6379/2' \
   GET LATEST_PROCESSED_BLOCK_NUMBER"
```

## Схема БД — накатить миграции

Если бинарь добавляет новые таблицы или колонки, применить через cqlsh перед запуском:

```bash
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "docker exec scylla cqlsh -u cassandra -p cassandra -e 'ALTER TABLE ...'"
```

Файлы схем: `scripts/db/models/lookups/`.

## Текущие env-переменные (realtime, DB=2)

```
MODE=production
EVM_CHAIN_ID=1
PRIMARY_RPC_HTTPS=http://100.64.0.60:8545
PRIMARY_RPC_WSS=ws://100.64.0.60:8546
BACKUP_RPC_HTTPS=http://100.64.0.7:8545
BACKUP_RPC_HTTPS_2=https://ethereum-rpc.publicnode.com
CM_CONNECTION_URL=redis://:ZCy8k4G6pcRYVFfm@127.0.0.1:6379/2
SCYLLA_DB_HOST=127.0.0.1
SCYLLA_DB_PORT=9042
SCYLLA_DB_KEYSPACE=eth
SCYLLA_DB_USERNAME=cassandra
SCYLLA_DB_PASSWORD=cassandra
SCYLLA_CHUNK_BUCKETS=24
SCYLLA_CHUNK_ERA=12000
FETCH_WORKERS=64
SAVE_EVERY=100
LOGS_GRAYLOG_HOST=144.76.108.185
LOGS_GRAYLOG_PORT=12201
LOGS_GRAYLOG_APP=indexer-eth-60
```

## Watermark логика

Wrapper-скрипт читает `Accum \d+→\K\d+` из своего лог-файла как watermark для `--from`.
Если лог новый (нет строк Accum) — стартует с дефолта `25422404`.
Redis cursor (DB=2) всегда выше, поэтому исторический catch-up проходится быстро.
