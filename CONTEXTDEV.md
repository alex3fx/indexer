# Dev Context

## Ветки

- **dev_test** — основная ветка разработки, полная история коммитов
- **alex_dev** — свёрнутая версия dev_test для Bitbucket (origin)
  - 2 коммита: `INIT` (базовое состояние) + `indexer mvp` (все изменения)
  - Пушится вручную с force после каждого обновления

## Схема работы

```
dev_test (full history)
  │
  ├── commit A
  ├── commit B       ──→  alex_dev:  INIT + "indexer mvp"  ──→  origin (Bitbucket)
  └── commit C
```

### Цикл обновления alex_dev

1. Накопить несколько коммитов в `dev_test`
2. Переключиться на `alex_dev`:

```bash
git checkout alex_dev
git diff dev_test alex_dev | git apply --index
git commit --amend --no-edit
git push origin alex_dev --force
```

Или при первом переносе изменений (apply + amend):
```bash
# Находясь в alex_dev:
git diff dev_test alex_dev | git apply --index
git commit --amend --no-edit
git push origin alex_dev --force
```

## Сборка и деплой на сервер

**Сервер:** `100.64.0.4`, пользователь `alexey_smolyakov`, ключ `~/.ssh/id_ed25519`  
**Zig:** только локально — на сервере не установлен.

### Сборка

```bash
cd /home/alex/lotos/task1/devindexer/indexer
/home/alex/lotos/zig-x86_64-linux-0.17.0-dev.263+0add2dfc4/zig build -Doptimize=ReleaseFast
# результат: zig-out/bin/raw (~10MB)
```

> Только ReleaseFast — Debug дедлочит pool workers (Zig 0.17-dev async IO).

### Загрузка на сервер

```bash
scp -i ~/.ssh/id_ed25519 zig-out/bin/raw alexey_smolyakov@100.64.0.4:~/raw_dev_test_N
```

Имя `raw_dev_test_N` — инкрементируем N при каждом деплое.

### Запуск (через скрипт — нельзя передать env через nohup напрямую)

```bash
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 "cat > /tmp/start_dev.sh << 'EOF'
#!/bin/bash
export MODE=production
export EVM_CHAIN_ID=1
export CM_CONNECTION_URL='redis://:ZCy8k4G6pcRYVFfm@127.0.0.1:6379/0'
export SCYLLA_DB_HOST=127.0.0.1
export SCYLLA_DB_PORT=9042
export SCYLLA_DB_KEYSPACE=eth
export SCYLLA_DB_USERNAME=cassandra
export SCYLLA_DB_PASSWORD=cassandra
exec /home/alexey_smolyakov/raw_dev_test_N
EOF
chmod +x /tmp/start_dev.sh"

ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "nohup /tmp/start_dev.sh > ~/overnight.log 2>&1 &"
```

### Остановка предыдущей версии

```bash
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "pgrep -f raw_dev_test_N && kill \$(pgrep -f raw_dev_test_N)"
```

Найти все запущенные индексеры (не только dev):
```bash
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "ps -u alexey_smolyakov -o pid,comm,args | grep -v -E 'sleep|python|systemd|sd-pam|PM2|bash|ps|grep|sshd'"
```

## Мониторинг

### Лог

```bash
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 "tail -f ~/overnight.log"
```

Формат строки реалтайм-блока:
```
[rt] blk=25273158 tx=273 log=682 itx=1427 kb=512+720+1314 | fetch=44 parse=5 xform=1 save=17 cursor=0 | total=67ms
```
- `kb=A+B+C` — размер ответов: block+receipts+traces в KB
- `fetch` — параллельный HTTP (все три запроса вместе), мс
- `parse` — JSON парсинг, мс
- `xform` — трансформация в DB-строки, мс
- `save` — запись в Scylla (CQL batch), мс
- `cursor` — запись курсора в Redis, мс

### Проверка ошибок

```bash
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "grep -v '^\[rt\]\|^EVM Indexer\|^RPC:\|^WSS:\|^Scylla:\|^Sync:\|^\[node_probe\]\|^Historical:\|^Accum\|^Realtime mode\|^Connecting\|^Starting\|^\[2' ~/overnight.log | grep -v '^\s*$'"
```

Нормально: только `Historical sync done: Xms  saved_blocks=N`  
Аномалии: строки `[realtime] block=... unavailable`, `WSS error`, `fatal:`

### Статистика метрик (Python)

```bash
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 "grep '^\[rt\]' ~/overnight.log | tail -60" | python3 -c "
import sys, re
rows = []
for line in sys.stdin:
    m = re.search(r'kb=(\d+)\+(\d+)\+(\d+).*fetch=(\d+).*parse=(\d+).*xform=(\d+).*save=(\d+).*total=(\d+)', line)
    if m:
        kb = int(m.group(1)) + int(m.group(2)) + int(m.group(3))
        rows.append({'kb': kb, 'fetch': int(m.group(4)), 'parse': int(m.group(5)),
                     'xform': int(m.group(6)), 'save': int(m.group(7)), 'total': int(m.group(8))})
n = len(rows)
print(f'Блоков: {n}')
def stats(vals, label, unit):
    s = sorted(vals)
    avg = sum(s)/len(s)
    p50 = s[len(s)//2]
    p95 = s[int(len(s)*0.95)]
    print(f'  {label:<8} avg={avg:6.1f}  p50={p50:5}  p95={p95:5}  {unit}')
print('\n=== мс ===')
for key in ['fetch','parse','xform','save','total']:
    stats([r[key] for r in rows], key, 'ms')
print('\n=== мкс/КБ ===')
for key in ['fetch','parse','save','total']:
    stats([r[key]*1000/r['kb'] for r in rows if r['kb']>0], key, 'µs/KB')
print(f'\navg KB/blk: {sum(r[\"kb\"] for r in rows)//n}')
"
```

Ориентиры (Ethereum mainnet, зона 25M, локальный RETH):
| stage | avg µs/KB | p95 µs/KB |
|-------|----------:|----------:|
| fetch |      ~20  |      ~27  |
| parse |       ~2  |       ~3  |
| save  |       ~6  |       ~9  |
| total |      ~29  |      ~38  |

### Redis курсор

```bash
ssh -i ~/.ssh/id_ed25519 alexey_smolyakov@100.64.0.4 \
  "redis-cli -u 'redis://:ZCy8k4G6pcRYVFfm@127.0.0.1:6379/0' GET LATEST_PROCESSED_BLOCK_NUMBER"
```
