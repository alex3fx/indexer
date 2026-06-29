# Scylla — read-only доступ и схема базы (ETH, keyspace `eth`)

_Создано: 2026-06-29._ Описывает БД ETH ERC-20 индексера на тест-сервере (`100.64.0.4`) и
read-only пользователя `reader`, заведённого для внешнего/аналитического чтения без риска
повлиять на запись (права — только `SELECT` на keyspace `eth`, без `MODIFY`/DDL).

## Доступ

- Хост: `100.64.0.4`, порт `9042` (CQL native protocol)
- Keyspace: `eth`
- Пользователь: `reader`
- Пароль: `LLCcvffYaEhS7pNMCfS1Dbar`
- Права: `SELECT` на `KEYSPACE eth` (проверено: `INSERT`/`UPDATE`/`DELETE` отдаются `Unauthorized`)

Строка подключения (cqlsh):
```bash
cqlsh 100.64.0.4 9042 -u reader -p 'LLCcvffYaEhS7pNMCfS1Dbar' -k eth
```

Python (`cassandra-driver`):
```python
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

auth = PlainTextAuthProvider(username="reader", password="LLCcvffYaEhS7pNMCfS1Dbar")
cluster = Cluster(["100.64.0.4"], port=9042, auth_provider=auth)
session = cluster.connect("eth")
```

**Важно про сеть**: `100.64.0.4` — приватный (CGNAT) адрес, доступен только из внутренней
сети/VPN инфраструктуры lotos. Снаружи без доступа к этой сети не подключится.

## Топология кластера

- Один узел, `org.apache.cassandra.dht.Murmur3Partitioner` (token range: `-2^63 … 2^63-1`)
- `num_tokens: 256` (vnodes), `murmur3_partitioner_ignore_msb_bits: 12`
- Keyspace replication: `SimpleStrategy`, RF=1 (нет репликации — единственная копия данных)

## Схема партиционирования ("chunk")

Основные таблицы (блоки/транзакции/логи/итд) партиционированы по столбцу `chunk` (int),
который вычисляется из номера блока по формуле **v3 "lane + era"**:

```
lane  = block_number % 24
era   = block_number // 12000      (целочисленное деление)
chunk = lane + 24 * era
```

На каждые 12 000 последовательных блоков приходится 24 партиции — внутри одной партиции
блоки идут с шагом 24 (один и тот же `lane`), что ограничивает размер ~500 блоков/партицию.

Внутри партиции данные отсортированы по `block_number` (и далее по `transaction_index`/
`log_index`/`trace_index`, где применимо).

Пример: блок `25_422_600` → `lane = 25422600 % 24 = 0`, `era = 25422600 // 12000 = 2118`,
`chunk = 0 + 24*2118 = 50832`.

ERC-20 таблицы (`erc20_tokens`, `erc20_owners`, `erc20_total_supplies`, `erc20_self_destructed`)
партиционированы по `address` (text) — без `chunk`, напрямую по адресу контракта.

## Таблицы

### Основные (партиционированы по `chunk`)

| Таблица | PRIMARY KEY | Назначение |
|---|---|---|
| `blocks` | `(chunk, number)` | Заголовки блоков: `miner`, `timestamp_s`, `timestamp_ms` |
| `transactions` | `(chunk, block_number, transaction_index)` | Транзакции: `hash`, `from_address`, `to_address`, `value`, `gas_*`, `status`, `input`, `method_id` и т.д. |
| `logs` | `(chunk, block_number, transaction_index, log_index)` | Event-логи: `address`, `data`, `topic_zeroth..third`, `rest_topics` (list), `removed` |
| `internal_transactions` | `(chunk, block_number, transaction_index, trace_index)` | Внутренние вызовы/переводы (из trace): `from_address`, `to_address`, `value` |
| `contracts` | `(chunk, block_number, transaction_index, trace_index)` | Деплои контрактов: `address`, `creator_address`, `creation_bytecode`, `deployed_bytecode`, `contract_factory` |
| `block_completions` | `(chunk, block_number)` | Служебная: счётчики `tx_count`/`log_count`/`itx_count`/`contract_count` на блок |

### Поиск контрактов по адресу / bytecode

| Таблица | PRIMARY KEY | Назначение |
|---|---|---|
| `contracts_by_addresses` | `(address)` | Деплои по адресу контракта: `creator`, `tx_hash`, `block_number`, `timestamp`, `creation_bytecode`, `deployed_bytecode`, `contract_factory` |
| `contracts_by_bytecode_hash` | `(bytecode_hash, address)` | Индекс контрактов по хэшу деплоед-байткода: `creation_block` |
| `bytecode_store` | `(bytecode_hash)` | Дедуплицированный байткод: `bytecode`, `size`, `first_seen_block`, `has_collision` |
| `bytecode_collision_registry` | `(bytecode_hash, address, bytecode)` | Коллизии хэшей (разный байткод, одинаковый хэш): `creation_block` |

### ERC-20 (партиционированы по `address`)

| Таблица | PRIMARY KEY | Назначение |
|---|---|---|
| `erc20_tokens` | `(address)` | Детекция ERC-20: флаги `has_transfer/approve/allowance/balance_of`, `name`, `symbol`, `decimals`, `is_*_following_standard` |
| `erc20_owners` | `(address)` | Владелец контракта: `initial_owner`, `latest_owner`, `is_ownership_renounced`, `updated_at_block` |
| `erc20_total_supplies` | `(address)` | Total supply: `initial_total_supply`, `latest_total_supply`, `updated_at_block` |
| `erc20_self_destructed` | `(address)` | Самоуничтоженные контракты: `at_block`, `at_timestamp` |

### Полные схемы колонок

**`blocks`**: `chunk int, number bigint, miner text, timestamp_s bigint, timestamp_ms bigint`

**`transactions`**: `chunk int, block_number bigint, transaction_index int, hash text,
from_address text, to_address text, value varint, gas_limit bigint, gas_price bigint,
gas_used bigint, max_priority_fee_per_gas bigint, max_fee_per_gas bigint,
cumulative_gas_used bigint, effective_gas_price bigint, contract_address text,
status tinyint, type tinyint, method_id text, input text,
block_timestamp_s bigint, block_timestamp_ms bigint`

**`logs`**: `chunk int, block_number bigint, transaction_index int, log_index int,
address text, data text, topic_zeroth text, topic_first text, topic_second text,
topic_third text, rest_topics list<text>, transaction_hash text, removed boolean,
block_timestamp_s bigint, block_timestamp_ms bigint`

**`internal_transactions`**: `chunk int, block_number bigint, transaction_index int,
trace_index int, from_address text, to_address text, value varint,
transaction_hash text, block_timestamp_s bigint, block_timestamp_ms bigint`

**`contracts`**: `chunk int, block_number bigint, transaction_index int, trace_index int,
address text, creation_method tinyint, creator_address text, contract_factory text,
creation_bytecode text, deployed_bytecode text, transaction_hash text,
block_timestamp_s bigint, block_timestamp_ms bigint`

**`contracts_by_addresses`**: `address text, creator text, tx_hash text,
block_number bigint, timestamp bigint, contract_factory text,
creation_bytecode text, deployed_bytecode text`

**`erc20_tokens`**: `address text, chain_id int, decimals smallint, detection_version int,
has_allowance boolean, has_approve boolean, has_balance_of boolean, has_transfer boolean,
has_transfer_from boolean, is_fully_following_standard boolean,
is_minimally_following_standard boolean, is_not_following_standard boolean,
is_partially_following_standard boolean, is_standard_decimals boolean,
name text, symbol text`

**`erc20_owners`**: `address text, chain_id int, initial_owner text, latest_owner text,
is_ownership_renounced boolean, updated_at_block bigint, updated_at_timestamp bigint`

**`erc20_total_supplies`**: `address text, chain_id int, initial_total_supply text,
latest_total_supply text, updated_at_block bigint, updated_at_timestamp bigint`

**`erc20_self_destructed`**: `address text, chain_id int, at_block bigint, at_timestamp bigint`

**`block_completions`**: `chunk int, block_number bigint, tx_count int, log_count int,
itx_count int, contract_count int`

## Обход таблиц через token range (без `chunk`-перечисления)

Для `chunk`-based таблиц перебирать `chunk` явно — **неэффективно**: murmur3 хэш от `chunk`
не связан с арифметикой lane/era. Правильный паттерн — резать кольцо токенов на N диапазонов:

```sql
SELECT * FROM transactions WHERE token(chunk) > ? AND token(chunk) <= ?;
```

Пример на Python (параллельный token-range scan):

```python
import threading
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

auth = PlainTextAuthProvider(username="reader", password="LLCcvffYaEhS7pNMCfS1Dbar")
cluster = Cluster(["100.64.0.4"], port=9042, auth_provider=auth)
session = cluster.connect("eth")

MIN_TOKEN = -(2**63)
MAX_TOKEN = 2**63 - 1
RANGES = 256
TABLE = "transactions"

def scan_range(lo, hi, results):
    rows = session.execute(
        f"SELECT * FROM {TABLE} WHERE token(chunk) > %s AND token(chunk) <= %s",
        (lo, hi)
    )
    count = sum(1 for _ in rows)
    results.append(count)

step = (MAX_TOKEN - MIN_TOKEN) // RANGES
boundaries = [MIN_TOKEN + i * step for i in range(RANGES + 1)]
boundaries[-1] = MAX_TOKEN

results = []
threads = [threading.Thread(target=scan_range, args=(boundaries[i], boundaries[i+1], results))
           for i in range(RANGES)]
for t in threads: t.start()
for t in threads: t.join()

print("total rows:", sum(results))
cluster.shutdown()
```

Для `address`-based таблиц (ERC-20) — аналогично `WHERE token(address) > ? AND token(address) <= ?`.

Замечания:
- `RANGES=256` — разумный дефолт; крупные диапазоны таймаутят на `logs`/`internal_transactions`
- Для скана по диапазону **блоков** эффективнее вычислить нужные `chunk` по формуле выше
  и делать `WHERE chunk = ? AND block_number >= ? AND block_number < ?` на каждый chunk
- Ретраи с backoff на таймаутах обязательны — штатная ситуация на таких объёмах
