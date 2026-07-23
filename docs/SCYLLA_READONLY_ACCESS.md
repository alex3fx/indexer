# Scylla — read-only доступ и схема базы (ETH, keyspace `eth`)

_Создано: 2026-06-29. Обновлено: 2026-07-07._ Описывает БД ETH ERC-20 индексера на тест-сервере
(`100.64.0.4`) и read-only пользователя `reader`, заведённого для внешнего/аналитического чтения
без риска повлиять на запись (права — только `SELECT` на keyspace `eth`, без `MODIFY`/DDL).

## Доступ

- Хост: `100.64.0.4`, порт `9042` (CQL native protocol)
- Keyspace: `eth`
- Пользователь: `reader`
- Пароль: `LLCcvffYaEhS7pNMCfS1Dbar`
- Права: `SELECT` на `KEYSPACE eth` (проверено: `INSERT`/`UPDATE`/`DELETE` → `Unauthorized`)

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
сети/VPN инфраструктуры lotos. Снаружи без доступа к этой сети не подключиться.

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

## Таблицы

### Основные (партиционированы по `chunk`)

| Таблица | PRIMARY KEY | Назначение |
|---|---|---|
| `blocks` | `(chunk, number)` | Заголовки блоков: `miner`, `timestamp_s`, `timestamp_ms` |
| `transactions` | `(chunk, block_number, transaction_index)` | Транзакции: `hash`, `from_address`, `to_address`, `value`, `gas_*`, `status`, `input`, `method_id` и т.д. |
| `logs` | `(chunk, block_number, transaction_index, log_index)` | Event-логи: `address`, `data`, `topic_zeroth..third`, `rest_topics` (list), `removed` |
| `internal_transactions` | `(chunk, block_number, transaction_index, trace_index)` | Внутренние вызовы/переводы (из trace): `from_address`, `to_address`, `value` |
| `block_completions` | `(chunk, block_number)` | Служебная: счётчики `tx_count`/`log_count`/`itx_count`/`contract_count` на блок |

### Контракты и байткод (v2-таблицы — актуальные)

| Таблица | PRIMARY KEY | Назначение |
|---|---|---|
| `contracts_by_address_v2` | `(address, block_number DESC)` | Деплои контрактов по адресу. **Одна строка на событие деплоя** — если адрес передеплоен, строк несколько. Кластеризация DESC: первая строка = последний деплой. |
| `bytecode_store_v2` | `(hash, seq)` | Дедуплицированный байткод (deployed). `seq` — обычно 0, >0 только при хэш-коллизии. Хранит: `bytecode` (blob, LZ4), `size`, `kind`, `verified`, `abi` (zlib), `source_ref`, `programming_language`. |
| `addresses_by_bytecode` | `(hash, seq, bucket, address)` | Обратный индекс: по хэшу deployed-байткода → список адресов. Одна строка на уникальную (bytecode_hash, address) пару. |
| `addresses_by_creation_bytecode` | `(hash, seq, bucket, address)` | То же, но по хэшу creation-байткода (init code). |
| `collision_registry_v2` | `(hash)` | Регистр хэш-коллизий байткода (разный код → одинаковый хэш). Редко. |
| `source_store` | `(hash, seq, chunk)` | Исходный код контрактов (zlib+chunked, max 512KB/chunk). Доступ через `bytecode_store_v2.source_ref`. |
| `pending_verifications` | `(address)` | Очередь верификаций от watcher-сервиса (временное хранилище). |

### Устаревшая snap-таблица (не используется для новых данных)

| Таблица | PRIMARY KEY | Назначение |
|---|---|---|
| `contracts_by_addresses` | `(address)` | Старая snap-таблица. Одна строка на адрес. Заполнялась snap-индексером из `address_info`. **Устарела**: данные перенесены в `contracts_by_address_v2`. |

### ERC-20 (партиционированы по `address`)

| Таблица | PRIMARY KEY | Назначение |
|---|---|---|
| `erc20_tokens` | `(address)` | Детекция ERC-20: флаги `has_transfer/approve/allowance/balance_of`, `name`, `symbol`, `decimals`, стандарт-флаги |
| `erc20_owners` | `(address)` | Владелец контракта: `initial_owner`, `latest_owner`, `is_ownership_renounced`, `updated_at_block` |
| `erc20_total_supplies` | `(address)` | Total supply: `initial_total_supply`, `latest_total_supply`, `updated_at_block` |
| `erc20_self_destructed` | `(address)` | Самоуничтоженные ERC-20 контракты: `at_block`, `at_timestamp` |

## Полные схемы колонок

**`contracts_by_address_v2`**:
```
address text,
block_number bigint,             -- clustering DESC
block_timestamp_ms bigint,
block_timestamp_s bigint,
bytecode_hash blob,              -- sha256 deployed bytecode; ссылка в bytecode_store_v2
bytecode_seq tinyint,            -- обычно 0; >0 при хэш-коллизии
contract_factory text,           -- адрес factory-контракта если деплой внутренний; "" если прямой
creation_hash blob,              -- sha256 creation bytecode; ссылка в addresses_by_creation_bytecode
creation_method tinyint,         -- 0=CREATE, 1=CREATE2, 99=unknown
creation_seq tinyint,
deployer text,                   -- tx.from_address (инициатор транзакции)
trace_index int,
transaction_index int,
tx_hash text                     -- null → ghost-строка (reverted CREATE2, не существует на блокчейне)
```

**`bytecode_store_v2`**:
```
hash blob,                       -- sha256 deployed bytecode (PK)
seq tinyint,                     -- 0 в норме; >0 при хэш-коллизии
abi blob,                        -- zlib-compressed JSON ABI (null если не верифицирован)
bytecode blob,                   -- raw deployed bytecode
check_hash blob,
first_seen_block bigint,
kind tinyint,
programming_language text,       -- "Solidity", "Vyper", и т.д. (null если не верифицирован)
size int,                        -- размер bytecode в байтах
source_ref text,                 -- ссылка на source_store (hex(hash:seq))
verified boolean,                -- верифицирован ли контракт
verified_at timestamp,
verified_via_address blob        -- адрес через который была получена верификация
```

**`addresses_by_bytecode`** / **`addresses_by_creation_bytecode`**:
```
hash blob,      -- sha256 bytecode (partition key part 1)
seq tinyint,    -- 0 в норме (partition key part 2)
bucket smallint,(partition key part 3; для шардирования больших partitions)
address text    -- (clustering key)
block_number bigint
```

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

**`source_store`**: `hash blob, seq tinyint, chunk int, data blob`
_(данные zlib-compressed, читать через decompress; chunk = индекс части 512KB)_

**`pending_verifications`**: `address text, abi blob, programming_language text,
received_at timestamp, source blob`

## Итерация контрактов: address + bytecode_hash

Основной паттерн для watcher — перебрать все задеплоенные контракты (адрес + хэш байткода):

```python
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import threading
import math

auth = PlainTextAuthProvider(username="reader", password="LLCcvffYaEhS7pNMCfS1Dbar")
cluster = Cluster(["100.64.0.4"], port=9042, auth_provider=auth)
session = cluster.connect("eth")

MIN_TOKEN = -(2**63)
MAX_TOKEN = 2**63 - 1
SEGMENTS = 256

def scan_range(lo, hi, last, results):
    # SELECT DISTINCT возвращает по одной строке на уникальный address
    # (первый/последний деплой — зависит от движка)
    # Для полного перебора всех событий деплоя — убрать DISTINCT
    if last:
        rows = session.execute(
            "SELECT address, bytecode_hash, tx_hash, block_number, creation_method, contract_factory "
            "FROM eth.contracts_by_address_v2 "
            "WHERE token(address) >= %s",
            (lo,)
        )
    else:
        rows = session.execute(
            "SELECT address, bytecode_hash, tx_hash, block_number, creation_method, contract_factory "
            "FROM eth.contracts_by_address_v2 "
            "WHERE token(address) >= %s AND token(address) < %s",
            (lo, hi)
        )
    for row in rows:
        if not row.tx_hash:
            continue  # пропустить ghost-строки (reverted CREATE2)
        results.append({
            "address": row.address,
            "bytecode_hash": row.bytecode_hash.hex() if row.bytecode_hash else None,
            "tx_hash": row.tx_hash,
            "block_number": row.block_number,
        })

total = float(MAX_TOKEN) - float(MIN_TOKEN) + 1
step = total / SEGMENTS
boundaries = [int(MIN_TOKEN + i * step) for i in range(SEGMENTS)]
boundaries.append(MAX_TOKEN)

all_results = []
threads = []
for i in range(SEGMENTS):
    r = []
    all_results.append(r)
    last = (i == SEGMENTS - 1)
    t = threading.Thread(target=scan_range, args=(boundaries[i], boundaries[i+1], last, r))
    threads.append(t)

for t in threads: t.start()
for t in threads: t.join()

contracts = [item for sublist in all_results for item in sublist]
print(f"total contracts scanned: {len(contracts)}")
cluster.shutdown()
```

**Получить bytecode и ABI для конкретного адреса:**

```python
# 1. Получить bytecode_hash адреса
row = session.execute(
    "SELECT address, bytecode_hash, bytecode_seq, tx_hash, block_number "
    "FROM eth.contracts_by_address_v2 WHERE address=%s",
    ("0xabcd...",)
).one()

if row and row.bytecode_hash:
    # 2. Получить bytecode из bytecode_store_v2
    bc = session.execute(
        "SELECT bytecode, size, verified, abi, programming_language, source_ref "
        "FROM eth.bytecode_store_v2 WHERE hash=%s AND seq=%s",
        (row.bytecode_hash, row.bytecode_seq or 0)
    ).one()
    print(f"size={bc.size} verified={bc.verified} lang={bc.programming_language}")
    if bc.abi:
        import zlib, json
        abi = json.loads(zlib.decompress(bc.abi))
```

**Найти все контракты с тем же байткодом:**

```python
# addresses_by_bytecode: один бакет = много адресов
for bucket in range(4):  # попробуй bucket 0..3, обычно хватает 0
    rows = session.execute(
        "SELECT address, block_number FROM eth.addresses_by_bytecode "
        "WHERE hash=%s AND seq=0 AND bucket=%s",
        (bytecode_hash_bytes, bucket)
    )
    for r in rows:
        print(r.address, r.block_number)
```

## Обход таблиц через token range

Для chunk-based таблиц перебирать `chunk` явно — неэффективно (murmur3 хэш от chunk не коррелирует
с арифметикой lane/era). Правильный паттерн — резать кольцо токенов на N диапазонов:

```sql
SELECT * FROM transactions WHERE token(chunk) >= ? AND token(chunk) < ?;
```

Для `address`-based таблиц (`contracts_by_address_v2`, ERC-20) — аналогично:
```sql
SELECT ... FROM contracts_by_address_v2 WHERE token(address) >= ? AND token(address) < ?;
```

Замечания:
- `SEGMENTS=256` — разумный дефолт; крупные диапазоны таймаутят на `logs`/`internal_transactions`
- Для скана по диапазону **блоков** эффективнее вычислить нужные `chunk` по формуле выше
  и делать `WHERE chunk = ? AND block_number >= ? AND block_number < ?` на каждый chunk
- Ретраи с backoff на таймаутах обязательны — штатная ситуация на таких объёмах
- `contracts_by_address_v2` имеет clustering key `block_number DESC` → первая строка на адрес = **последний деплой**

## Примечания по данным

- **ghost-строки** (`tx_hash = null`): ~102,640 строк — reverted CREATE2-деплои, адрес был
  записан snap-индексером но транзакция не прошла. Всегда фильтровать по `tx_hash IS NOT NULL`
  (в CQL: проверять пустую строку в коде, т.к. CQL не поддерживает `WHERE tx_hash != null`).
- **creation_method**: `0=CREATE`, `1=CREATE2`, `99=unknown`
- **contract_factory**: пустая строка `""` = прямой деплой; непустая = деплой через factory-контракт
- **bytecode_hash** в `contracts_by_address_v2`: `blob`, 32 байта SHA-256 deployed bytecode.
  Соответствует `hash` в `bytecode_store_v2` и `addresses_by_bytecode`.
- **source_store**: chunked (chunk=0,1,2,...), каждый chunk до 512KB, данные zlib-compressed.
