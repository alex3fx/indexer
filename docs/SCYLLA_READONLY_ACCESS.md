# Scylla — read-only доступ и схема базы (Polygon, keyspace `pol`)

_Создано: 2026-06-23._ Описывает БД индексера Polygon на проде (`100.64.0.64`) и read-only
пользователя `reader`, заведённого для внешнего/аналитического чтения без риска повлиять на
запись (PK=`reader`, права — только `SELECT` на keyspace `pol`, без `MODIFY`/DDL).

## Доступ

- Хост: `100.64.0.64`, порт `9042` (CQL native protocol)
- Keyspace: `pol`
- Пользователь: `reader`
- Пароль: **передаётся отдельно, не хранится в этом документе** (см. секретный канал/менеджер
  паролей команды)
- Права: `SELECT` на `KEYSPACE pol` (проверено: `INSERT`/`UPDATE`/`DELETE` отдаются `Unauthorized`)

Строка подключения (cqlsh), `<PASSWORD>` — подставить реальный пароль:
```bash
cqlsh 100.64.0.64 9042 -u reader -p '<PASSWORD>' -k pol
```

Python (`cassandra-driver`):
```python
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

auth = PlainTextAuthProvider(username="reader", password="<PASSWORD>")
cluster = Cluster(["100.64.0.64"], port=9042, auth_provider=auth)
session = cluster.connect("pol")
```

JDBC-подобная строка (для инструментов, которые её ожидают):
```
cassandra://reader:<PASSWORD>@100.64.0.64:9042/pol
```

**Важно про сеть**: `100.64.0.64` — приватный (CGNAT) адрес, доступен только из внутренней сети/VPN
инфраструктуры lotos. Снаружи без доступа к этой сети не подключится.

## Топология кластера

- Один узел, `org.apache.cassandra.dht.Murmur3Partitioner` (token range: `-2^63 … 2^63-1`)
- `num_tokens: 256` (vnodes), `murmur3_partitioner_ignore_msb_bits: 12`
- Keyspace replication: `SimpleStrategy`, RF=1 (нет репликации — единственная копия данных)

## Схема партиционирования ("chunk")

Все основные таблицы партиционированы по столбцу `chunk` (int), который вычисляется из номера
блока по формуле **"lane + era"**:

```
lane = block_number % 64
era  = block_number / 32000        (целочисленное деление)
chunk = lane + 64 * era
```

То есть на каждые 32 000 последовательных блоков приходится 64 партиции (`chunk`-а) — внутри одной
партиции номера блоков идут с шагом 64 (один и тот же `lane`). Это ограничивает размер партиции
~500 блоков на партицию при равномерной плотности.

Внутри партиции данные отсортированы по `block_number` (и далее по `transaction_index`/`log_index`/
`trace_index`, где применимо) — `CLUSTERING ORDER BY (block_number ASC, ...)`.

Пример: блок `31267567` → `lane = 31267567 % 64 = 47`, `era = 31267567 // 32000 = 977`,
`chunk = 47 + 64*977 = 62575`.

## Таблицы

Все таблицы партиционированы по `chunk` (кроме `contracts_by_addresses`, см. ниже).

| Таблица | PRIMARY KEY | Назначение |
|---|---|---|
| `blocks` | `(chunk, number)` | Заголовки блоков: `miner`, `timestamp_s`, `timestamp_ms` |
| `transactions` | `(chunk, block_number, transaction_index)` | Транзакции: `hash`, `from_address`, `to_address`, `value`, `gas_*`, `status`, `input`, `method_id` и т.д. |
| `logs` | `(chunk, block_number, transaction_index, log_index)` | Event-логи: `address`, `data`, `topic_zeroth..third`, `rest_topics` (list), `removed` |
| `internal_transactions` | `(chunk, block_number, transaction_index, trace_index)` | Внутренние вызовы/переводы (из trace): `from_address`, `to_address`, `value` |
| `contracts` | `(chunk, block_number, transaction_index, trace_index)` | Деплои контрактов: `address`, `creator_address`, `creation_bytecode`, `deployed_bytecode`, `contract_factory` |
| `contracts_by_addresses` | `(address)` | Те же деплои, но партиционированы по адресу контракта — для быстрого поиска "когда/кем задеплоен адрес X" (НЕ по `chunk`, нет привязки к диапазону блоков для full-scan) |
| `block_completions` | `(chunk, block_number)` | Служебная: счётчики `tx_count`/`log_count`/`itx_count`/`contract_count` на блок — используется для верификации полноты записи блока |

### Колонки по таблицам

**`blocks`**: `chunk int, number bigint, miner text, timestamp_s bigint, timestamp_ms bigint`

**`transactions`**: `chunk int, block_number bigint, transaction_index int, hash text, from_address text,
to_address text, value varint, gas_limit bigint, gas_price bigint, gas_used bigint,
max_priority_fee_per_gas bigint, max_fee_per_gas bigint, cumulative_gas_used bigint,
effective_gas_price bigint, contract_address text, status tinyint, type tinyint, method_id text,
input text, block_timestamp_s bigint, block_timestamp_ms bigint`

**`logs`**: `chunk int, block_number bigint, transaction_index int, log_index int, address text,
data text, topic_zeroth text, topic_first text, topic_second text, topic_third text,
rest_topics list<text>, transaction_hash text, removed boolean, block_timestamp_s bigint,
block_timestamp_ms bigint`

**`internal_transactions`**: `chunk int, block_number bigint, transaction_index int, trace_index int,
from_address text, to_address text, value varint, transaction_hash text, block_timestamp_s bigint,
block_timestamp_ms bigint`

**`contracts`**: `chunk int, block_number bigint, transaction_index int, trace_index int, address text,
creation_method tinyint, creator_address text, contract_factory text, creation_bytecode text,
deployed_bytecode text, transaction_hash text, block_timestamp_s bigint, block_timestamp_ms bigint`

**`contracts_by_addresses`**: `address text, creator text, tx_hash text, block_number bigint,
timestamp bigint, contract_factory text, creation_bytecode text, deployed_bytecode text`

**`block_completions`**: `chunk int, block_number bigint, tx_count int, log_count int, itx_count int,
contract_count int`

## Обход всей таблицы через token range (без `chunk`-перечисления)

Перебирать `chunk` явным образом (`WHERE chunk = N`) для full-table scan — **неэффективно и
небезопасно для полноты**: значение `chunk` хэшируется в случайный токен (murmur3 от значения
партиционного ключа не связан с арифметикой lane/era), поэтому явный перебор по известным `chunk`
попадает в случайные шарды и из-за birthday paradox не гарантирует покрытие всех физических
диапазонов хранения при небольшом количестве samples.

**Правильный паттерн** — резать кольцо токенов (`-2^63 … 2^63-1`) на N равных диапазонов и читать
параллельно через `token(chunk)`:

```sql
SELECT * FROM transactions WHERE token(chunk) > ? AND token(chunk) <= ?;
```

Пример на Python (`cassandra-driver`), параллельное чтение в N диапазонов:

```python
import threading
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

auth = PlainTextAuthProvider(username="reader", password="<PASSWORD>")
cluster = Cluster(["100.64.0.64"], port=9042, auth_provider=auth)
session = cluster.connect("pol")

MIN_TOKEN = -(2**63)
MAX_TOKEN = 2**63 - 1
RANGES = 256          # больше диапазонов = более мелкие, параллелизуемые чтения
TABLE = "transactions"

def scan_range(lo, hi, results):
    rows = session.execute(
        f"SELECT * FROM {TABLE} WHERE token(chunk) > %s AND token(chunk) <= %s",
        (lo, hi)
    )
    count = 0
    for row in rows:
        count += 1
        # обработать row...
    results.append(count)

step = (MAX_TOKEN - MIN_TOKEN) // RANGES
boundaries = [MIN_TOKEN + i * step for i in range(RANGES + 1)]
boundaries[-1] = MAX_TOKEN  # подровнять край

results = []
threads = []
for i in range(RANGES):
    t = threading.Thread(target=scan_range, args=(boundaries[i], boundaries[i+1], results))
    threads.append(t)
    t.start()
for t in threads:
    t.join()

print("total rows:", sum(results))
cluster.shutdown()
```

Замечания:
- `RANGES` подбирается под желаемую конкурентность (256 — разумный дефолт; не делать слишком
  крупными — единичные огромные партиции/диапазоны таймаутят на больших таблицах типа `logs`/
  `internal_transactions`).
- Если нужен скан только за конкретный диапазон **блоков** (а не вся таблица), это **отдельная
  задача** — `chunk` не отображается на диапазон блоков напрямую (один `chunk` = блоки одного `lane`
  в пределах одной `era`, разбросанные по случайным физическим токенам). Для диапазона блоков
  эффективнее точечно вычислить набор нужных `(chunk, block_number)` пар по формуле выше и делать
  point/range-запросы `WHERE chunk = ? AND block_number >= ? AND block_number < ?` для каждого
  затронутого `chunk`, а не token-range scan всей таблицы.
- Ретраи с backoff на таймаутах обязательны при больших token-range сканах — это штатная ситуация
  на таблицах такого объёма, не признак сбоя (см. `tools/find_missing_blocks.py` и
  `tools/localize_gap2.py` в репозитории — готовые примеры с retry+backoff на этом же паттерне).
