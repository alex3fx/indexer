# Bytecode Verification API

HTTP JSON API для интеграции watcher с ETH-индексером.
Предоставляет поиск контрактов по адресу/байткоду, клон-поиск и верификацию.

**Сервер:** `100.64.0.4:8080`  
**Источник:** `tools/bytecode_api/main.go`  
**Запуск на сервере:** tmux-сессия `bytecode_api`, скрипт `~/run_bytecode_api.sh` (auto-restart).

---

## Авторизация

Все endpoints (кроме `/health`) требуют заголовок:

```
Api-Access-Key: 354bf5a9-a29a-4879-9f3d-d3c09c6a610a
```

Без заголовка или с неверным значением — `401 Unauthorized`.

```bash
curl -H "Api-Access-Key: 354bf5a9-a29a-4879-9f3d-d3c09c6a610a" \
     "http://100.64.0.4:8080/contract?address=0x..."
```

---

## Общие правила

### chain_id

Все endpoints принимают параметр `chain_id` в query string.

| Значение | Результат |
|---|---|
| `1` или отсутствует | OK (ETH mainnet) |
| любое другое | HTTP 501 `{"error":"chain_id=X is not implemented yet"}` |

### Формат ответа

Все ответы — JSON. Ошибки:

```json
{"error": "описание ошибки"}
```

HTTP-коды ошибок: `400` (невалидный запрос), `401` (неверный ключ), `404` (не найдено),
`501` (chain_id не поддержан), `500` (ошибка БД).

### Адреса

Адреса принимаются в любом регистре (`0xAbCd...` или `0xabcd...`) — нормализуются к lowercase внутри.

### Timestamps

Все timestamp-поля (`block_timestamp`, `verified_at`) возвращаются как **целое число в миллисекундах**
(Unix timestamp в ms). Например: `1704067200000` = `2024-01-01T00:00:00Z`.

---

## Два вида байткода

Каждый контракт имеет **два** разных байткода:

| | Deployed bytecode | Creation bytecode |
|---|---|---|
| Другое название | Runtime code | Init code |
| Что содержит | Код, который выполняется при вызовах | Конструктор + аргументы + сам deployed bytecode |
| Откуда берётся | `eth_getCode(address)` | Поле `data` в транзакции деплоя |
| Хранится в `bytecode_store_v2` | ✅ `kind=0` | ✅ `kind=1` |
| Обратный индекс (поиск → адреса) | ✅ `addresses_by_bytecode` | ✅ `addresses_by_creation_bytecode` |

---

## Endpoints

### `GET /health`

```
GET /health
→ 200 ok
```

Авторизация не требуется.

---

### `GET /contract?address=0x{addr}`

Полная информация о контракте в ETHSCAN-совместимом формате.

**Запрос:**

```
GET /contract?address=0x219e497a09202a3534f653e63faaeab6689c1d22
```

**Ответ (200):**

```json
{
  "address":              "0x219e497a09202a3534f653e63faaeab6689c1d22",
  "block_number":         14000001,
  "block_timestamp":      1641034800000,
  "tx_hash":              "0xabc123...",
  "contract_creator":     "0xdeadbeef...",
  "contract_factory":     null,
  "verified":             true,
  "verified_at":          1688400000000,
  "programming_language": "solidity",
  "abi":                  [{"name":"transfer","type":"function",...}],
  "deployed_bytecode":    "0x6080604052...",
  "creation_bytecode":    "0x6080604052...",
  "source":               "pragma solidity ^0.8.0;\n..."
}
```

| Поле | Тип | Описание |
|---|---|---|
| `address` | string | Адрес контракта |
| `block_number` | int | Блок первого деплоя |
| `block_timestamp` | int (ms) | Unix timestamp блока в миллисекундах |
| `tx_hash` | string | Хеш транзакции деплоя |
| `contract_creator` | string | Адрес, инициировавший деплой (EOA или вызывающий контракт) |
| `contract_factory` | string\|null | Фабричный контракт (CREATE2); `null` при обычном деплое |
| `verified` | bool | Верифицирован ли bytecode |
| `verified_at` | int (ms)? | Timestamp верификации (только если `verified: true`) |
| `programming_language` | string? | Язык программирования (только если верифицирован) |
| `abi` | array? | ABI (только если верифицирован) |
| `deployed_bytecode` | string? | Hex deployed bytecode (0x-prefix); может отсутствовать для пустого кода |
| `creation_bytecode` | string? | Hex creation bytecode; `null` для контрактов до v18 индексера |
| `source` | string? | Текст исходного кода (только если верифицирован) |

**Ответ (404):** контракт не найден в индексе.

---

### `/same` — поиск клонов

```
GET  /same?address=0x{addr}[&limit=N&offset=N]
GET  /same?deployed_bytecode=0x{hex}[&limit=N&offset=N]
GET  /same?creation_bytecode=0x{hex}[&limit=N&offset=N]
POST /same
```

Возвращает все адреса с тем же **deployed bytecode** (по умолчанию) или **creation bytecode**.
При поиске по deployed_bytecode фильтрует CREATE2+selfdestruct редеплои: только адреса с **текущим** совпадающим кодом.

> **Для watcher: используйте `POST /same` при поиске по bytecode.**
> GET ограничен длиной URL (deployed bytecode до EIP-170 проходит, но POST надёжнее).
> POST принимает тело до 4 MB.

**Параметры пагинации:**

| Параметр | Описание |
|---|---|
| `limit` | Максимальное количество адресов в ответе. `0` (по умолчанию) = без ограничений |
| `offset` | Смещение от начала отсортированного списка. `0` по умолчанию |

Адреса возвращаются в **отсортированном** порядке (стабильная пагинация).

**POST-тело** (три варианта — передавать ровно один из них):

```json
{"address": "0x219e497a09202a3534f653e63faaeab6689c1d22"}
```

```json
{"deployed_bytecode": "0x6080604052...", "limit": 100, "offset": 0}
```

```json
{"creation_bytecode": "0x..."}
```

**Ответ для deployed_bytecode (200):**

```json
{
  "deployed_bytecode_hash": "0x38f9c201...",
  "deployed_bytecode_seq":  0,
  "total":   142,
  "count":   100,
  "offset":  0,
  "limit":   100,
  "addresses": [
    "0x219e497a09202a3534f653e63faaeab6689c1d22",
    "0xabc123...",
    ...
  ]
}
```

**Ответ для creation_bytecode (200):**

```json
{
  "creation_bytecode_hash": "0x7aef1200...",
  "creation_bytecode_seq":  0,
  "total":   15,
  "count":   15,
  "offset":  0,
  "limit":   0,
  "addresses": [...]
}
```

- `total` — суммарное количество адресов (до пагинации)
- `count` — количество адресов в текущей странице
- `limit: 0` означает нет ограничения (все результаты)

---

### `POST /verify`

Верификация контракта: привязывает ABI и исходный код к deployed bytecode hash.
Верификация распространяется на **все клоны** — достаточно верифицировать один адрес из группы.

**ABI и source обязательны вместе — одно без другого не принимается.**

**Тело запроса:**

```json
{
  "address":              "0x219e497a09202a3534f653e63faaeab6689c1d22",
  "abi":                  "[{\"name\":\"transfer\",\"type\":\"function\",...}]",
  "source":               "pragma solidity ^0.8.0;\n\ncontract Token {...}",
  "programming_language": "solidity"
}
```

| Поле | Тип | Обязательность | Описание |
|---|---|---|---|
| `address` | string | да | Адрес контракта |
| `abi` | string | да (если есть source) | ABI как JSON-строка (массив) |
| `source` | string | да (если есть abi) | Текст исходного кода (Solidity, Vyper и т.п.) |
| `programming_language` | string | нет | Язык: `"solidity"`, `"vyper"`, `"yul"` и т.п. |

Правило: **abi и source либо оба, либо ни одного.** Иначе 400.

**Ответ `status: "verified"` (200)** — верификация прошла:

```json
{
  "status":                 "verified",
  "deployed_bytecode_hash": "0x38f9c201...",
  "deployed_bytecode_seq":  0,
  "verified_at":            1688400000000,
  "address_count":          142,
  "addresses":              ["0x219e...", ...]
}
```

**Ответ `status: "already_verified"` (200)** — bytecode уже верифицирован:

```json
{
  "status":                 "already_verified",
  "deployed_bytecode_hash": "0x38f9c201...",
  "deployed_bytecode_seq":  0,
  "address_count":          142,
  "addresses":              ["0x219e...", ...]
}
```

**Ответ `status: "pending"` (200)** — адрес ещё не проиндексирован; верификация сохранена и применится автоматически:

```json
{
  "status":  "pending",
  "address": "0x219e497a09202a3534f653e63faaeab6689c1d22"
}
```

---

## Механика pending-верификации

Если `/verify` вызван до того, как контракт попал в индекс:

1. API сохраняет `{address, abi, source, programming_language}` в таблицу `pending_verifications`.
2. Когда индексер (Zig, realtime-режим) обрабатывает блок с деплоем этого контракта — автоматически применяет верификацию.
3. После этого `/contract?address=...` вернёт `verified: true`.

Watcher может периодически перепроверять `/contract` для адресов со статусом `pending`.

---

## Примеры (curl)

```bash
BASE="http://100.64.0.4:8080"
KEY="Api-Access-Key: 354bf5a9-a29a-4879-9f3d-d3c09c6a610a"

# Проверить доступность (без ключа)
curl "$BASE/health"

# Информация о контракте
curl -H "$KEY" "$BASE/contract?address=0x219e497a09202a3534f653e63faaeab6689c1d22"

# Найти все клоны по адресу (пагинация: первые 50)
curl -H "$KEY" "$BASE/same?address=0x219e...&limit=50&offset=0"

# Найти клоны по deployed bytecode (POST — надёжнее для длинного hex)
curl -H "$KEY" -H "Content-Type: application/json" \
  -X POST "$BASE/same" \
  -d '{"deployed_bytecode": "0x6080604052...", "limit": 100}'

# Найти клоны по creation bytecode
curl -H "$KEY" "$BASE/same?creation_bytecode=0x6080..."

# Верифицировать контракт
curl -H "$KEY" -H "Content-Type: application/json" \
  -X POST "$BASE/verify" \
  -d '{
    "address": "0x219e497a09202a3534f653e63faaeab6689c1d22",
    "abi": "[{\"name\":\"transfer\",\"type\":\"function\",\"inputs\":[{\"name\":\"to\",\"type\":\"address\"},{\"name\":\"amount\",\"type\":\"uint256\"}],\"outputs\":[{\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"nonpayable\"}]",
    "source": "pragma solidity ^0.8.0;\n\ncontract ERC20 { ... }",
    "programming_language": "solidity"
  }'
```

---

## Ограничения и поведение

- **Только ETH mainnet** (`chain_id=1`). Другие сети — 501.
- **ABI хранится zlib-сжатым** в Scylla; API возвращает уже распакованный JSON.
- **Source** принимается и возвращается как обычная строка (текст исходного кода).
- **`creation_bytecode` в `/contract`** — заполнен для всех контрактов (0 → HEAD).
  Исторические данные восстановлены из архивной таблицы `contracts_by_addresses` инструментом
  `backfill_creation_from_snap` (2026-07-06). Для ~0.03% контрактов, не попавших в архив
  (CREATE2 re-deploys), значение может быть `null`.
- **`/same` для популярных bytecode** (ERC-20 factory и т.п.) может вернуть тысячи адресов.
  Используйте пагинацию (`limit` / `offset`). `total` всегда показывает полное число.
- **`/bytecode`** — удалён в v4. Использовать `/contract`.
