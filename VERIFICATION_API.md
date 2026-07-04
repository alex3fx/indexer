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
`501` (chain_id или функция не поддержана), `500` (ошибка БД).

### Адреса

Адреса принимаются в любом регистре (`0xAbCd...` или `0xabcd...`) — нормализуются к lowercase внутри.

---

## Два вида байткода

Каждый контракт имеет **два** разных байткода:

| | Deployed bytecode | Creation bytecode |
|---|---|---|
| Другое название | Runtime code | Init code |
| Что содержит | Код, который выполняется при вызовах | Конструктор + аргументы + сам deployed bytecode |
| Откуда берётся | `eth_getCode(address)` | Поле `data` в транзакции деплоя |
| Хранится в `bytecode_store_v2` | ✅ `kind=0` | ✅ `kind=1` |
| Обратный индекс (поиск → адреса) | ✅ `addresses_by_bytecode` | ❌ нет (требует отдельную таблицу) |

**Следствие:** `/same` умеет искать только по **deployed_bytecode**. Поиск по `creation_bytecode`
возвращает `501` — такой индекс ещё не построен.

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

Информация о конкретном контракте: блок деплоя, оба bytecode identity, размер, статус верификации, ABI.

**Запрос:**

```
GET /contract?address=0x219e497a09202a3534f653e63faaeab6689c1d22
```

**Ответ (200):**

```json
{
  "address":                "0x219e497a09202a3534f653e63faaeab6689c1d22",
  "block_number":           14000001,
  "tx_hash":                "0xabc123...",
  "deployer":               "0xdeadbeef...",
  "deployed_bytecode_hash": "0x38f9c201...",
  "deployed_bytecode_seq":  0,
  "creation_bytecode_hash": "0x7aef1200...",
  "creation_bytecode_seq":  0,
  "size":                   2048,
  "verified":               true,
  "verified_at":            "2026-07-02T19:06:07Z",
  "abi":                    [{"name":"transfer","type":"function",...}],
  "source_ref":             "source_store"
}
```

Поля `verified_at`, `abi`, `source_ref` присутствуют только если `verified: true`.

**Ответ (404):** контракт не найден в индексе.

---

### `/same` — поиск клонов по одинаковому deployed bytecode

```
GET  /same?address=0x{addr}
GET  /same?deployed_bytecode=0x{hex}
GET  /same?creation_bytecode=0x{hex}   → 501 (нет индекса)
POST /same
```

Возвращает все адреса с тем же **deployed bytecode** (runtime code).
Фильтрует CREATE2+selfdestruct редеплои: только адреса с **текущим** совпадающим кодом.

> **Для watcher: используйте `POST /same` при поиске по bytecode.**
> GET ограничен длиной URL (deployed bytecode до EIP-170 проходит, но POST надёжнее).
> POST принимает тело до 4 MB.

**POST-тело** (три варианта — передавать ровно один из них):

```json
{"address": "0x219e497a09202a3534f653e63faaeab6689c1d22"}
```

```json
{"deployed_bytecode": "0x6080604052..."}
```

```json
{"creation_bytecode": "0x..."}
```

**Ответ (200):**

```json
{
  "deployed_bytecode_hash": "0x38f9c201...",
  "deployed_bytecode_seq":  0,
  "count":                  142,
  "addresses": [
    "0x219e497a09202a3534f653e63faaeab6689c1d22",
    "0xabc123...",
    ...
  ]
}
```

**Ответ (501) при `creation_bytecode`:**

```json
{
  "error": "creation_bytecode search is not yet implemented: there is no reverse index for creation bytecode. Use deployed_bytecode= to search by runtime code (what eth_getCode returns), or address= to look up a specific contract."
}
```

---

### `POST /verify`

Верификация контракта: привязывает ABI и исходный код к deployed bytecode hash.
Верификация распространяется на **все клоны** — достаточно верифицировать один адрес из группы.

**ABI и source обязательны вместе — одно без другого не принимается.**

**Тело запроса:**

```json
{
  "address": "0x219e497a09202a3534f653e63faaeab6689c1d22",
  "abi":     "[{\"name\":\"transfer\",\"type\":\"function\",...}]",
  "source":  "0x504b0304..."
}
```

| Поле | Тип | Обязательность | Описание |
|---|---|---|---|
| `address` | string | да | Адрес контракта |
| `abi` | string | да (если есть source) | ABI как JSON-строка (массив) |
| `source` | string | да (если есть abi) | Исходник как hex-строка байт (zip/tar.gz архив) |

Правило: **abi и source либо оба, либо ни одного.** Иначе 400.

**Ответы:**

`status: "verified"` (200) — верификация прошла:

```json
{
  "status":                 "verified",
  "deployed_bytecode_hash": "0x38f9c201...",
  "deployed_bytecode_seq":  0,
  "verified_at":            "2026-07-04T10:00:00Z",
  "address_count":          142,
  "addresses":              ["0x219e...", ...]
}
```

`status: "already_verified"` (200) — bytecode уже верифицирован:

```json
{
  "status":                 "already_verified",
  "deployed_bytecode_hash": "0x38f9c201...",
  "deployed_bytecode_seq":  0,
  "address_count":          142,
  "addresses":              ["0x219e...", ...]
}
```

`status: "pending"` (200) — адрес ещё не проиндексирован; верификация сохранена и применится автоматически:

```json
{
  "status":  "pending",
  "address": "0x219e497a09202a3534f653e63faaeab6689c1d22"
}
```

---

## Механика pending-верификации

Если `/verify` вызван до того, как контракт попал в индекс:

1. API сохраняет `{address, abi, source}` в таблицу `pending_verifications`.
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

# Найти все клоны по адресу
curl -H "$KEY" "$BASE/same?address=0x219e497a09202a3534f653e63faaeab6689c1d22"

# Найти все клоны по deployed bytecode (POST — надёжнее для длинного hex)
curl -H "$KEY" -H "Content-Type: application/json" \
  -X POST "$BASE/same" \
  -d '{"deployed_bytecode": "0x6080604052..."}'

# Верифицировать контракт (abi + source обязательны вместе)
curl -H "$KEY" -H "Content-Type: application/json" \
  -X POST "$BASE/verify" \
  -d '{
    "address": "0x219e497a09202a3534f653e63faaeab6689c1d22",
    "abi": "[{\"name\":\"transfer\",\"type\":\"function\",\"inputs\":[{\"name\":\"to\",\"type\":\"address\"},{\"name\":\"amount\",\"type\":\"uint256\"}],\"outputs\":[{\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"nonpayable\"}]",
    "source": "0x504b0304..."
  }'

# Неверный ключ → 401
curl "$BASE/contract?address=0x..." 
# {"error":"missing or invalid Api-Access-Key header"}

# creation_bytecode → 501 (нет индекса)
curl -H "$KEY" "$BASE/same?creation_bytecode=0x..."
```

---

## Ограничения

- **Только ETH mainnet** (`chain_id=1`). Другие сети — 501.
- **`/same?creation_bytecode`** — 501, нет обратного индекса.
- **`/same` может вернуть тысячи адресов** для популярного bytecode (ERC-20 factory и т.п.).
- **ABI хранится zlib-сжатым** в Scylla; API возвращает уже распакованный JSON.
- **Source** — произвольные байты в hex (zip, tar.gz и т.п.), формат не валидируется.
- **`/bytecode`** — удалён в v4. Использовать `/contract`.
