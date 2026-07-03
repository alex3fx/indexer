# Bytecode Verification API

HTTP JSON API для интеграции watcher с ETH-индексером.
Предоставляет поиск контрактов по адресу/байткоду, клон-поиск и верификацию.

**Сервер:** `100.64.0.4:8080`  
**Источник:** `tools/bytecode_api/main.go`  
**Запуск на сервере:** tmux-сессия `bytecode_api`, скрипт `~/run_bytecode_api.sh` (auto-restart).

---

## Общие правила

### chain_id

Все endpoints принимают параметр `chain_id` в query string.

| Значение | Результат |
|---|---|
| `1` или отсутствует | OK (ETH mainnet) |
| любое другое | HTTP 501 `{"error":"chain_id=X is not implemented yet"}` |

```
GET /contract?address=0x...&chain_id=1
POST /verify?chain_id=1
```

### Формат ответа

Все ответы — JSON. Ошибки:

```json
{"error": "описание ошибки"}
```

HTTP-коды ошибок: `400` (невалидный запрос), `404` (не найдено), `501` (chain_id не поддержан), `500` (ошибка БД).

### Адреса

Адреса принимаются в любом регистре (`0xAbCd...` или `0xabcd...`) — нормализуются к lowercase внутри.

---

## Endpoints

### `GET /health`

Проверка доступности сервиса.

```
GET /health
→ 200 ok
```

---

### `GET /contract?address=0x{addr}&chain_id=1`

Информация о конкретном контракте: блок деплоя, bytecode identity, размер, статус верификации, ABI.

**Запрос:**

```
GET /contract?address=0x219e497a09202a3534f653e63faaeab6689c1d22&chain_id=1
```

**Ответ (200):**

```json
{
  "address":       "0x219e497a09202a3534f653e63faaeab6689c1d22",
  "block_number":  14000001,
  "tx_hash":       "0xabc123...",
  "deployer":      "0xdeadbeef...",
  "bytecode_hash": "0x38f9c201...",
  "bytecode_seq":  0,
  "size":          2048,
  "kind":          "deployed",
  "verified":      true,
  "verified_at":   "2026-07-02T19:06:07Z",
  "abi":           [{"name":"transfer","type":"function",...}],
  "source_ref":    "source_store"
}
```

Поля `verified_at`, `abi`, `source_ref` присутствуют только если `verified: true` и данные есть.

`kind`: `"deployed"` — runtime bytecode; `"creation"` — initcode (редко).

**Ответ (404):** контракт не найден в индексе (ещё не доиндексирован или не существует).

---

### `GET /same?address=0x{addr}&chain_id=1`
### `GET /same?bytecode=0x{hex}&chain_id=1`
### `POST /same?chain_id=1`

Возвращает все адреса с тем же deployed bytecode (клоны/прокси с одинаковым кодом).

Фильтрует CREATE2+selfdestruct редеплои: возвращает только адреса, у которых **текущий** байткод совпадает.

**POST-тело** (альтернатива GET-параметрам):

```json
{"address": "0x219e497a09202a3534f653e63faaeab6689c1d22"}
```

или

```json
{"bytecode": "0x6080604052..."}
```

**Ответ (200):**

```json
{
  "bytecode_hash": "0x38f9c201...",
  "bytecode_seq":  0,
  "count":         142,
  "addresses":     [
    "0x219e497a09202a3534f653e63faaeab6689c1d22",
    "0xabc123...",
    ...
  ]
}
```

`bytecode_seq` — всегда `0`, кроме теоретических SHA256-коллизий (вероятность ~0).

---

### `POST /verify?chain_id=1`

Верификация контракта: привязывает ABI (и опционально исходный код) к bytecode hash.
Верификация распространяется на **все клоны** — достаточно верифицировать один адрес из группы.

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
| `address` | string | да | Адрес контракта (любой регистр) |
| `abi` | string | да | ABI как JSON-строка (массив) |
| `source` | string | нет | Исходник как hex-строка байт архива (zip/tar) |

**Ответы:**

**`status: "verified"` (200)** — верификация прошла успешно:

```json
{
  "status":        "verified",
  "bytecode_hash": "0x38f9c201...",
  "bytecode_seq":  0,
  "verified_at":   "2026-07-02T19:06:07Z",
  "address_count": 142,
  "addresses":     ["0x219e...", "0xabc123...", ...]
}
```

**`status: "already_verified"` (200)** — bytecode уже верифицирован ранее:

```json
{
  "status":        "already_verified",
  "bytecode_hash": "0x38f9c201...",
  "bytecode_seq":  0,
  "address_count": 142,
  "addresses":     ["0x219e...", ...]
}
```

**`status: "pending"` (200)** — адрес ещё не проиндексирован; верификация сохранена и будет применена автоматически, когда индексер обработает блок с этим контрактом:

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
2. Когда индексер (Zig, realtime-режим) обрабатывает блок с деплоем этого контракта — автоматически применяет верификацию: обновляет `bytecode_store_v2`, удаляет pending-запись.
3. После этого `/contract?address=...` вернёт `verified: true`.

Watcher может периодически перепроверять `/contract` для адресов со статусом `pending`.

---

## Примеры (curl)

```bash
BASE="http://100.64.0.4:8080"

# Проверить доступность
curl "$BASE/health"

# Информация о контракте
curl "$BASE/contract?address=0x219e497a09202a3534f653e63faaeab6689c1d22&chain_id=1"

# Найти все клоны по адресу
curl "$BASE/same?address=0x219e497a09202a3534f653e63faaeab6689c1d22&chain_id=1"

# Найти все клоны по bytecode
curl "$BASE/same?bytecode=0x6080604052...&chain_id=1"

# Верифицировать контракт
curl -X POST "$BASE/verify?chain_id=1" \
  -H "Content-Type: application/json" \
  -d '{
    "address": "0x219e497a09202a3534f653e63faaeab6689c1d22",
    "abi": "[{\"name\":\"transfer\",\"type\":\"function\",\"inputs\":[{\"name\":\"to\",\"type\":\"address\"},{\"name\":\"amount\",\"type\":\"uint256\"}],\"outputs\":[{\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"nonpayable\"}]",
    "source": ""
  }'

# Неподдерживаемая сеть → 501
curl "$BASE/contract?address=0x219e497a09202a3534f653e63faaeab6689c1d22&chain_id=137"
# {"error":"chain_id=137 is not implemented yet"}
```

---

## Ограничения и поведение

- **Только ETH mainnet** (`chain_id=1`). Другие сети — 501.
- **`/same` без фильтрации может вернуть тысячи адресов** для популярного bytecode (ERC-20 factory и т.п.) — учитывайте при пагинации на стороне watcher.
- **ABI хранится zlib-сжатым** в Scylla; API возвращает уже распакованный JSON.
- **Source** передаётся как hex от произвольных байт (zip, tar.gz и т.п.) — API не валидирует формат.
- **`/bytecode`** — legacy endpoint, читает из старой таблицы `contracts_by_addresses`. Использовать только для обратной совместимости; предпочитайте `/contract`.
