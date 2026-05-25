# Задача: приведение zigparser к production-ready замене TS-парсера

## Цель

Полностью заменить TypeScript-систему (`historical.ts` + `save.ts` × 51) одним бинарником
`zigparser2` без потери данных и изменения схемы ScyllaDB.

---

## Что уже работает

| Функционал | TS | zigparser |
|---|---|---|
| Все 6 типов сущностей (blocks / txs / logs / internal_txs / contracts / contracts_by_addr) | ✓ | ✓ |
| Все 5 типов транзакций (Legacy / 0x1 / 0x2 / 0x3 / 0x4) | ✓ | ✓ |
| milliTimestamp + fallback timestamp×1000 | ✓ | ✓ |
| chunk = floor(block_number / RAW_CHUNK_SIZE) | ✓ | ✓ |
| UNLOGGED BATCH вставка | — | ✓ |
| Параллельные воркеры (PIPELINE) | 51 процессов через PM2 | PIPELINE=N потоки |
| Realtime (newHeads WS + polling) | — | ✓ |
| Метрики + JSON-отчёт | — | ✓ |

---

## Критические баги (могут создавать некорректные данные)

### B1 — Несоответствие типа `value` в CQL

**Где:** `db.zig:892,971,1161,1202` — поля `transactions.value` и `internal_transactions.value`

**Проблема:**
Схема ScyllaDB объявляет `value bigint` (CQL int64, 8 байт big-endian).
Zigparser кодирует `value` через `valVarint` — это CQL тип `varint` (big-endian two's complement,
переменная длина). Для `0x0` → отправляется 1 байт `\x00`, тогда как bigint ожидает 8 байт.

ScyllaDB, получая байты неправильной длины для bigint-параметра в prepared statement, может:
- вернуть ошибку → строка молча дропается (`recvFrameDiscard` не читает ответ)
- интерпретировать 1 байт как неверное значение

Дополнительно: ETH value может превышать int64 (> 9.22 ETH × 10¹⁸ wei). Bigint недостаточен
для хранения крупных переводов.

**Варианты исправления:**
1. Изменить схему `value` с `bigint` на `varint` — сохранит произвольную точность, но потребует
   миграции данных и несовместимо с существующей TS-базой.
2. Оставить `bigint` + заменить `valVarint` на `valBigint` (convert hex → i64). Значения >i64 max
   обрезаются до MaxInt64, что совпадает с поведением cassandra-driver в TS.
3. **(Рекомендуется для drop-in замены)** Вариант 2: `valBigint` + cap to MaxInt64.

**Исправление:**
```zig
// db.zig txsWorker ~строка 892
// было:
valVarint(&v, A, r.value) catch continue;
// стало:
valBigint(&v, A, hexToI64Capped(r.value)) catch continue;
```
Функция `hexToI64Capped` парсит hex-строку в i64, насыщая при переполнении.

---

### B2 — Пропущенные internal_txs с `to_address == null`

**Где:** `transform.zig:292`

**Проблема:**
```zig
if (from_addr.len > 0 and to_addr.len > 0 and trace.action.value != null) {
```
Условие `to_addr.len > 0` фильтрует трейсы типа `create`/`create2` где `to` = null.
TS хранит такие строки с `to_address = null` (ScyllaDB nullable text).

**Исправление:**
```zig
// Убрать условие на to_addr, разрешить пустую строку (будет NULL в Scylla)
if (from_addr.len > 0 and trace.action.value != null) {
```
Одновременно проверить, что `valText` корректно записывает пустую строку как NULL (`-1`),
а не пустую строку (`""`).

---

## Важные функциональные отличия

### F1 — Нет fallback/reserve RPC URL

**TS:** При ошибке хотя бы одного из трёх запросов (`getBlock/getReceipts/getTraces`)
повторяет все три через резервный публичный URL (`publicnode.com`).

**zigparser:** Ошибка = `bd.err = true` → строка пропускается в `transformBlock:189`.
Транзиентные сбои RPC-ноды создают молчаливые дыры в данных.

**Что реализовать:**
Добавить в конфиг `RESERVE_RPC_URL`. При ошибке одного запроса — повторить через него
отдельно (не все три — TS повторяет все три, что неэффективно; можно только упавший).

```zig
// config.zig
reserve_rpc_url: []const u8, // RESERVE_RPC_URL=""
```

Порядок в rpc.zig: если один из 3 запросов вернул ошибку и `reserve_rpc_url.len > 0` →
повторить через reserve_rpc_url. Установить `bd.err = true` только если оба URL не ответили.

---

### F2 — Поведение при отсутствии `LATEST_PROCESSED_BLOCK_NUMBER` в Redis

**TS:** Стартует с блока 0 (genesis).

**zigparser (main.zig:53-56):**
```zig
if (from == 0) {
    std.debug.print("ERROR: LATEST_PROCESSED_BLOCK_NUMBER not set in Redis\n", .{});
    return error.NoStartBlock;
}
```
Аварийно завершается. Первый запуск на пустой БД требует ручной установки ключа.

**Что реализовать:**
Либо стартовать с 0 как TS, либо добавить env var `FROM_BLOCK` (конфигурируемый стартовый блок)
и использовать его как дефолт при отсутствии ключа в Redis.

```zig
// config.zig
from_block: u64, // FROM_BLOCK=0
```

---

### F3 — Отсутствие проверки ошибок от ScyllaDB

**Где:** `db.zig:394` — `recvFrameDiscard(self.fd)`

**Проблема:**
После каждого BATCH запроса ответ от ScyllaDB читается и **выбрасывается** без парсинга.
CQL error frames (opcode=0x00) игнорируются. Любые ошибки вставки (schema mismatch,
overloaded, auth expired) создают дыры без предупреждения.

**Что реализовать (минимум):**
Разобрать ответ: если opcode = ERROR (0x00) — распечатать код ошибки и сообщение.
Можно сделать нефатальным (логировать + счётчик), но не игнорировать.

```zig
fn recvFrameCheck(fd: i32) void {
    var hdr: [9]u8 = undefined;
    _ = std.os.linux.read(fd, &hdr, 9); // best-effort, ignore read error
    if (hdr[4] == 0x00) { // ERROR opcode
        // read and log error code + message
    }
    // drain body
}
```

---

### F4 — Chain ID / выбор keyspace и RPC URL

**TS:** Выбирает RPC URL по `CHAIN_ID` (mainnet / BSC — hardcode tailscale-адресов).
Zigparser: `chain_id` есть в конфиге, но **нигде не используется**.

**Что реализовать:**
Добавить в env: `CHAIN_ID` должен быть обязательным (или хотя бы логировать предупреждение).
Логика выбора RPC по CHAIN_ID не нужна — в zigparser URL явно задаётся через `RPC_URL`,
что правильнее. Но `chain_id` можно использовать для:
- Логирования при старте: `Chain ID: {d}`
- Выбора keyspace по умолчанию (если `SCYLLA_DB_KEYSPACE` не задан)

---

## Операционные требования

### O1 — Совместимость env vars с TS

Zigparser должен читать те же переменные окружения что и TS, или предоставить mapping-таблицу.

| TS env var | zigparser env var | Статус |
|---|---|---|
| `CHAIN_ID` | `CHAIN_ID` | ✓ (не используется) |
| `RAW_CHUNK_SIZE` | `RAW_CHUNK_SIZE` | ✓ |
| `CM_CONNECTION_URL` | `CM_CONNECTION_URL` | ✓ |
| `SCYLLA_DB_CONTACT_POINTS` | `SCYLLA_DB_CONTACT_POINTS` | ✓ |
| `SCYLLA_DB_CREDENTIALS` | `SCYLLA_DB_CREDENTIALS` | ✓ |
| `SCYLLA_DB_KEYSPACE` | `SCYLLA_DB_KEYSPACE` | ✓ |
| `RPC_URL` (кастом) | `RPC_URL` | ✓ |
| — | `RESERVE_RPC_URL` | **Нужно добавить** |
| — | `FROM_BLOCK` | **Нужно добавить** |
| `TO_BLOCK` (hardcode) | `TO_BLOCK` | ✓ (env var лучше) |

---

### O2 — Инициализация БД при первом запуске

TS требует ручного запуска DDL-скриптов. Zigparser не создаёт схему автоматически.

Документировать порядок запуска:
1. Применить все SQL из `sql/scylla_db/`
2. Установить `LATEST_PROCESSED_BLOCK_NUMBER = 0` в Redis (или добавить `FROM_BLOCK=0`)
3. Запустить `zigparser2`

---

### O3 — Мониторинг прогресса

TS логирует `PROGRESS: i/to, LEFT: n, B: n, TXS: n ...` после каждого батча.
Zigparser печатает прогресс только в исторической моде через `metrics.print()` в конце работы.

**Что реализовать:**
Добавить периодический print прогресса в историческом режиме (каждые N батчей или N секунд):
```
[12345678] chunk=12345 txs=2341 logs=5622 itx=123 b/s=1.2k
```

---

## Что НЕ нужно реализовывать

- **BullMQ / очередь задач** — архитектурный выбор, не функциональное требование.
  zigparser обрабатывает fetch+transform+save в одном процессе, что быстрее (нет сериализации JSON).

- **51 PM2 воркер** — заменяется `PIPELINE=N`. Один процесс, N потоков.

- **DragonflyDB** — нужен только Redis (ioredis). zigparser использует Redis напрямую.

- **dotenvx / pass** — это операционный инструмент TS-развёртывания, не часть парсера.

---

## Приоритет работ

| Приоритет | Задача | Риск без исправления |
|---|---|---|
| **P0** | B1: исправить тип `value` (valVarint → valBigint) | Молчаливый дроп строк в Scylla |
| **P0** | B2: разрешить null `to_address` в internal_txs | Потеря ~N% internal_tx записей |
| **P1** | F3: добавить логирование CQL errors | Невозможно диагностировать проблемы записи |
| **P1** | F1: reserve RPC URL | Дыры данных при сбое ноды |
| **P2** | F2: FROM_BLOCK при пустом Redis | Неудобный первый запуск |
| **P2** | O3: прогресс per-батч | Нет видимости статуса |
| **P3** | F4: chain_id в логах | Косметика |

---

## Проверка после реализации

1. Запустить zigparser2 на диапазоне 100 блоков с транзакциями
2. Сравнить строки в Scylla с эталонным TS-прогоном на тех же блоках:
   - `SELECT count(*) FROM internal_transactions WHERE chunk IN (...)` — должно совпадать
   - `SELECT value FROM transactions WHERE ...` — сравнить несколько крупных переводов
3. Специально проверить блоки с contract-creation trace (null `to_address` в trace action)
4. Временно включить логирование CQL errors и убедиться, что errors = 0 для valid блоков
