# CONSISTENCY.md — Механизм консистентности данных индексера

## Проблема

Индексер пишет данные одного блока в **6 таблиц параллельно**. Если процесс упал в середине
записи (crash, OOM, network timeout), в базе остаются частично записанные блоки: транзакции
есть, а internal_transactions — нет. Читатель не может отличить «блок в процессе записи» от
«блок полностью проиндексирован».

Нужен способ ответить на вопрос: *для данного диапазона блоков — какие из них полностью
записаны, каких нет?*

---

## Архитектура: 7 таблиц вместо 6

```
blocks                  ← данные
transactions            ← данные
logs                    ← данные
internal_transactions   ← данные
contracts               ← данные
contracts_by_addresses  ← данные

block_completions       ← маркер завершённости (пишется последним)
```

Ключевой инвариант:

> **Наличие строки в `block_completions` означает, что все 6 data-таблиц для этого блока
> записаны успешно.**

---

## Схема block_completions

```sql
CREATE TABLE block_completions (
  chunk          int,
  block_number   bigint,
  tx_count       int,
  log_count      int,
  itx_count      int,
  contract_count int,
  PRIMARY KEY ((chunk), block_number)
) WITH CLUSTERING ORDER BY (block_number ASC);
```

- **Partition key**: `chunk` — тот же, что и в data-таблицах (`block_number / 1000` или
  `block_number % REMAP_MOD`). Обеспечивает co-location с данными на одном шарде Scylla.
- **Clustering key**: `block_number` — позволяет делать range scan внутри чанка.
- **Счётчики** (`tx_count`, `log_count`, `itx_count`, `contract_count`) — для будущей
  детальной верификации (сейчас используются только в режиме VERIFY).

---

## Протокол записи (db.zig)

### Порядок записи в saveBatch / WorkerPool.saveBatch

```
┌─────────────────────────────────────────────────────────┐
│  Параллельно (pool потоков или persistent workers):      │
│                                                          │
│  blocks worker[0]          txs worker[0..3]              │
│  logs worker[0..5]         itxs worker[0..11]            │
│  contracts worker[0..2]    cba worker[0..1]              │
│                                                          │
│  Все 6 таблиц пишутся одновременно.                     │
└────────────────────────┬────────────────────────────────┘
                         │ join / barrier (pending==0)
                         ▼
              if had_error → return error.SaveFailed
                         │
                         ▼
         writeBlockCompletions(pool, ent)   ← ПОСЛЕДНИМ
```

`writeBlockCompletions` вызывается **только если все 6 таблиц записаны без ошибок**.
Если хотя бы один воркер поймал CQL ошибку (`had_error=true`), функция не вызывается
и маркер не пишется.

### writeBlockCompletions

Использует тот же механизм UNLOGGED BATCH, что и data-таблицы:

```zig
const BS_COMP: usize = 50;  // строк на батч (фиксированный, не настраивается)
```

Для каждого блока в батче подсчитываются строки из всех entities (линейный проход
по уже отсортированным по block_number данным) и пишется одна строка с counts.

---

## Режим верификации (VERIFY=1)

Запуск:

```bash
VERIFY=1 FROM_BLOCK=0 TO_BLOCK=21000000 \
  SCYLLA_DB_CONTACT_POINTS='["127.0.0.1:9042"]' \
  SCYLLA_DB_KEYSPACE=eth \
  SCYLLA_DB_CREDENTIALS='{"username":"cassandra","password":"cassandra"}' \
  ./indexer
```

Exit code: `0` = все блоки присутствуют, `1` = найдены пропуски.

### Двухуровневая стратегия

Наивный подход — `SELECT block_number FROM block_completions WHERE chunk=N` — не работает
для больших диапазонов: один чанк может содержать до 656 000 строк (21M блоков / 32 чанка),
CQL вернёт только первые ~5000 без явного paging.

Решение: **два уровня проверки**:

```
для каждого chunk в диапазоне from..to:
    ┌─────────────────────────────────────────────────┐
    │ Level 1: COUNT (O(1))                           │
    │                                                 │
    │ SELECT count(*) FROM block_completions          │
    │   WHERE chunk=N AND block_number>=from          │
    │   AND block_number<=to                          │
    │                                                 │
    │ count == expected?  → ✅ чанк полный, пропустить │
    │ count != expected?  → ❌ идём на Level 2         │
    └──────────────────────┬──────────────────────────┘
                           │ только для проблемных чанков
                           ▼
    ┌─────────────────────────────────────────────────┐
    │ Level 2: PAGED SCAN (находит конкретные пробелы) │
    │                                                 │
    │ SELECT block_number FROM block_completions      │
    │   WHERE chunk=N AND block_number>=from          │
    │   AND block_number<=to                          │
    │ PAGE_SIZE = 5000, loop с paging_state           │
    │                                                 │
    │ Сравниваем полученные block_number с ожидаемыми │
    │ → выводим конкретные missing блоки              │
    └─────────────────────────────────────────────────┘
```

### CQL paging

Scylla обрезает ответы по умолчанию (~5000 строк). Для полного сканирования
используется нативный CQL paging:

- Флаг `0x04` в QUERY frame → `PAGE_SIZE=5000`
- Флаг `0x08` → передача `paging_state` из предыдущего ответа
- Бит `0x0002` в RESULT frame flags → `HAS_MORE_PAGES`

Пока `HAS_MORE_PAGES == true`, цикл повторяет запрос с полученным `paging_state`.

### Ожидаемое количество блоков

**Режим linear** (`REMAP_MOD=0`, `chunk = block_number / chunk_size`):

```
expected = min(chunk_to, to) - max(chunk_from, from) + 1
```

**Режим remap** (`chunk = block_number % REMAP_MOD`):

```
first_block_in_chunk = from + ((chunk - from % remap_mod) + remap_mod) % remap_mod
expected = (to - first_block_in_chunk) / remap_mod + 1
```

### Вывод

```
Verifying 21000001 blocks (0..21000000)  mode=linear

[GAP] chunk 25079: expected 1000, found 997
  [MISSING] block 25079100
  [MISSING] block 25079543
  [MISSING] block 25079891

✅ OK — all 21000001 blocks indexed
```

или при наличии пропусков:

```
❌ FAIL — 3/21000001 blocks missing from block_completions
```

(максимум 30 missing блоков выводится явно, остальные суммируются)

---

## Обработка ошибок в индексере

### Ошибка записи данных

Если CQL воркер получил ошибку при записи любой из 6 data-таблиц:

```
had_error.store(true)
         ↓
join/barrier
         ↓
if had_error → return error.SaveFailed  (block_completions НЕ пишется)
         ↓
realtime.zig: processBlock вернёт error.SaveFailed
         ↓
runCatchupAndRealtime: остановится, залогирует ошибку
```

Блок без маркера `block_completions` будет обнаружен при следующей верификации.

### Ошибка записи block_completions

Если `writeBlockCompletions` сама упала (CQL timeout, disconnect):

```
return error.SaveFailed  →  блок будет переиндексирован
```

Данные в 6 таблицах **идемпотентны** — повторная запись тех же строк безвредна
(INSERT без IF NOT EXISTS перезапишет идентичные данные).

### Redis cursor

Курсор (`LATEST_PROCESSED_BLOCK_NUMBER`) обновляется **после успешного** `saveBatch`,
то есть после записи `block_completions`. Если парсер упал — курсор не сдвинулся,
следующий запуск переиндексирует блок с нуля.

---

## Текущие ограничения и что нужно доделать

### 1. Верификация не проверяет data-таблицы

Сейчас `VERIFY=1` проверяет только `block_completions`. Если строка есть в
`block_completions`, но, например, часть internal_transactions отсутствует — это
не будет обнаружено.

**Почему так:** `block_completions` пишется только при `had_error==false`, а
`had_error` устанавливается при любой CQL-ошибке воркера. Значит, если данные записались
успешно — маркер отражает реальность.

**Что может пойти не так:** silent data corruption на стороне Scylla (крайне редко),
или баги в encode-логике (строка закодирована неправильно, но CQL не вернул ошибку).

**Нужно сделать:** добавить опциональный Level 3 — `SELECT count(*) FROM transactions
WHERE chunk=N` и сравнение с `block_completions.tx_count`.

### 2. Нет автоматической переиндексации при обнаружении gaps

Сейчас `VERIFY=1` только **сообщает** о пропусках. Нужен режим `REINDEX=1`, который
забирает список missing блоков и пускает их через pipeline заново.

### 3. block_completions не используется при catchup

При `runCatchupAndRealtime` парсер читает курсор из Redis (`LATEST_PROCESSED_BLOCK_NUMBER`)
и начинает с `cursor+1`. Если часть блоков до курсора была потеряна — они не будут
переиндексированы автоматически.

**Правильное поведение:** перед стартом catchup запускать verify на диапазоне
`FROM_BLOCK..cursor`, и если есть gaps — добавить их в очередь для переиндексации.

### 4. Нет TTL / retention

`block_completions` растёт вместе с данными. При 21M блоков = 21M строк.
В текущей схеме нет механизма очистки при удалении исторических данных.

---

## Сводка

| Компонент | Файл | Назначение |
|-----------|------|-----------|
| `block_completions` table | `schema.cql` | Маркер «блок полностью записан» |
| `writeBlockCompletions` | `db.zig:1166` | Запись маркеров, вызов после 6 таблиц |
| `saveBatch` / `WorkerPool.saveBatch` | `db.zig:1249 / 1361` | Протокол записи с барьером |
| `runVerify` | `verify.zig:286` | Точка входа в режим верификации |
| `countChunk` | `verify.zig:199` | Level 1: SELECT count(*) |
| `findGapsForChunk` | `verify.zig:213` | Level 2: paged SELECT block_number |
| `VERIFY=1` env | `main.zig:52` | Активация режима верификации |
