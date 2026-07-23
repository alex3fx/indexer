# ETH ERC-20 Indexer — Reorg Analysis & Fix Plan

Создан: 2026-07-22. Последнее обновление: 2026-07-22.

Исходники инструментов: `tools/reorg_scanner/`, `tools/forked_block_checker/`, `tools/retro_reorg_scan/`.

---

## Суть проблемы

ETH mainnet — PoS, реорги глубиной 1 слот происходят регулярно (~170 блоков в диапазоне 25422404–25580592 по данным Etherscan). Два сценария попадания реорг-данных в нашу БД:

### Механизм v21 (до v22, блоки 25,422,404–25,525,803)

`saveBlockRt` успешно записывает **orphaned блок** (все txs за индексами 0..N_orphan-1) → Scylla CQL-ошибка (`std::bad_alloc`) → `.fatal` → retry → к этому моменту реорг разрешён, RPC возвращает **canonical блок** → canonical перезаписывает индексы 0..N_canon-1 → `block_completions.tx_count = N_canon`.

Результат: если N_orphan > N_canon → хвост orphaned txs (индексы N_canon..N_orphan-1) остаётся в `eth.transactions`. Статус: **ORPHANED_SUPERSET** (canonical данные целы, лишние строки).

### Механизм v22 (с v22, блоки 25,525,804+)

v22 хранит `block_hash` и детектирует реорг на блоке N+1 (parent hash mismatch). Записывает в `eth.forked_blocks`. **Не переиндексирует блок N.** Результат: в БД лежит orphaned блок полностью. Статус: ORPHANED_SUPERSET, MISSING_CANONICAL или MIXED.

---

## Найденные реорги (2026-07-22, reorg_scanner)

Диапазон: 25,422,404 → 25,587,207 (164,804 блока, 76 секунд, 0 ошибок).

### 10 блоков v21 — ORPHANED_SUPERSET (потерь canonical данных нет)

| block | canonical txs | stored txs | orphan_only |
|-------|--------------|------------|-------------|
| 25437746 | 85  | 177 | 92  |
| 25438292 | 114 | 358 | 244 |
| 25443896 | 170 | 186 | 16  |
| 25453411 | 169 | 185 | 16  |
| 25455458 | 41  | 164 | 123 |
| 25457675 | 187 | 210 | 23  |
| 25459357 | 179 | 549 | 370 |
| 25459563 | 47  | 152 | 105 |
| 25463126 | 172 | 437 | 265 |
| 25464942 | 222 | 327 | 105 |
| **ИТОГО** | **1,386** | **2,745** | **1,359** |

### 7 блоков v22 — DATA INTEGRITY ISSUE (>25,580,592)

| block | status | canonical txs | stored txs | orphan_only | lost |
|-------|--------|--------------|------------|-------------|------|
| 25581374 | MIXED             | 416 | 177 | 28  | 267 |
| 25583383 | MIXED             | 47  | 152 | 119 | 14  |
| 25583534 | MISSING_CANONICAL | 185 | 74  |  0  | 111 |
| 25585170 | MISSING_CANONICAL | 766 | 204 |  0  | 562 |
| 25585201 | MISSING_CANONICAL | 434 | 358 |  0  | 76  |
| 25585231 | MISSING_CANONICAL | 213 | 115 |  0  | 98  |
| 25586892 | MIXED             | 722 | 583 | 29  | 168 |
| **ИТОГО** | | **2,783** | **1,663** | **176** | **1,296** |

Эти 7 блоков — за пределами Etherscan-диапазона 25580592 и относятся к следующей задаче.

---

## Сравнение с Etherscan Forked Blocks (170 блоков в 25422404–25580592)

Запуск `forked_block_checker` на 170 блоках из списка Etherscan:

```
OK (clean):           133 (78%) — реорги произошли, но мы проиндексировали canonical
ORPHANED_SUPERSET:     37 (22%) — canonical данные есть + 7,347 orphaned txs сверху
MISSING_CANONICAL:      0
MIXED:                  0
```

Из 37 наш `reorg_scanner` методом tx-diff нашёл 10 (25422404–25580592) — потому что 27 блоков имеют orphan_only, но метод tx-сравнения обнаруживает только те, где orphan count влияет на хэш-сравнение. Все 37 были проверены forked_block_checker вручную и тоже ORPHANED_SUPERSET.

Полные логи:
- `tools/reorg_scanner/reorg_scan_25422404_25587207.log` — reorg_scanner output
- `tools/reorg_scanner/reorg17_diff.log` — детальный diff по 17 реорг-блокам
- `tools/forked_block_checker/etherscan170_diff.log` — diff по 170 Etherscan-блокам

---

## Валидация: Scylla vs Dune (canonical)

Диапазон 25,422,404–25,580,592 (только v21 блоки, Dune Query 8068198):

| метрика | Scylla `block_completions` | Dune canonical | дельта |
|---------|---------------------------|----------------|--------|
| tx_count sum  | 54,487,366  | 54,487,366  | **0** ✓ |
| log_count sum | 119,340,532 | 119,340,532 | **0** ✓ |

**Ключевой вывод:** `block_completions.tx_count` и `log_count` точно совпадают с Dune, потому что их записывает canonical retry (не orphaned write). Разница только в `eth.transactions`:

```
eth.transactions actual rows = 54,487,366 (canonical) + 1,359 (orphaned tail) = 54,488,725
eth.logs actual rows         = 119,340,532 (canonical) + X orphaned logs (не подсчитаны)
```

### Формула быстрой валидации

```
canonical_tx_count = SUM(block_completions.tx_count, range)   [O(chunks), секунды]
valid iff: canonical_tx_count == Dune_tx_count

actual_stored_tx  = canonical_tx_count + Σ orphan_only[reorg_blocks]
actual_stored_log = canonical_log_count + Σ orphan_logs[reorg_blocks]
```

Для проверки конкретного диапазона:
1. `sum_block_completions --from=F --checkpoint=T` → bc_tx_sum, bc_log_sum
2. Dune Query 8068198 (template) с нужным диапазоном
3. Если bc_sum == Dune → canonical данные чистые; разница = orphaned хвосты

---

## Что делать дальше (статус: PENDING)

### Задача 1: Cleanup v21 orphaned txs в 25422404–25580592

**Что удалять:** orphaned tail txs (индексы > bc_tx_count) и их данные в связанных таблицах.

Список orphaned tx hashes — в `tools/reorg_scanner/reorg17_diff.log` (секция `ORPHAN_ONLY_HASHES`).

Затронутые таблицы:
- `eth.transactions` — DELETE WHERE chunk=? AND block_number=? AND hash=?
- `eth.logs` — DELETE WHERE chunk=? AND block_number=? AND transaction_hash=? (или transaction_index)
- `contracts_by_address_v2` — если orphaned tx содержал CREATE → нужно удалить

**Инструмент:** написать `tools/reorg_cleanup/` (Go), dry-run режим по умолчанию.
Входные данные: файл с (block_number, orphan_tx_hash) парами из reorg17_diff.log.

**Верификация после:** `reorg_scanner --from=25422404 --to=25580592` → должно быть 0 реоргов.

**Статус:** не начато (требует явного подтверждения).

### Задача 2: Re-index 7 v22 блоков (>25,580,592)

Эти блоки содержат MISSING_CANONICAL/MIXED — нужно:
1. Удалить все данные для блока (txs, logs, contracts)
2. Переиндексировать с canonical RPC-данных

Или исправить v22 indexer и запустить backfill.

**Статус:** не начато.

### Задача 3: Фикс v22 indexer для будущих реоргов

При обнаружении реорга на N+1 (parent hash mismatch) — переиндексировать N:
1. Загрузить canonical block N через RPC
2. Удалить старые данные блока N из eth.transactions, eth.logs
3. Записать canonical данные

Файл: `src/pipeline/writer.zig` (функция `saveBlockRt` / realtime loop).

**Статус:** не начато.

---

## Инструменты

| инструмент | назначение | статус |
|-----------|-----------|--------|
| `tools/reorg_scanner/` | полный ретроскан [from, to], fast (hash) + slow (tx) path | **готов** |
| `tools/forked_block_checker/` | детальный tx-diff по списку блоков | **готов** |
| `tools/retro_reorg_scan/` | ранний вариант (tx_count mismatch, записывает в forked_blocks) | готов, устарел |
| `tools/sum_block_completions/` | быстрый подсчёт bc_tx/log sum для Dune сравнения | **готов** |
| `tools/reorg_cleanup/` | удаление orphaned данных, dry-run режим | **не написан** |

### Быстрый запуск reorg_scanner на сервере

```bash
ssh alexey_smolyakov@100.64.0.4

# Проверить текущее состояние (Redis cursor = текущий head):
nohup ~/reorg_scanner --from=25422404 --to=0 \
  '--redis=redis://:ZCy8k4G6pcRYVFfm@127.0.0.1:6379/2' \
  --output=~/reorg_scan_$(date +%Y%m%d).log > ~/reorg_scan_stdout.log 2>&1 &

# Быстрый подсчёт сумм:
~/sum_block_completions --pass=cassandra --from=25422404 --checkpoint=25587207 --workers=16
```
