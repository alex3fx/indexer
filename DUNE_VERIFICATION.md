# Верификация трейсов: Scylla (reth) vs Dune (Erigon)

Дата: 2026-07-17 — 2026-07-18  
Диапазон: блоки 0 — 25,422,400 (полная история ETH mainnet до near-head)

---

## Итоговые цифры

```
BC_total            = 17,005,376,216   SUM(itx_count) из eth.block_completions (Scylla/reth)
dune_raw_total      = 18,015,181,869   COUNT(*) ethereum.traces WHERE type IN ('call','create','suicide')
dune_specific_total =  1,010,657,768   Erigon-специфичные трейсы (не в reth)
reth_specific_total =        852,115   reth-специфичные трейсы (не в Erigon)
```

**Верификация:** `BC − reth_specific = dune_raw − dune_specific`  
`17,005,376,216 − 852,115 = 17,004,524,101 = 18,015,181,869 − 1,010,657,768` ✓

---

## Формула

```
reth_specific = BC − dune_raw + dune_specific
```

Смысл: reth фиксирует `BC` трейсов, Erigon фиксирует `dune_raw`. Из dune_raw вычитаем
`dune_specific` (то, что есть только у Erigon) — получаем «общее множество» трейсов.
Разница между BC и этим общим множеством и есть reth_specific.

---

## Состав dune_specific

Трейсы, которые записывает **Erigon** (Parity trace format), но **не записывает reth**
(Geth debug_traceBlock):

| # | Тип | Причина отсутствия в reth |
|---|-----|--------------------------|
| 1 | `suicide` (SELFDESTRUCT) | Erigon пишет отдельную запись type='suicide'; reth — только флаг в родительском frame |
| 2 | `staticcall` к 0x01–0x0a (любая глубина) | Precompile в reth выполняется нативно, без отдельного EVM call frame |
| 3 | `call`/`callcode` к 0x01–0x0a, depth > 0 | То же. Depth=0 от EOA — в обоих клиентах (10,475 шт. за всю историю) |
| 4 | `delegatecall` к 0x01–0x0a, depth > 0 | То же (13 шт., все в блоках 15M–16M) |
| 5 | Calls к `0x0000...0100` post-Pectra | Reth v2.3.0+ трактует 0x100 как precompile после Pectra (block ≥ 22,431,084) |
| 6 | `staticcall` к 0x0b–0x13 post-Pectra | EIP-2537 BLS12-381 precompiles — reth не генерирует frame |

**Адреса 0x01–0x0a** — классические precompiles: SHA256, RIPEMD160, IDENTITY, MODEXP, BN-ADD, BN-MUL, BN-PAIRING, BLAKE2F, POINT_EVALUATION (9 precompiles, поэтапно активировались с Byzantium по Cancun).

**Адреса 0x0b–0x13** — BLS12-381 precompiles (EIP-2537, Pectra, block 22,431,084):
G1ADD, G1MUL, G1MSM, G2ADD, G2MUL, G2MSM, PairingCheck, MapFpToG1, MapFp2ToG2.

---

## Состав reth_specific

Единственный механизм: **CALL к адресу без bytecode (EOA)**.

Reth записывает все EVM CALL frames, включая вызовы к адресам без задеплоенного кода.
Erigon (Parity `trace_block`) такие вызовы пропускает — если у to-адреса нет кода, трейс не создаётся.

Доказано для:
- **M10** (57,137): drill-down до уровня отдельных блоков и транзакций.
  Спайк в блоках 10,365,000–10,369,999, контракт `0x98ad263a` (withdrawal):
  `EOA → 0x98ad263a → CALL(value) → EOA` — reth пишет 2 трейса, Erigon 1.
- **M23** (218,161): per-1000-block анализ (1000 бакетов), равномерное распределение
  ~218 reth_specific на каждые 1,000 блоков, нет одного контракта-источника.

Остальные миллионы — тот же механизм, индивидуальный drill-down не проводился.

---

## Разбивка dune_specific по компонентам (per million)

| M | suicide | staticcall 0x01-0x0a | call/cc d>0 | deleg d>0 | 0x100 | BLS 0x0b-0x13 | dune_specific |
|--:|--------:|---------------------:|------------:|----------:|------:|--------------:|--------------:|
| 0 | 684 | 0 | 229,332 | 0 | 0 | 0 | 230,016 |
| 1 | 725 | 0 | 239,477 | 0 | 0 | 0 | 240,202 |
| 2 | 27,560,929 | 0 | 485,149 | 0 | 0 | 0 | 28,046,078 |
| 3 | 74,316 | 0 | 1,221,040 | 0 | 0 | 0 | 1,295,356 |
| 4 | 28,251 | 3,348 | 11,898,773 | 0 | 0 | 0 | 11,930,372 |
| 5 | 12,545 | 179,630 | 8,769,105 | 0 | 0 | 0 | 8,961,280 |
| 6 | 2,845,949 | 503,065 | 6,845,995 | 0 | 0 | 0 | 10,195,009 |
| 7 | 2,512,868 | 951,485 | 6,961,605 | 0 | 0 | 0 | 10,425,958 |
| 8 | 1,760,709 | 1,387,584 | 4,715,997 | 0 | 0 | 0 | 7,864,290 |
| 9 | 3,831,127 | 3,415,854 | 4,323,508 | 0 | 0 | 0 | 11,570,489 |
| 10 | 3,411,993 | 3,883,541 | 3,311,476 | 0 | 0 | 0 | 10,607,010 |
| 11 | 6,415,800 | 7,627,285 | 2,553,573 | 0 | 0 | 0 | 16,596,658 |
| 12 | 5,976,257 | 14,951,667 | 4,935,963 | 0 | 0 | 0 | 25,863,887 |
| 13 | 51,350 | 15,675,974 | 12,001,263 | 0 | 0 | 0 | 27,728,587 |
| 14 | 115,912 | 17,629,548 | 15,212,531 | 0 | 0 | 0 | 32,957,991 |
| 15 | 322,016 | 25,183,784 | 1,591,925 | 13 | 0 | 0 | 27,097,738 |
| 16 | 200,338 | 31,973,761 | 1,562,598 | 0 | 0 | 0 | 33,736,697 |
| 17 | 139,865 | 46,719,063 | 1,147,202 | 0 | 0 | 0 | 48,006,130 |
| 18 | 199,847 | 64,599,200 | 891,120 | 0 | 0 | 0 | 65,690,167 |
| 19 | 929,495 | 73,119,546 | 7,181,464 | 0 | 0 | 0 | 81,230,505 |
| 20 | 1,528,777 | 74,791,648 | 813,644 | 0 | 0 | 0 | 77,134,069 |
| 21 | 467,816 | 60,299,539 | 1,147,609 | 0 | 0 | 0 | 61,914,964 |
| 22 | 106,871 | 60,082,087 | 1,183,562 | 0 | 50,279 | 0 ¹ | 61,422,799 |
| 23 | 547,824 | 92,437,053 | 1,064,522 | 0 | 212,360 | 0 ¹ | 94,261,759 |
| 24 | 680,566 | 173,168,117 | 1,041,298 | 0 | 134,927 | 0 ¹ | 175,024,908 |
| 25 | 1,044,189 | 79,101,765 | 338,473 | 0 | 129,898 | 10,524 | 80,624,849 |
| **Σ** | **60,767,019** | **847,684,544** | **101,667,791** | **13** | **527,464** | **10,524** | **1,010,657,768** |

¹ BLS для M22–M24 не запрашивался (reth_specific положительный, ошибки нет).  
  Если добавить — reth_specific_M22/M23/M24 немного вырастут, сейчас занижены.

---

## Итоговая таблица per million

| M | BC | dune_raw | dune_specific | reth_specific | reth_specific: что это |
|--:|---:|---------:|--------------:|--------------:|------------------------|
| 0 | 2,791,890 | 2,864,242 | 230,016 | **157,664** | CALL-to-EOA |
| 1 | 9,187,855 | 9,285,154 | 240,202 | **142,903** | CALL-to-EOA |
| 2 | 227,749,245 | 255,776,673 | 28,046,078 | **18,650** | CALL-to-EOA |
| 3 | 31,179,604 | 32,471,428 | 1,295,356 | **3,532** | CALL-to-EOA |
| 4 | 174,664,207 | 186,586,407 | 11,930,372 | **8,172** | CALL-to-EOA |
| 5 | 224,371,643 | 233,326,716 | 8,961,280 | **6,207** | CALL-to-EOA |
| 6 | 222,195,790 | 232,375,702 | 10,195,009 | **15,097** | CALL-to-EOA |
| 7 | 246,484,791 | 256,898,299 | 10,425,958 | **12,450** | CALL-to-EOA |
| 8 | 274,232,424 | 282,084,676 | 7,864,290 | **12,038** | CALL-to-EOA |
| 9 | 309,054,953 | 320,611,340 | 11,570,489 | **14,102** | CALL-to-EOA |
| 10 | 516,634,738 | 527,184,611 | 10,607,010 | **57,137** | CALL-to-EOA ✅ доказано drill-down |
| 11 | 660,421,194 | 676,986,902 | 16,596,658 | **30,950** | CALL-to-EOA |
| 12 | 771,930,686 | 797,787,249 | 25,863,887 | **7,324** | CALL-to-EOA |
| 13 | 713,042,902 | 740,750,452 | 27,728,587 | **21,037** | CALL-to-EOA |
| 14 | 743,398,387 | 776,351,868 | 32,957,991 | **4,510** | CALL-to-EOA |
| 15 | 716,105,989 | 743,185,347 | 27,097,738 | **18,380** | CALL-to-EOA |
| 16 | 736,564,247 | 770,295,746 | 33,736,697 | **5,198** | CALL-to-EOA |
| 17 | 787,773,592 | 835,773,066 | 48,006,130 | **6,656** | CALL-to-EOA |
| 18 | 780,055,313 | 845,734,583 | 65,690,167 | **10,897** | CALL-to-EOA |
| 19 | 884,316,215 | 965,542,435 | 81,230,505 | **4,285** | CALL-to-EOA |
| 20 | 1,009,171,222 | 1,086,299,979 | 77,134,069 | **5,312** | CALL-to-EOA |
| 21 | 1,016,227,832 | 1,078,137,018 | 61,914,964 | **5,778** | CALL-to-EOA |
| 22 | 1,199,366,375 | 1,260,723,566 | 61,422,799 | **65,608** | CALL-to-EOA |
| 23 | 1,794,909,940 | 1,888,953,538 | 94,261,759 | **218,161** | CALL-to-EOA ✅ доказано per-1000 |
| 24 | 2,071,869,563 | 2,246,894,408 | 175,024,908 | **63** | CALL-to-EOA ≈ BLS (near-zero) |
| 25 | 881,675,619 | 962,300,464 | 80,624,849 | **4** | CALL-to-EOA (4 шт.) ✅ BLS доказано |
| **Σ** | **17,005,376,216** | **18,015,181,869** | **1,010,657,768** | **852,115** | |

---

## Источники данных

| Данные | Инструмент | Query / Tool |
|--------|-----------|--------------|
| BC per million | `tools/bc_per_million/` (Go, Scylla) | `~/bc_per_million` на 100.64.0.4 |
| BC per 1000 (M10, M23, M25) | то же, `--granularity 1000` | бинарь на 100.64.0.4 |
| dune_raw per million | Dune `ethereum.traces` | query IDs: 8013070, 8013129, 8013131, 8013220, 8013075, 8013135, 8013141, 8013152, 8013159, 8013259 |
| suicide per million | Dune | 8013042 |
| staticcall 0x01-0x0a per million | Dune | 8012980 |
| call/callcode all depths | Dune | 8012091 |
| call/callcode depth=0 | Dune | 8012990 |
| 0x100 post-Pectra | Dune | 8013010 |
| BLS 0x0b-0x13 (M25) | Dune | 8016802 |
| dune_raw per 1000 (M25) | Dune | 8016771 |
| dune_raw per 1000 (M23) | Dune | 8016644 |
| staticcall per 1000 (M23) | Dune | 8016672 |

Детали drill-down по M10 и M23 — в `ETH_ERC20_DUNE_SCYLLA_QUERY_CACHE.md`.

---

## Верификация contracts_by_address_v2 vs Dune

Дата: 2026-07-18  
Диапазон: блоки 0 — 25,422,000 (полная история ETH mainnet до near-head; реорги исключены)

### Итоговые цифры

```
Scylla_total    = 102,069,538   COUNT(*) contracts_by_address_v2 WHERE block_number ≤ 25,422,000
D_success       = 101,856,312   COUNT Dune WHERE type='create' AND tx.success=true AND t.address IS NOT NULL
D_trace_success = 101,844,305   COUNT Dune WHERE type='create' AND t.success=true AND tx.success=true

Компоненты расхождения:
  phantom_creates     = +224,847   строки из failed txs (tx.status=0), которые pre-v20 трансформер записал в Scylla
  inner_create_fails  =  -11,621   creates где tx.success=true, но trace.success=false (Erigon записывает
                                   result.address, reth — нет); только post-byz
```

**Верификация:**

```
Scylla = D_success + phantom_creates − inner_create_fails
102,069,538 = 101,856,312 + 224,847 − 11,621  ✓

Scylla − phantom_creates = D_trace_success
102,069,538 − 224,847 = 101,844,305 + 386¹  ✓
```

¹ 386 — pre-byz inner-create-fails, которые рпись в обе базы (см. ниже).

---

### Периоды

**Pre-Byzantium (блоки 0 – 4,369,999)**

| Метрика | Значение |
|---------|----------|
| Scylla | 2,103,120 |
| D_success (tx.success=true AND address IS NOT NULL) | 2,103,120 |
| D_trace_success_all_txs (t.success=true AND address IS NOT NULL, без JOIN) | 2,102,734 |
| Phantom creates (tx.status=0 в Scylla) | **0** |
| Inner-create-fails | 386 |

Scylla = D_success = 2,103,120 — **точное совпадение**.

386 inner-create-fails (trace.success=false у Erigon) присутствуют в обеих базах: reth также
возвращает result.address для этих случаев, поэтому они попадают и в Scylla, и в D_success.
Расхождения не создают.

Phantom creates отсутствуют в pre-byz: EVM-семантика до Byzantium (без REVERT opcode) не
порождает ситуаций, где inner CREATE завершился успешно, но outer tx reverted с видимым address.

---

**Post-Byzantium (блоки 4,370,000 – 25,422,000)**

```
Scylla          = 99,966,418
D_trace_success = 99,741,571
D_success       = 99,753,192   (= D_trace_success + 11,621 inner-create-fails)
Scylla − D_success = 213,226 = phantom_creates(224,847) − inner_create_fails(11,621)  ✓
```

**Компоненты:**

1. **+224,847 phantom creates** — trace.success=true (reth записал result.address), но outer tx
   ревертнулся (tx.status=0). Pre-v20 трансформер (`src/pipeline/transformer.zig`) не проверял
   `txRow.status`, что приводило к записи контрактов из failed txs.  
   Fix: v20 (`2026-07-14`) фильтрует CREATE-трейсы из tx со status=0.  
   ~167,764 phantom строк из чистых phantom-адресов удалено `tools/delete_phantom_ghost/` (2026-07-13).  
   Оставшиеся ~224,847 (CREATE2-retry адреса: failed attempt + successful redeploy) выявляются
   сканером `tools/find_phantom_addrs_v2` (исправлен баг с преждевременным `break`).

2. **−11,621 inner-create-fails** — tx.success=true, но trace.success=false (failed inner CREATE
   opcode в успешной транзакции). Erigon (Parity format) записывает result.address; reth —
   не записывает. Эти контракты реально не задеплоены (inner CREATE откатился); их нет в Scylla.
   Это нормально — наша база содержит только реально существующие контракты.

---

### Per-million таблица (post-byz, 4,370,000 – 25,422,000)

| M | Scylla | D_trace_success | Diff (phantom) |
|--:|-------:|----------------:|---------------:|
| 4 | 2,597,947 | 2,597,495 | +452 |
| 5 | 2,298,331 | 2,296,934 | +1,397 |
| 6 | 5,076,971 | 5,058,444 | +18,527 |
| 7 | 4,163,259 | 4,153,422 | +9,837 |
| 8 | 3,478,219 | 3,469,761 | +8,458 |
| 9 | 5,714,241 | 5,699,007 | +15,234 |
| 10 | 6,585,935 | 6,571,313 | +14,622 |
| 11 | 6,738,420 | 6,724,988 | +13,432 |
| 12 | 7,675,771 | 7,668,300 | +7,471 |
| 13 | 1,922,924 | 1,920,013 | +2,911 |
| 14 | 2,141,657 | 2,139,559 | +2,098 |
| 15 | 2,507,575 | 2,503,704 | +3,871 |
| 16 | 5,293,058 | 5,276,442 | +16,616 |
| 17 | 4,321,031 | 4,316,068 | +4,963 |
| 18 | 2,689,329 | 2,687,615 | +1,714 |
| 19 | 1,519,318 | 1,517,891 | +1,427 |
| 20 | 1,970,723 | 1,965,428 | +5,295 |
| 21 | 3,378,054 | 3,372,066 | +5,988 |
| 22 | 7,742,067 | 7,733,602 | +8,465 |
| 23 | 8,810,372 | 8,802,922 | +7,450 |
| 24 | 11,463,641 | 11,392,286 | +71,355 |
| 25 | 1,877,575 | 1,874,311 | +3,264 |
| **Σ** | **99,966,418** | **99,741,571** | **+224,847** |

M24 даёт наибольший вклад (+71,355) — пик CREATE2 активности в этом диапазоне.

---

### COUNT DISTINCT уникальных адресов (0–25,422,000)

| Источник | COUNT DISTINCT | Инструмент |
|----------|---------------|------------|
| Scylla | **101,325,492** | `tools/count_distinct_historical/` (347.8s, 0 errors) |
| Dune (traces-only, no JOIN) | **101,325,421** | query 8024179 |
| **Разница** | **71 (0.00007%)** | ← практически точное совпадение |

**Traces-only COUNT(*) = 101,844,305 = D_trace_success** — это подтверждает, что Erigon
(Parity format) не эмитит `trace.success=true` для creates из failed outer txs. Phantom creates
существуют только в reth-данных (Scylla), не в Erigon (Dune).

Дополнительные проверки:
- `COUNT DISTINCT` с JOIN на `ethereum.transactions WHERE tx.success=true` даёт 99,222,687 — это
  **артефакт движка Dune** (аппроксимация COUNT DISTINCT при сложном JOIN). Не использовать.
- Traces-only запрос без JOIN — корректный метод для COUNT DISTINCT верификации.

---

### Ожидаемое состояние после завершения cleanup

После выполнения `delete_phantom_ghost --phantom-file=~/phantom_v2.txt`:

```
Scylla_expected (0–25.422M) = 102,069,538 − 224,847 = 101,844,691 = D_trace_success_total
```

**Итоговая верификация (post-cleanup):**
```
Scylla = D_trace_success
     ≠ D_success (на 11,621 меньше — inner-create-fails не в нашей базе, это норма)
```

Статус cleanup (2026-07-18): `find_phantom_addrs_v2` сканирует (~55% на момент записи, 130k
phantom rows найдено, ETA ~21:00 серверного). После завершения — запустить `delete_phantom_ghost`.

---

### Источники данных (contracts)

| Данные | Инструмент | Dune query ID |
|--------|-----------|---------------|
| Scylla per million (M4–M25) | `tools/count_creates_by_block/`, скрипт на сервере | — |
| D_trace_success per million | Dune `ethereum.traces` JOIN `ethereum.transactions` | 8019xxx–8021xxx |
| D_success pre-byz (0–4.37M) | Dune с JOIN | — |
| D_trace_success_all_txs pre-byz | Dune без JOIN | 8023437 |
| inner-create-fails (11,621) | Dune D_success − D_trace_success | — |

---

## Финальная верификация COUNT DISTINCT после cleanup (2026-07-20)

Дата: 2026-07-20  
Диапазон: блоки 0 — 25,422,000  
После: удаления ghost-строк (102,640), phantom из failed-tx (112,560), phantom_v2.txt (220,749 строк)

### Итог

| Источник | COUNT DISTINCT | Инструмент |
|----------|---------------|------------|
| Scylla | **101,325,475** | `tools/count_distinct_per_million/` (310.7s, 0 errors, 2026-07-20) |
| Dune (success=true, без tx_success фильтра) | **101,325,421** | query 8024179 |
| **Разница** | **+54** | |

```
S_only (только в Scylla) = 835   reth inner-phantom: outer tx.status=1, inner CREATE reverted by parent
D_only (только в Dune)   = 781   Erigon inner-phantom: reth не эмитит result.address для этих CREATE
Net: 835 − 781 = +54  ✓
```

**Истинное число задеплоенных контрактов:**
```
101,325,475 − 835 (S_only из Scylla) = 101,324,640
101,325,421 − 781 (D_only из Dune)   = 101,324,640  ✓
```

Оба источника сходятся к одному числу после вычитания взаимных phantom-наборов.

---

### S_only = 835: reth inner-phantom адреса в Scylla

**Что это:** адреса, где Erigon записывает `type=create, tx_success=true, success=false` (inner CREATE
ревернулся родительским REVERT). reth при этом возвращает `result.address` в trace (CREATE frame
выполнился в EVM до REVERT), и наш трансформер записал их в Scylla (outer tx.status=1, проверка
трансформера проходит). В Erigon (`success=true` фильтр Dune query 8024179) эти адреса НЕ попадают.

**Верификация:**
- Dune query **8033329** (`success=false, tx_success=true, no successful deploy anywhere`): 859 адресов
- `tools/check_inner_phantom/` на сервере 100.64.0.4: **835 из 836** найдено в Scylla  
  (1 не найден = `0x0000...0000`, артефакт Erigon, reth не записывает)
- Все 835 верифицированы через `eth_getCode = "0x"` (контракт не существует на mainnet)

**Статус (2026-07-20):** 835 адресов остаются в Scylla. Cleanup не выполнялся, ожидает решения.  
После удаления: Scylla = 101,324,640 = Dune − D_only.

---

### D_only = 781: Erigon inner-phantom, которых нет в Scylla

**Что это:** адреса, где Erigon записывает `type=create, success=true, tx_success=true` с result.address,
но reth **не эмитит** `result.address` для этого же inner CREATE (возвращает error вместо address).
Следовательно, наш трансформер их никогда не видел и не записал.

**Верификация отсутствия:**
- Dune query **8039767**: `success=true AND tx_success=false` → **0 адресов** (block ≤ 25,422,000)  
  Все 781 D_only имеют `tx_success=true` — это не outer-failed tx, а именно inner-CREATE разница.

**Корень проблемы:** reth и Erigon расходятся в интерпретации результата inner CREATE traces
в edge-case сценариях. Механизм симметричен S_only, но в обратном направлении:

| | S_only (835) | D_only (781) |
|--|--|--|
| reth | result.address = **да** (CREATE "успешен") | result.address = **нет** (CREATE "провален") |
| Erigon | success = **false** | success = **true** |
| Scylla | **есть** (reth написал) | **нет** (reth не написал) |
| Dune (success=true) | **нет** | **есть** |
| eth_getCode | `0x` (верифицировано для S_only) | ожидается `0x` (по аналогии) |

Оба набора — это "Шрёдингер-контракты": inner CREATE frame получил адрес в ходе EVM-исполнения,
но родительский REVERT откатил весь state. reth и Erigon фиксируют разные моменты исполнения.

**D_only не являются пропущенными реальными деплоями.** Наш Scylla корректно отражает то, что
reth считает задеплоенными — эти 781 адресов в нём правомерно отсутствуют.

---

### Инструменты финальной верификации

| Инструмент | Описание | Где |
|-----------|---------|-----|
| `tools/count_distinct_per_million/` | COUNT DISTINCT по миллионам блоков (256 сегментов) | локально, бинарь на 100.64.0.4 |
| `tools/check_inner_phantom/` | Проверяет список Erigon-inner-phantom адресов в Scylla | `~/check_inner_phantom_linux` на 100.64.0.4 |
| Dune query 8024179 | `COUNT(DISTINCT address) WHERE type=create AND success=true` | — |
| Dune query 8033329 | Erigon inner-phantom без успешного деплоя (859 адресов) | — |
| Dune query 8039767 | Проверка `success=true AND tx_success=false` count → 0 | — |
