# CLAUDESCYLLA.md — Анализ скорости записи в ScyllaDB
Дата: 2026-05-25 | Модель: Claude Sonnet 4.6

## Что уже сделано в коде (фактически)

Прочитал `zigtest2/src/db.zig` и `realtime.zig` полностью. Основные факты о текущей реализации:

| Оптимизация | Статус | Где в коде |
|-------------|--------|------------|
| Prepared statements | ✅ | `prepareAll()`, `db.zig:646` |
| UNLOGGED BATCH 100 rows | ✅ | `batchSendRows()`, `db.zig:233` |
| CONSISTENCY ONE | ✅ | `CQL_CONSISTENCY_ONE = 0x0001`, `db.zig:121` |
| TCP_NODELAY | ✅ | `db.zig:45–48` |
| Parallel workers per table | ✅ | `spawnTable()`, `db.zig:922` |
| writev (zero-copy header) | ✅ | `tcpWritev()`, `db.zig:62` |
| PrevBatch (save∥fetch) | ✅ historical | `pipeline.zig:241` |
| PrevBatch в realtime | ❌ отсутствует | `realtime.zig:75–81` — synchronous |
| CQL stream pipelining | ❌ не используется | `sendFrameStream()` написана, `db.zig:187`, но нигде не вызывается |
| Token/shard-aware routing | ❌ | round-robin `acquire()`, `db.zig:622` |

## Три гипотезы: что конкретно менять

### H5: Commitlog tuning — самое быстрое

**Текущий Docker:** нет `--unsafe-bypass-fsync`, нет явного `commitlog-sync`.

В Scylla `developer-mode=1` не отключает fsync автоматически. Каждый BATCH → commitlog append → fsync → ответ клиенту. На NVMe fsync стоит 1–4ms. На 30 батчей/блок → 30–120ms суммарного ожидания, но они параллельны через pool=48.

**Тест A — bypass-fsync (нижняя граница latency):**
```bash
docker stop scylla && docker rm scylla
docker run -d --name scylla --network host scylladb/scylla:6.2 \
  --smp 32 --memory 128G --developer-mode 1 --overprovisioned 1 \
  --unsafe-bypass-fsync 1 \
  --listen-address 127.0.0.1 --rpc-address 127.0.0.1 \
  --broadcast-rpc-address 127.0.0.1 \
  --authenticator PasswordAuthenticator --authorizer CassandraAuthorizer
```
Ожидание: save → 8–10ms (как на WSL tmpfs). Это не production, но показывает потолок.

**Тест B — commitlog periodic (production-safe):**
Добавить к флагам: `--commitlog-sync periodic --commitlog-sync-period-in-ms 5000`
Ожидание: save → 12–14ms. Durability: потеря до 5s данных при crash.

**Что даст:** на WSL tmpfs с bypass-fsync было 8.5ms. На реальном диске с bypass — ожидаем 10–12ms (real I/O path, но без ожидания fsync ack).

---

### H2: Async save pipeline — код в realtime.zig

**Ключевой факт из кода:** `PrevBatch` уже реализован в `pipeline.zig:241–243` для исторического режима. В `realtime.zig` он не используется — `processBlock()` (строка 75–81) вызывает `saveBatch()` синхронно и возвращает только после join всех тредов.

**Текущий поток (WS mode):**
```
[recv block N from pipe] → [fetch N: 3ms] → [transform: 1ms] → [save N: 17ms] → [cursor N] → [recv N+1]
```
Пока идёт save(N), block N+1 уже ждёт в OS pipe, но мы его не читаем.

**Предлагаемый поток:**
```
[recv N] → [fetch N: 3ms] → [transform N: 1ms] → spawn save(N) thread
                                                         ↓
[recv N+1] → [fetch N+1: 3ms] → [join save(N)] → [cursor N] → [transform N+1] → spawn save(N+1) ...
```

**Изменение в realtime.zig:** разбить `processBlock` на `fetchTransformBlock()` (возвращает `Entities`) и отдельный `save` — спаунить save как thread сразу после transform, читать следующий блок из channel, join делать перед следующим save.

**Расчёт:**
- До: `total = fetch + transform + save = 3 + 1 + 17 = 21ms + Redis = 25ms`
- После: `total = max(save_prev, fetch_cur) + transform + Redis = max(17, 3) + 1 + ~2ms = 20ms`
- Реальный выигрыш в тесте (100 блоков instant): ≈ 5–7ms на avg

**Важно:** курсор Redis обновляется только после join save(N), то есть durable write latency не меняется — мы просто перекрываем ожидание следующим fetch.

---

### H1: Shard-aware routing

**Проблема:** round-robin `acquire()` в `db.zig:622` распределяет 48 соединений случайно по эфемерным портам. В Scylla `smp=32`: входящий порт клиента определяет, какой shard обслуживает соединение:
```
server_shard = client_ephemeral_port % num_shards
```
При 48 соединениях они раскидываются по ~32 шардам. Все данные идут в chunk 25079 → один шард X. ~47/48 запросов форвардируются cross-shard.

**Решение — shard-aware ports:** Scylla слушает `19042 + shard_id` для прямых подключений к шардам.

**Реализация в db.zig:**
1. При инициализации пула: отправить `OPTIONS` frame на порт 9042, прочитать `SCYLLA_SHARD_INFO` из `SUPPORTED` ответа → получить `num_shards`, `sharding_algorithm`, `sharding_ignore_msb_bits`
2. Вычислить token для partition key: `chunk = block_number / 1000` → INT32 → Murmur3 hash
3. `shard_id = (token >> ignore_msb_bits) % num_shards`
4. Открыть все 48 соединений на `port = 19042 + shard_id`

**Scylla Murmur3 для INT32 partition key:**
```
bytes = big_endian(int32_value)
token = murmur3_x86_128(bytes)[lower_64]
shard = (token >> ignore_msb) % num_shards
```

**Ожидание:** все 48 соединений будут работать с шардом напрямую. Форвардинг = 0. Выигрыш ~2–4ms при 30+ батчах на блок.

**Ограничение:** для теста с chunk=25079 (1 partition) все 48 коннекций всё равно упираются в один шард. Эффект сильнее при разных chunk (production-like).

---

## Сравнение с другими анализами

| Пункт | Grok | CDX | Claude |
|-------|------|-----|--------|
| Читал реальный код | ❌ | ✅ частично | ✅ полностью |
| Обнаружил sendFrameStream() unused | ❌ | ❌ | ✅ |
| Обнаружил PrevBatch не в realtime | ❌ | ❌ | ✅ |
| Consistency | LOCAL_ONE рекомендует | ONE уже стоит | ONE уже стоит |
| gocql рекомендации | ✅ (но неприменимо) | ✅ как ориентир | не применимо, Zig raw CQL |
| Приоритет H5 | 4е место | 1е место | 1е место |
| Async pipeline | "fire-and-forget" | "enqueue vs durable_ack" | конкретный код |
| Shard-aware | "19042 port" абстрактно | верно, но без кода | конкретная реализация |
| remap-mod ссылка на данные | ❌ | ✅ 5.36ms | ✅ |

**Расхождение с Grok:** gocql-специфичные советы (`TokenAwareHostPolicy`, `NumConns`) не применимы — это Zig с raw CQL TCP. NumConns у нас управляется напрямую через `POOL_SIZE` и `SPLIT`.

**Расхождение с CDX:** CDX правильно определил приоритеты, но пропустил ключевую деталь — `sendFrameStream()` уже реализована в коде (db.zig:187). Это означает, что CQL-уровневый pipelining (несколько in-flight запросов на одно соединение без ожидания ack) практически готов. CDX также не предложил конкретную реализацию H2 (async pipeline), я — предложил.

---

## Приоритет и ожидаемые результаты

| # | Гипотеза | Сложность | Ожидаемый Δsave | Как |
|---|----------|-----------|-----------------|-----|
| **H5** | bypass-fsync | Config only | **−7–9ms** | docker restart |
| **H5b** | commitlog periodic | Config only | **−5–6ms** | docker restart |
| **H2** | Async save pipeline | ~50 строк Zig | **−5–7ms total** | realtime.zig |
| **H1** | Shard-aware ports | ~150 строк Zig | **−2–4ms** | db.zig |
| **H4** | sendFrameStream pipeline | ~100 строк Zig | **−1–3ms** | db.zig |

**Комбинированный прогноз** (H5b + H2 + H1):
- save: 17ms → ~8ms (`−9ms`)
- total: 25ms → ~15ms

**Предел без remap:** даже с идеальными оптимизациями, пока все блоки в одном chunk, bottleneck = пропускная способность одного шарда Scylla. С remap-mod=32 на сервере — ожидаем save 4–6ms (по аналогии с WSL remap-mod=8 → 5.4ms).

---

## Порядок экспериментов

1. **H5: bypass-fsync** → docker restart → прогнать тест → зафиксировать нижнюю границу
2. **H5b: commitlog periodic** → отдельный прогон → определить production-safe baseline
3. **H2: async pipeline в realtime.zig** → собрать → прогнать
4. **H1: shard-aware** → собрать → прогнать с chunk=25079 (single shard) и с remap (multi shard)
5. **remap-mod=32 на сервере** — отдельный тест, TS-incompatible но показывает предел системы
