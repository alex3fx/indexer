# Задача: дедуплицированное хранилище байткодов с разрешением коллизий и системой верификации

## Контекст

Ты работаешь в существующем индексере Polygon-блокчейна на Zig. Пайплайн: Fetcher → Parser → Transformer → Writer Pool, память — arena-per-block, backpressure через bounded queues, хранилище — ScyllaDB, per-block транзакции. Перед началом работы изучи существующий код:

1. Найди и прочитай модули работы со ScyllaDB (CQL-клиент, prepared statements, batch-логику).
2. Найди место в пайплайне, где извлекаются деплои контрактов (creation code / deployed code из receipts или trace).
3. Найди существующий контур reorg-обработки и confirmation buffer — новая функциональность обязана в него встроиться.
4. Прочитай, как устроены существующие миграции схемы (если есть), и следуй той же конвенции.

Не переписывай существующую архитектуру. Встраивайся в неё.

## Что нужно реализовать

### 1. Схема ScyllaDB (миграция)

```sql
CREATE TABLE bytecode_store (
    hash        blob,      -- sha256(bytecode)
    seq         tinyint,   -- 0 всегда; >0 только при реальной коллизии sha256
    bytecode    blob,
    check_hash  blob,      -- keccak256(bytecode), независимый контрольный хэш
    size        int,
    kind        tinyint,   -- 0 = deployed, 1 = creation
    verified    boolean,
    verified_at timestamp,
    verified_via_address blob,
    abi         blob,      -- json, zstd-сжатый
    source_ref  text,
    PRIMARY KEY ((hash), seq)
) WITH compression = {'sstable_compression': 'ZstdCompressor'};

CREATE TABLE contracts_by_address (
    address        blob,
    block_number   bigint,
    bytecode_hash  blob,
    bytecode_seq   tinyint,
    creation_hash  blob,
    creation_seq   tinyint,
    tx_hash        blob,
    deployer       blob,
    PRIMARY KEY ((address), block_number)
) WITH CLUSTERING ORDER BY (block_number DESC);

CREATE TABLE addresses_by_bytecode (
    hash    blob,
    seq     tinyint,
    bucket  smallint,   -- первый байт адреса: address[0]
    address blob,
    block_number bigint,
    PRIMARY KEY ((hash, seq, bucket), address)
);

CREATE TABLE bytecode_stats (
    hash blob,
    seq  tinyint,
    address_count counter,
    PRIMARY KEY ((hash, seq))
);

CREATE TABLE collision_registry (
    hash blob PRIMARY KEY,
    detected_at timestamp,
    seq_count tinyint,
    note text
);

CREATE TABLE source_store (
    hash  blob,
    seq   tinyint,
    chunk int,
    data  blob,          -- чанки ~512KB
    PRIMARY KEY ((hash, seq), chunk)
);

CREATE TABLE pending_verifications (
    address blob PRIMARY KEY,
    source  blob,
    abi     blob,
    received_at timestamp
);
```

### 2. Модуль `bytecode_store.zig` — content-addressed store с разрешением коллизий

Идентичность байткода — составной идентификатор `BytecodeId = struct { hash: [32]u8, seq: i8 }`. Весь остальной код оперирует только `BytecodeId`, никогда голым хэшем.

Функция `resolveOrInsert(bytecode: []const u8, kind: Kind) !BytecodeId`, алгоритм строго такой:

1. `h = sha256(bytecode)`, `ck = keccak256(bytecode)`, `size = bytecode.len`.
2. `SELECT seq, size, check_hash, bytecode FROM bytecode_store WHERE hash = h` — читаем всю партицию (в норме 0–1 строка).
3. Для каждой строки: сравнить `size`, затем `check_hash`, при совпадении — **побайтовое сравнение** (`std.mem.eql`). Совпало → вернуть существующий `(h, seq)`. Байтовое сравнение обязательно всегда — это горячий путь детекции коллизий, он не должен быть мёртвым кодом за флагом.
4. Ни одна строка не совпала, но строки были → **коллизия sha256**: `INSERT ... IF NOT EXISTS` с `seq = max_seq + 1` (LWT), запись в `collision_registry`, лог уровня critical + метрика для алертинга. Если LWT не прошёл (конкурентная вставка) — перечитать партицию и повторить с шага 3.
5. Партиция пуста → `INSERT ... IF NOT EXISTS` с `seq = 0`. LWT не прошёл — перечитать, повторить с шага 3.

Оптимизация: перед походом в Scylla проверять in-memory LRU-кэш `hash → BytecodeId` (подавляющее большинство деплоев — клоны proxy с одинаковым кодом; кэш срежет 99%+ обращений). Кэш валиден, потому что content-addressed данные иммутабельны. Размер кэша — конфигурируемый, дефолт ~100k записей.

LWT используется **только** на вставке нового уникального байткода (редкое событие). Вставка адресов идёт обычными writes без LWT.

### 3. Интеграция в пайплайн индексера

В обработке блока, при обнаружении деплоя контракта:

1. `deployed_id = resolveOrInsert(deployed_code, .deployed)`
2. `creation_id = resolveOrInsert(creation_code, .creation)`
3. Insert в `contracts_by_address` (address, block_number, оба id, tx_hash, deployer).
4. Insert в `addresses_by_bytecode` с `bucket = address[0]`.
5. Increment `bytecode_stats.address_count`.
6. Прочитать `verified` из `bytecode_store` (или из кэша): если true → эмитить эвент `NewVerifiedAddress` (см. раздел 5).
7. Проверить `pending_verifications` по адресу: есть запись → запустить процесс верификации (раздел 4) и удалить pending-запись.

Пункты 3–5 должны исполняться только для подтверждённых блоков — после прохождения существующего confirmation buffer, тем же путём, каким сейчас пишутся остальные таблицы. При reorg откатываются только связи (`contracts_by_address`, `addresses_by_bytecode`, декремент stats); строки `bytecode_store` не трогаем — content-addressed данные безвредны.

Память: все временные буферы (хэши, результаты чтений) — из arena блока, как принято в кодовой базе. Байткод для кэша копировать в отдельный аллокатор кэша, не в арену.

### 4. API верификации

Публичная функция/эндпоинт (встрой в существующий транспорт индексера — посмотри, как сервисы сейчас общаются; если транспорта нет, сделай функцию модуля, вызываемую снаружи):

`verify(address, source_archive: []const u8, abi_json: []const u8) !VerifyResult`

1. Резолв `address → BytecodeId` через `contracts_by_address` (последняя версия по block_number). Адрес не найден → upsert в `pending_verifications`, вернуть `.pending`.
2. Сжать abi zstd'ом. Записать source-архив чанками по 512KB в `source_store`.
3. `UPDATE bytecode_store SET verified=true, verified_at=now, verified_via_address=addr, abi=..., source_ref=... WHERE hash=? AND seq=? IF verified=false` (LWT). Не прошло → уже верифицирован, вернуть `.already_verified`.
4. Эмит эвентов: прочитать `bytecode_stats.address_count`. Если count ≤ порога (конфиг, дефолт 1000) — один эвент `VerificationCompleted` со списком адресов (пагинация по 256 бакетам `addresses_by_bytecode`). Если больше — эвент `VerificationCompleted{bytecode_id, count}` без списка + поток `VerifiedAddressesBatch` батчами (размер батча — конфиг, дефолт 1000).

Инвалидировать/обновить запись в LRU-кэше (`verified` флаг).

### 5. Эвенты

Посмотри, есть ли в кодовой базе механизм эмита эвентов (WebSocket emit, очередь и т.п.) — используй его. Если нет — определи интерфейс `EventSink` (vtable-паттерн, как принято в Zig) и реализуй заглушку с логированием, чтобы транспорт подключался позже.

Типы эвентов:
- `VerificationCompleted { bytecode_id, address_count, addresses: ?[]Address }`
- `VerifiedAddressesBatch { bytecode_id, addresses: []Address, batch_index, is_last }`
- `NewVerifiedAddress { address, bytecode_id, block_number }`

Семантика — at-least-once. В гонке между верификацией и вставкой нового адреса допустимо, что адрес попадёт в оба эвента; консьюмеры дедуплицируют по ключу `(address, bytecode_id)`. Порядок операций, гарантирующий отсутствие потерь: verifier сканирует адреса ПОСЛЕ установки флага verified; indexer читает флаг ПОСЛЕ вставки адреса.

### 6. Read API (для существующего data-provider слоя)

- `getContract(address) → { address, creation_code, deployed_code, abi?, source?, verified }` — 4 атрибута.
- `getSiblings(address) → пагинируемый список адресов с тем же deployed bytecode` — резолв address → BytecodeId, затем обход бакетов `addresses_by_bytecode`.
- `getAddressesByBytecode(bytecode_id) → тот же пагинируемый список`.

Пагинация — стандартная для кодовой базы; если нет конвенции — курсор `(bucket, last_address)`.

## Требования к качеству

- Zig-идиомы кодовой базы: явные аллокаторы в сигнатурах, errdefer для cleanup, никаких скрытых аллокаций.
- Prepared statements для всех CQL-запросов, подготовка один раз при старте.
- Метрики (в существующую систему метрик, если есть): cache hit rate, число LWT-ретраев, счётчик коллизий (алерт при > 0), latency resolveOrInsert.
- Тесты: unit-тесты на логику разрешения коллизий с мокнутым стораджем — обязательно покрыть случаи: пустая партиция; дедуп-хит; коллизия (одинаковый sha256, разные байты — сконструируй фейковые данные, подменив функцию хэширования в тесте через DI/comptime-параметр); конкурентная вставка (LWT-ретрай); гонка верификация/новый адрес.
- Идемпотентность всех writes: повторная обработка блока после рестарта не должна ломать данные (counter в `bytecode_stats` — единственное неидемпотентное место; защити его existing-check'ом через `addresses_by_bytecode` перед инкрементом, либо задокументируй допустимую погрешность и добавь reconcile-джобу).

## Порядок работы

1. Изучи кодовую базу (модули Scylla, пайплайн, reorg, эвенты, тесты) — составь короткий план интеграции и покажи его перед написанием кода.
2. Миграция схемы.
3. `bytecode_store.zig` + unit-тесты на коллизии.
4. Интеграция в пайплайн (за фичефлагом, если в проекте есть такая практика).
5. Верификация + эвенты.
6. Read API.
7. Прогон всех тестов проекта, `zig build test`.

Коммить по шагам с осмысленными сообщениями. Если обнаружишь конфликт с существующей схемой (например, уже есть таблица контрактов) — остановись и опиши варианты миграции данных, не принимай решение молча.
