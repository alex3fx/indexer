# CDXRESEARCH — выводы после анализа SHARD.md

Дата: 2026-05-21

## Контекст из SHARD.md

Тестовый диапазон:

```text
blocks 25078197..25079196
RAW_CHUNK_SIZE=1000
chunk = block_number / 1000
```

Этот диапазон попадает только в два raw chunks:

```text
25078
25079
```

Значит для raw таблиц активны только 2 hot partitions и, как следствие, максимум 2 hot shards. Увеличение `smp` само по себе не делает эти данные распределенными по 20-24 shards.

Лучший результат из `SHARD.md`:

```text
smp=24, memory=28G, tmpfs, unsafe-bypass-fsync
warm run: около 14.7 ms/block
```

Для `smp=2, memory=4G`:

```text
warm run: около 19.3 ms/block
```

## 1. Почему больше shards дает прирост, хотя данных только на 2 shards

Прирост не из-за того, что записи raw tables начали использовать все shards. Partitioning остался тем же: два `chunk` keys дают два hot shards.

Вероятная причина прироста в другом:

- меняется memory per shard;
- меняется размер и частота memtable flush;
- меняется поведение compaction/flush scheduling;
- при tmpfs + `--unsafe-bypass-fsync=1` маленькие частые flush могут быть дешевле, чем редкие большие;
- parser перекрывает fetch/transform и save, поэтому wall-clock не равен простой сумме fetch + save.

В `smp=24, memory=28G` получается около `1.17 GB/shard`. Это похоже на удачный режим для данного теста: Scylla чаще сбрасывает меньшие memtables, а tmpfs и bypass-fsync делают эти сбросы дешевыми.

Важно: это test-mode эффект. Он не означает, что один 1000-block range начал полноценно использовать 24 ядра для записи raw data.

## 2. Можно ли настроить быструю запись на 2 shards

Да, но ожидаемый потолок ниже, чем у лучшего `smp=24` warm run.

Режим для честного 2-hot-shard теста:

```text
--smp=2
--memory=4G или 6G
data/commitlog на tmpfs
--developer-mode=1
--overprovisioned
optional: --unsafe-bypass-fsync=1 только для speed test
```

Ожидаемый ориентир из `SHARD.md`:

```text
smp=2, memory=4G: около 19.3 ms/block warm run
```

Практически важные настройки:

- не учитывать `TRUNCATE` во времени теста;
- использовать `UNLOGGED BATCH` примерно по 100 rows/frame;
- не писать непрерывным firehose без пауз, если нужно сравнение с parser behavior;
- держать memory per shard примерно в районе 1-2 GB, не уходить в слишком маленькие shard memory;
- обязательно проверять CQL errors и row counts после теста.

Что, скорее всего, не даст 14-15 ms/block на `smp=2`:

- один только tmpfs;
- увеличение client connections;
- увеличение pipeline;
- bypass-fsync без подходящего flush/memtable режима.

## 3. Почему второй тест быстрее: warmup или дубликаты

Обе причины возможны, поэтому это нужно разделять тестовым протоколом.

### Warmup базы

Второй прогон может быть быстрее из-за нормального прогрева:

- page cache уже содержит mock data и часть файлов Scylla;
- schema/system tables и внутренние структуры уже открыты/созданы;
- prepared/schema caches прогреты;
- allocator/page cache/CPU governor стабилизировались;
- Scylla уже прошла стартовые flush/compaction эффекты.

Это нормальный warm-run эффект.

### Дубликаты

Если второй прогон пишет те же primary keys без очистки, это уже не честный fresh write benchmark.

Scylla `INSERT` является upsert:

- запись не пропускается полностью;
- mutation все равно идет в commitlog/memtable;
- но если те же cells еще в memtable, повторная запись может быть дешевле fresh insert;
- итоговый рост SSTables и давление на flush/compaction могут быть меньше.

Поэтому быстрый второй прогон может быть частично объяснен дубликатами.

## Правильный warm-run протокол

`TRUNCATE` не включать в measured elapsed.

Рекомендуемый порядок:

1. Start Scylla.
2. Apply schema.
3. Сделать warmup.
4. Выполнить `TRUNCATE` всех таблиц вне измеряемого времени.
5. Дождаться, что Scylla не находится под сильной compaction/flush нагрузкой.
6. Seed Redis на block before range.
7. Запустить measured run.
8. Проверить CQL errors и row counts.

Expected counts для текущего 1000-block dump:

| Таблица | Expected |
|---|---:|
| `blocks` | 1000 |
| `transactions` | 269720 |
| `logs` | 772257 |
| `internal_transactions` | 2150513 |
| `contracts` | 2071 |
| `contracts_by_addresses` | около 2058-2071 |

Если warmup делается на том же диапазоне, `TRUNCATE` перед measured run обязателен. Еще лучше делать warmup на другом chunk range, чтобы прогреть базу без риска измерять duplicate upsert behavior.

## Главный вывод

Для production-behavior одного последовательного 1000-block range нужно помнить: raw записи идут только в 2 hot shards. Поэтому `smp=2` является честным режимом для проверки именно этого bottleneck.

Для максимальной скорости на тестовой машине `smp=24, memory=28G, tmpfs, unsafe-bypass-fsync` может быть быстрее за счет внутреннего поведения Scylla, но это уже не означает, что workload распределен по 24 shards.
