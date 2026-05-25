#!/usr/bin/env bash
# Переносит правки из ветки test обратно в master.
# Обновляет только файлы, которые уже есть в master — test.txt и прочее
# из dev-ветки в master не попадает.
set -euo pipefail

MASTER="master"
TEST="test"

CURRENT=$(git symbolic-ref --short HEAD 2>/dev/null || echo "detached")

git checkout "$MASTER"

# Обновляем только файлы, которые уже отслеживаются в master.
# git checkout test -- <file> берёт версию файла из test.
while IFS= read -r file; do
    git checkout "$TEST" -- "$file" 2>/dev/null || true
done < <(git ls-files)

# Если нет изменений — выходим
if git diff --cached --quiet; then
    echo "Нет правок относительно $TEST. master актуален."
    git checkout "$CURRENT"
    exit 0
fi

echo "Изменённые файлы:"
git diff --cached --name-only

# Коммит
git commit -m "apply review fixes from test"

git push origin "$MASTER"

git checkout "$CURRENT"
echo "Готово. master обновлён и запушен."
