#!/usr/bin/env bash
# Синхронизирует файлы из master в ветку test и пушит.
# Используется для обновления PR test -> dev перед ревью.
set -euo pipefail

MASTER="master"
TEST="test"

# Сохраняем текущую ветку, чтобы вернуться после
CURRENT=$(git symbolic-ref --short HEAD 2>/dev/null || echo "detached")

git checkout "$TEST"

# Копируем все файлы из master в рабочий каталог
git checkout "$MASTER" -- .

# Если нет изменений — выходим
if git diff --cached --quiet; then
    echo "Нет изменений относительно master. Всё актуально."
    git checkout "$CURRENT"
    exit 0
fi

# Коммит со списком последних сообщений из master
MASTER_LOG=$(git log "$TEST"..origin/"$MASTER" --oneline 2>/dev/null \
             || git log "$TEST".."$MASTER" --oneline)

git commit -m "$(printf 'sync from master\n\n%s' "$MASTER_LOG")"

git push origin "$TEST"

git checkout "$CURRENT"
echo "Готово. Ветка $TEST обновлена и запушена."
