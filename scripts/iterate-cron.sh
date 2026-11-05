#!/usr/bin/env bash
# Runner для cron-задачи iterate-tunshell-mcp (планировщик remote-agents).
# Запускает одну итерацию рекурсивного улучшения проекта по docs/ITERATE.md.
#
# Отличия от старой команды (opencode -p "..." --cwd ...):
#   1. opencode v2: флага --cwd больше нет — рабочий каталог задаём через cd,
#      неинтерактивный запуск — через подкоманду `run` (+ --auto для разрешений).
#   2. Порядок редиректа исправлен: старое `2>&1 >> log` теряло stderr.
#      Здесь весь вывод пишется изнутри скрипта: >> "$LOG" 2>&1.
#   3. flock: у КАЖДОГО инстанса remote-agent демона свой планировщик (Scheduler::load
#      в state.rs), поэтому cron-слот может сработать дважды. Кроме того, итерация
#      может длиться дольше 30 минут. Блокировка решает обе проблемы.
#
# Использование: iterate-cron.sh ["переопределение промпта"]

set -u

PROJ="/home/ojo/dev/tunshell_mcp_agents"
LOG="/tmp/iterate.log"
LOCK="/tmp/iterate-tunshell-mcp.lock"
OPENCODE="/home/ojo/.opencode/bin/opencode"
PROMPT="${1:-Выполни одну итерацию согласно docs/ITERATE.md}"

stamp() { date '+%Y-%m-%d %H:%M:%S'; }

# --- единственный экземпляр (дубли планировщиков / долгие итерации) ---
exec 9>"$LOCK"
if ! flock -n 9; then
  echo "[$(stamp)] SKIP: предыдущая итерация ещё выполняется" >> "$LOG"
  exit 0
fi

echo "[$(stamp)] START iterate-tunshell-mcp" >> "$LOG"

if ! cd "$PROJ" 2>>"$LOG"; then
  echo "[$(stamp)] ERROR: не найден каталог проекта $PROJ" >> "$LOG"
  exit 1
fi

"$OPENCODE" run --auto --title "iterate-tunshell-mcp" --model "openrouter/z-ai/glm-5.3" "$PROMPT" >> "$LOG" 2>&1
rc=$?

echo "[$(stamp)] DONE rc=$rc" >> "$LOG"
exit $rc
