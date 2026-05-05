#!/usr/bin/env bash
set -Eeuo pipefail

REPO_DIR="${REPO_DIR:-/root/obscura}"
DATA_DIR="${DATA_DIR:-$REPO_DIR/data/b2b}"
SITE_DIR="${SITE_DIR:-$DATA_DIR/site}"
PORT="${PORT:-8080}"
INTERVAL_SECONDS="${INTERVAL_SECONDS:-18000}"
HEALTH_INTERVAL_SECONDS="${HEALTH_INTERVAL_SECONDS:-60}"
FALLBACK_MODEL="${FALLBACK_MODEL:-gpt-5.3-codex-spark}"
RUN_EXPORT_ON_START="${RUN_EXPORT_ON_START:-0}"
TAKE_OWNERSHIP="${TAKE_OWNERSHIP:-1}"

LOG_DIR="$DATA_DIR/logs"
LOG_FILE="$LOG_DIR/fallback-orchestrator.log"
SERVER_LOG="$LOG_DIR/static-site-server.log"
PID_FILE="$DATA_DIR/site-server-${PORT}.pid"
STATE_FILE="$DATA_DIR/fallback-orchestrator-state.json"
HANDOFF_FILE="$DATA_DIR/FALLBACK_HANDOFF.md"
LOCK_DIR="$DATA_DIR/.fallback-export.lock"

mkdir -p "$LOG_DIR"

now_epoch() {
  date +%s
}

iso_now() {
  date -u +%FT%TZ
}

iso_from_epoch() {
  date -u -d "@$1" +%FT%TZ
}

log() {
  local message="$1"
  printf '[%s] %s\n' "$(iso_now)" "$message" | tee -a "$LOG_FILE"
}

company_json_count() {
  if [[ -d "$DATA_DIR/directory/companies" ]]; then
    find "$DATA_DIR/directory/companies" -type f -name '*.json' | wc -l
  else
    printf '0\n'
  fi
}

company_html_count() {
  if [[ -d "$SITE_DIR/companies" ]]; then
    find "$SITE_DIR/companies" -type f -name '*.html' | wc -l
  else
    printf '0\n'
  fi
}

profile_line_count() {
  if [[ -f "$DATA_DIR/company_profiles.jsonl" ]]; then
    wc -l < "$DATA_DIR/company_profiles.jsonl"
  else
    printf '0\n'
  fi
}

write_state() {
  local status="$1"
  local next_export_epoch="$2"
  cat > "$STATE_FILE" <<EOF
{
  "updated_at": "$(iso_now)",
  "status": "$status",
  "fallback_model": "$FALLBACK_MODEL",
  "port": $PORT,
  "health_interval_seconds": $HEALTH_INTERVAL_SECONDS,
  "export_interval_seconds": $INTERVAL_SECONDS,
  "next_export_at_epoch": $next_export_epoch,
  "next_export_at_utc": "$(iso_from_epoch "$next_export_epoch")",
  "profile_jsonl_lines": $(profile_line_count),
  "directory_company_json_files": $(company_json_count),
  "site_company_html_files": $(company_html_count)
}
EOF
}

write_handoff() {
  local status="$1"
  local next_export_epoch="$2"
  cat > "$HANDOFF_FILE" <<EOF
# B2B Fallback Handoff

Updated: $(iso_now)
Status: $status
Repository: $REPO_DIR
Data directory: $DATA_DIR
Site URL: http://127.0.0.1:$PORT/
Fallback model requested: $FALLBACK_MODEL
Next scheduled export/restart: $(iso_from_epoch "$next_export_epoch")

## Current Counts

- JSONL profiles: $(profile_line_count)
- Directory JSON profiles: $(company_json_count)
- Static HTML profiles: $(company_html_count)

## Operating Notes

- This watchdog keeps the local static B2B website available.
- Every $INTERVAL_SECONDS seconds it reruns the export and restarts the static server.
- zellij cannot detect or recover this chat's token usage. It has no access to the parent conversation token counter.
- If the main chat stops, attach to the zellij session and continue from this handoff.
- GLEIF pilot data is identity/address baseline data. It is not campaign-ready contact/catalog enrichment.

## Useful Commands

Attach to the monitor:

\`\`\`sh
zellij attach b2b-fallback-orchestrator
\`\`\`

Watch logs:

\`\`\`sh
tail -f "$LOG_FILE"
\`\`\`

Run a manual export:

\`\`\`sh
cd "$REPO_DIR"
cargo run -p obscura-b2b -- export --out "$DATA_DIR"
\`\`\`

Start a manual Spark fallback from Codex CLI:

\`\`\`sh
codex --model "$FALLBACK_MODEL" --cd "$REPO_DIR" --sandbox danger-full-access --ask-for-approval never "Continue B2B orchestration from $HANDOFF_FILE. First read docs/b2b-gleif-10k-pilot.md and check git status."
\`\`\`
EOF
}

matching_site_pids() {
  pgrep -f "[p]ython3 -m http.server $PORT --directory $SITE_DIR" || true
}

site_healthy() {
  curl -fsS --max-time 5 "http://127.0.0.1:$PORT/" >/dev/null
}

stop_site_server() {
  local pids
  pids="$(matching_site_pids)"
  if [[ -n "$pids" ]]; then
    log "stopping static site server pids: $pids"
    # shellcheck disable=SC2086
    kill $pids 2>/dev/null || true
    sleep 1
    # shellcheck disable=SC2086
    kill -9 $pids 2>/dev/null || true
  fi
  rm -f "$PID_FILE"
}

start_site_server() {
  mkdir -p "$SITE_DIR"
  log "starting static site server on port $PORT"
  nohup python3 -m http.server "$PORT" --directory "$SITE_DIR" >> "$SERVER_LOG" 2>&1 &
  printf '%s\n' "$!" > "$PID_FILE"
  sleep 1
}

ensure_site_server() {
  if site_healthy; then
    return 0
  fi

  log "site health check failed; restarting static server"
  stop_site_server
  start_site_server

  if site_healthy; then
    log "site health check recovered"
    return 0
  fi

  log "site health check still failing after restart"
  return 1
}

run_export() {
  if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    log "export already running; skipping this cycle"
    return 0
  fi
  trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' RETURN

  log "running B2B export"
  (
    cd "$REPO_DIR"
    if [[ -f /root/.cargo/env ]]; then
      # shellcheck disable=SC1091
      . /root/.cargo/env
    fi
    cargo run -p obscura-b2b -- export --out "$DATA_DIR"
  ) >> "$LOG_FILE" 2>&1
  log "B2B export finished"
}

main() {
  local next_export_epoch
  next_export_epoch="$(( $(now_epoch) + INTERVAL_SECONDS ))"

  log "fallback watchdog starting with model marker $FALLBACK_MODEL"
  write_state "starting" "$next_export_epoch"
  write_handoff "starting" "$next_export_epoch"

  if [[ "$TAKE_OWNERSHIP" == "1" ]]; then
    stop_site_server
    start_site_server
  fi

  if [[ "$RUN_EXPORT_ON_START" == "1" || ! -f "$SITE_DIR/index.html" ]]; then
    run_export
    stop_site_server
    start_site_server
  fi

  while true; do
    local now
    now="$(now_epoch)"

    if (( now >= next_export_epoch )); then
      write_state "exporting" "$next_export_epoch"
      write_handoff "exporting" "$next_export_epoch"
      if run_export; then
        stop_site_server
        start_site_server
      fi
      next_export_epoch="$(( $(now_epoch) + INTERVAL_SECONDS ))"
    fi

    if ensure_site_server; then
      write_state "healthy" "$next_export_epoch"
      write_handoff "healthy" "$next_export_epoch"
    else
      write_state "site_unhealthy" "$next_export_epoch"
      write_handoff "site_unhealthy" "$next_export_epoch"
    fi

    sleep "$HEALTH_INTERVAL_SECONDS"
  done
}

main "$@"
