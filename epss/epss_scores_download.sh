#!/usr/bin/env bash
# Download today's EPSs CSV.GZ into ./data and write run logs into ./logs
# Usage: ./epss_scores_download.sh [YYYY-MM-DD]

set -euo pipefail

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
DATA_DIR="$SCRIPT_DIR/data"
LOG_DIR="$SCRIPT_DIR/logs"
DATE="${1:-$(date -d 'yesterday' +%F)}"

# Ensure log dir exists (create only if missing) so we can write the log file
if [ -d "$LOG_DIR" ]; then
  CREATED_LOG_DIR=0
else
  mkdir -p "$LOG_DIR"
  CREATED_LOG_DIR=1
fi

TIMESTAMP="$(date +"%Y-%m-%d_%H%M%S")"
LOG_FILE="$LOG_DIR/epss_download_${TIMESTAMP}.log"

# logger: writes only to log file (no stdout/stderr)
log() {
  local level="$1"; shift
  local msg="$*"
  local now
  now="$(date --iso-8601=seconds)"
  printf '%s %s %s\n' "$now" "$level" "$msg" >>"$LOG_FILE"
}

log "INFO" "Script started"
if [ "$CREATED_LOG_DIR" -eq 1 ]; then
  log "INFO" "Created log directory: $LOG_DIR"
else
  log "INFO" "Log directory exists: $LOG_DIR"
fi

log "INFO" "Date requested: $DATE"
log "INFO" "Log file: $LOG_FILE"

# Check data dir existence; create only if missing
if [ -d "$DATA_DIR" ]; then
  log "INFO" "Data directory exists: $DATA_DIR"
else
  mkdir -p "$DATA_DIR"
  log "INFO" "Created data directory: $DATA_DIR"
fi

URL="https://epss.empiricalsecurity.com/epss_scores-${DATE}.csv.gz"
OUT="$DATA_DIR/epss_scores-${DATE}.csv.gz"
TMP="${OUT}.part"

log "INFO" "Target URL: $URL"

if [ -f "$OUT" ]; then
  log "INFO" "Already downloaded: $OUT"
  exit 0
fi

# ensure temporary files are removed on exit
ERR_TMP="$(mktemp --tmpdir epss_err.XXXXXX)" || ERR_TMP="/tmp/epss_err_${TIMESTAMP}.log"
trap 'rm -f "$TMP" "$ERR_TMP"' EXIT

# prefer curl, fallback to wget
if command -v curl >/dev/null 2>&1; then
  log "INFO" "Using curl to download"
    http_code="$(curl -sS --location --write-out "%{http_code}" --output "$TMP" "$URL" 2>"$ERR_TMP")" || curl_exit=$?
  # allow insecure TLS (disable cert verification) if needed
  #http_code="$(curl -sS --insecure --location --write-out "%{http_code}" --output "$TMP" "$URL" 2>"$ERR_TMP")" || curl_exit=$?
  curl_exit=${curl_exit:-0}
  http_code="${http_code##*$'\n'}"
  if [ "$http_code" = "200" ]; then
    mv -f "$TMP" "$OUT"
    log "INFO" "Downloaded and saved to $OUT"

    # wait and unzip to .csv (keep .gz temporarily)
    log "INFO" "Waiting 3s before extracting $OUT"
    sleep 3
    CSV_OUT="${OUT%.gz}"
    if gunzip -c "$OUT" > "$CSV_OUT" 2>>"$ERR_TMP"; then
      log "INFO" "Extracted to $CSV_OUT"
      log "INFO" "csv file unzipped successfully"

      # wait a second, then remove commented rows starting with '#'
      sleep 1
      if sed -i '/^#/d' "$CSV_OUT" 2>>"$ERR_TMP"; then
        log "INFO" "Removed the commented rows from the csv file that started with from the #"
      else
        err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
        log "ERROR" "Failed to remove commented rows from $CSV_OUT. stderr=${err_msg:-<none>}"
        exit 5
      fi

      # move to canonical name
      CANON="$DATA_DIR/epss_scores.csv"

      # If an old canonical file exists remove it and log its creation/modification time
      if [ -f "$CANON" ]; then
        # try to read birth time (%w); if unavailable fallback to modification time (%y)
        created="$(stat -c %w "$CANON" 2>/dev/null || true)"
        if [ -z "$created" ] || [ "$created" = "-" ]; then
          created="$(stat -c %y "$CANON" 2>/dev/null || true)"
        fi

        if rm -f "$CANON" 2>>"$ERR_TMP"; then
          log "INFO" "Removed old epss_scores.csv created at ${created:-<unknown>}"
        else
          err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
          log "WARNING" "Failed to remove old epss_scores.csv. stderr=${err_msg:-<none>}"
        fi
      fi

      if mv -f "$CSV_OUT" "$CANON" 2>>"$ERR_TMP"; then
        log "INFO" "Created $CANON"

        # remove all .csv.gz files so only epss_scores.csv remains
        shopt -s nullglob
        gz_files=("$DATA_DIR"/*.csv.gz)
        for gzf in "${gz_files[@]:-}"; do
          if rm -f "$gzf" 2>>"$ERR_TMP"; then
            log "INFO" "Removed gzip file: $gzf"
          else
            err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
            log "WARNING" "Failed to remove gzip file $gzf. stderr=${err_msg:-<none>}"
          fi
        done

        # clean logs directory: keep only latest 3 log files
        # use ls -1t to get newest first; ignore if no matches
        mapfile -t recent_logs < <(ls -1t "$LOG_DIR"/epss_download_*.log 2>/dev/null || true)
        if [ "${#recent_logs[@]}" -gt 3 ]; then
          # files to remove are from index 3 onward
          for old in "${recent_logs[@]:3}"; do
            if rm -f "$old" 2>>"$ERR_TMP"; then
              log "INFO" "Removed old log file: $old"
            else
              err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
              log "WARNING" "Failed to remove old log file $old. stderr=${err_msg:-<none>}"
            fi
          done
        fi

        log "INFO" "Script finished successfully"
        exit 0
      else
        err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
        log "ERROR" "Failed to create $CANON. stderr=${err_msg:-<none>}"
        exit 6
      fi

    else
      err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
      log "ERROR" "Extraction failed for $OUT. stderr=${err_msg:-<none>}"
      exit 4
    fi

  elif [ "$http_code" = "404" ]; then
    rm -f "$TMP"
    err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
    log "ERROR" "HTTP 404 Not Found for $URL. curl exit=$curl_exit stderr=${err_msg:-<none>}"
    exit 1
  else
    rm -f "$TMP"
    err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
    log "ERROR" "Download failed for $URL. http_code=${http_code:-<none>} curl_exit=${curl_exit:-0} stderr=${err_msg:-<none>}"
    exit 2
  fi

elif command -v wget >/dev/null 2>&1; then
  log "INFO" "curl not found; using wget to download"
  if wget --server-response -O "$TMP" "$URL" 2>"$ERR_TMP"; then
  # wget: skip certificate validation if required
  #if wget --no-check-certificate --server-response -O "$TMP" "$URL" 2>"$ERR_TMP"; then
    mv -f "$TMP" "$OUT"
    log "INFO" "Downloaded and saved to $OUT"

    # wait and unzip to .csv (keep .gz temporarily)
    log "INFO" "Waiting 3s before extracting $OUT"
    sleep 3
    CSV_OUT="${OUT%.gz}"
    if gunzip -c "$OUT" > "$CSV_OUT" 2>>"$ERR_TMP"; then
      log "INFO" "Extracted to $CSV_OUT"
      log "INFO" "csv file unzipped successfully"

      # wait a second, then remove commented rows starting with '#'
      sleep 1
      if sed -i '/^#/d' "$CSV_OUT" 2>>"$ERR_TMP"; then
        log "INFO" "Removed the commented rows from the csv file that started with from the #"
      else
        err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
        log "ERROR" "Failed to remove commented rows from $CSV_OUT. stderr=${err_msg:-<none>}"
        exit 5
      fi

      # move to canonical name
      CANON="$DATA_DIR/epss_scores.csv"

      # If an old canonical file exists remove it and log its creation/modification time
      if [ -f "$CANON" ]; then
        # try to read birth time (%w); if unavailable fallback to modification time (%y)
        created="$(stat -c %w "$CANON" 2>/dev/null || true)"
        if [ -z "$created" ] || [ "$created" = "-" ]; then
          created="$(stat -c %y "$CANON" 2>/dev/null || true)"
        fi

        if rm -f "$CANON" 2>>"$ERR_TMP"; then
          log "INFO" "Removed old epss_scores.csv created at ${created:-<unknown>}"
        else
          err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
          log "WARNING" "Failed to remove old epss_scores.csv. stderr=${err_msg:-<none>}"
        fi
      fi

      if mv -f "$CSV_OUT" "$CANON" 2>>"$ERR_TMP"; then
        log "INFO" "Created $CANON"

        # remove all .csv.gz files so only epss_scores.csv remains
        shopt -s nullglob
        gz_files=("$DATA_DIR"/*.csv.gz)
        for gzf in "${gz_files[@]:-}"; do
          if rm -f "$gzf" 2>>"$ERR_TMP"; then
            log "INFO" "Removed gzip file: $gzf"
          else
            err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
            log "WARNING" "Failed to remove gzip file $gzf. stderr=${err_msg:-<none>}"
          fi
        done

        # clean logs directory: keep only latest 3 log files
        mapfile -t recent_logs < <(ls -1t "$LOG_DIR"/epss_download_*.log 2>/dev/null || true)
        if [ "${#recent_logs[@]}" -gt 3 ]; then
          for old in "${recent_logs[@]:3}"; do
            if rm -f "$old" 2>>"$ERR_TMP"; then
              log "INFO" "Removed old log file: $old"
            else
              err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
              log "WARNING" "Failed to remove old log file $old. stderr=${err_msg:-<none>}"
            fi
          done
        fi

        log "INFO" "Script finished successfully"
        exit 0
      else
        err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
        log "ERROR" "Failed to create $CANON. stderr=${err_msg:-<none>}"
        exit 6
      fi

    else
      err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
      log "ERROR" "Extraction failed for $OUT. stderr=${err_msg:-<none>}"
      exit 4
    fi

  else
    rm -f "$TMP"
    err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
    if printf '%s' "$err_msg" | grep -q ' 404 '; then
      log "ERROR" "HTTP 404 Not Found for $URL. wget stderr=${err_msg:-<none>}"
      exit 1
    else
      log "ERROR" "wget download failed for $URL. wget stderr=${err_msg:-<none>}"
      exit 2
    fi
  fi

else
  log "ERROR" "Neither curl nor wget is available"
  exit 3
fi