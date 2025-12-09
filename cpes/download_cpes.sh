#!/usr/bin/env bash
# Download CPE data from NVD API into ./data and write run logs into ./logs
# Usage: ./download_cpes.sh [test]
#   test: Download only 5 pages for testing (default: download all pages)

set -euo pipefail

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
DATA_DIR="$SCRIPT_DIR/data"
LOG_DIR="$SCRIPT_DIR/logs"
API_URL="https://services.nvd.nist.gov/rest/json/cpes/2.0"

# Determine if test mode
TEST_MODE=0
if [ $# -gt 0 ] && [ "$1" = "test" ]; then
    TEST_MODE=1
fi

# Ensure log dir exists (create only if missing) so we can write the log file
if [ -d "$LOG_DIR" ]; then
    CREATED_LOG_DIR=0
else
    mkdir -p "$LOG_DIR"
    CREATED_LOG_DIR=1
fi

TIMESTAMP="$(date +"%Y-%m-%d_%H%M%S")"
LOG_FILE="$LOG_DIR/cpes_download_${TIMESTAMP}.log"

# logger: writes only to log file (no stdout/stderr)
log() {
    local level="$1"; shift
    local msg="$*"
    local now
    now="$(date --iso-8601=seconds)"
    printf '%s %s %s\n' "$now" "$level" "$msg" >>"$LOG_FILE"
}

SCRIPT_START_TIME=$(date +%s)
SCRIPT_START_TIMESTAMP=$(date --iso-8601=seconds)

log "INFO" "Script started at $SCRIPT_START_TIMESTAMP"
if [ "$TEST_MODE" -eq 1 ]; then
    log "INFO" "Running in TEST mode (5 pages only)"
else
    log "INFO" "Running in FULL mode (all pages)"
fi

if [ "$CREATED_LOG_DIR" -eq 1 ]; then
    log "INFO" "Created log directory: $LOG_DIR"
else
    log "INFO" "Log directory exists: $LOG_DIR"
fi

log "INFO" "Log file: $LOG_FILE"

# Check data dir existence; create only if missing
if [ -d "$DATA_DIR" ]; then
    log "INFO" "Data directory exists: $DATA_DIR"
else
    mkdir -p "$DATA_DIR"
    log "INFO" "Created data directory: $DATA_DIR"
fi

# Check if python3 is available for JSON processing
if ! command -v python3 >/dev/null 2>&1; then
    log "ERROR" "python3 is not installed. Please install python3 to process JSON data"
    exit 3
fi

# Define output files
TEMP_DIR="$DATA_DIR/temp_$$"
CPES_JSON="$DATA_DIR/cpes_${TIMESTAMP}.json"
CPES_TXT="$DATA_DIR/cpes_${TIMESTAMP}.txt"
CPES_CSV="$DATA_DIR/cpes_${TIMESTAMP}.csv"

# Canonical files (latest versions without timestamp)
CANON_JSON="$DATA_DIR/cpes.json"
CANON_TXT="$DATA_DIR/cpes.txt"
CANON_CSV="$DATA_DIR/cpes.csv"

# Ensure temporary files are removed on exit
ERR_TMP="$(mktemp --tmpdir cpes_err.XXXXXX)" || ERR_TMP="/tmp/cpes_err_${TIMESTAMP}.log"
trap 'rm -rf "$TEMP_DIR" "$ERR_TMP"' EXIT

mkdir -p "$TEMP_DIR"
log "INFO" "Created temporary directory: $TEMP_DIR"

# Initialize output files
> "$CPES_TXT"
> "$CPES_JSON"

log "INFO" "Target API: $API_URL"
log "INFO" "Output JSON: $CPES_JSON"
log "INFO" "Output TXT: $CPES_TXT"
log "INFO" "Output CSV: $CPES_CSV"

# Function to download a page with error handling
download_page() {
    local url="$1"
    local output="$2"
    local http_code
    local curl_exit=0
    
    if command -v curl >/dev/null 2>&1; then
        # More aggressive connection settings to prevent resets
        # --tcp-nodelay: Send data immediately without buffering
        # --no-keepalive: Don't try to reuse connections (fresh connection each time)
        # --connect-timeout 20: Shorter connection timeout
        # --max-time 180: Shorter max time (3 minutes)
        http_code="$(curl -sS --location --tcp-nodelay --no-keepalive --connect-timeout 20 --max-time 180 --retry 2 --retry-delay 3 --write-out "%{http_code}" --output "$output" "$url" 2>"$ERR_TMP")" || curl_exit=$?
        curl_exit=${curl_exit:-0}
        http_code="${http_code##*$'\n'}"
        
        if [ "$http_code" = "200" ]; then
            return 0
        elif [ "$http_code" = "404" ]; then
            err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
            log "ERROR" "HTTP 404 Not Found for $url. curl exit=$curl_exit stderr=${err_msg:-<none>}"
            return 1
        else
            err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
            log "ERROR" "Download failed for $url. http_code=${http_code:-<none>} curl_exit=${curl_exit:-0} stderr=${err_msg:-<none>}"
            return 2
        fi
    elif command -v wget >/dev/null 2>&1; then
        if wget --server-response -O "$output" "$url" 2>"$ERR_TMP"; then
            return 0
        else
            err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
            if printf '%s' "$err_msg" | grep -q ' 404 '; then
                log "ERROR" "HTTP 404 Not Found for $url. wget stderr=${err_msg:-<none>}"
                return 1
            else
                log "ERROR" "wget download failed for $url. wget stderr=${err_msg:-<none>}"
                return 2
            fi
        fi
    else
        log "ERROR" "Neither curl nor wget is available"
        return 3
    fi
}

# Step 1: Get total results count
log "INFO" "Fetching initial page to determine total CPE count..."
FIRST_PAGE="$TEMP_DIR/page_0.json"

if ! download_page "${API_URL}?resultsPerPage=1&startIndex=0" "$FIRST_PAGE"; then
    log "ERROR" "Failed to download initial page"
    exit 1
fi

# Extract metadata using python
TOTAL_RESULTS=$(python3 -c "import json; data=json.load(open('$FIRST_PAGE')); print(data.get('totalResults', 0))" 2>>"$ERR_TMP")
FORMAT=$(python3 -c "import json; data=json.load(open('$FIRST_PAGE')); print(data.get('format', ''))" 2>>"$ERR_TMP")
VERSION=$(python3 -c "import json; data=json.load(open('$FIRST_PAGE')); print(data.get('version', ''))" 2>>"$ERR_TMP")
TIMESTAMP_META=$(python3 -c "import json; data=json.load(open('$FIRST_PAGE')); print(data.get('timestamp', ''))" 2>>"$ERR_TMP")

if [ -z "$TOTAL_RESULTS" ] || [ "$TOTAL_RESULTS" = "0" ]; then
    err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
    log "ERROR" "Failed to get total results count from API. stderr=${err_msg:-<none>}"
    exit 1
fi

log "INFO" "Total CPE entries available: $TOTAL_RESULTS"

# Calculate number of pages needed (max 10000 per page)
RESULTS_PER_PAGE=10000
TOTAL_PAGES=$(( (TOTAL_RESULTS + RESULTS_PER_PAGE - 1) / RESULTS_PER_PAGE ))

# Apply test mode limit
if [ "$TEST_MODE" -eq 1 ]; then
    if [ "$TOTAL_PAGES" -gt 5 ]; then
        TOTAL_PAGES=5
        log "INFO" "TEST MODE: Limiting to 5 pages"
    fi
fi

log "INFO" "Number of pages to download: $TOTAL_PAGES"

# Initialize JSON file with metadata (only if starting fresh)
if [ ! -f "$CPES_JSON" ]; then
    cat > "$CPES_JSON" << EOF
{
  "resultsPerPage": $RESULTS_PER_PAGE,
  "startIndex": 0,
  "totalResults": $TOTAL_RESULTS,
  "format": "$FORMAT",
  "version": "$VERSION",
  "timestamp": "$TIMESTAMP_META",
  "products": [
EOF
fi

# Step 2: Download all pages with automatic retry on failure (infinite retry until success)
CHECKPOINT_FILE="$TEMP_DIR/checkpoint.txt"
MAX_RETRIES=5  # Increased retries due to network instability
SLEEP_BETWEEN_REQUESTS=7  # 7 seconds to give more breathing room between requests
FULL_RETRY_COUNT=0
DOWNLOAD_COMPLETE=0

log "INFO" "Starting download process with infinite retry capability"
log "INFO" "Script will keep retrying until all $TOTAL_PAGES pages are successfully downloaded"

while [ $DOWNLOAD_COMPLETE -eq 0 ]; do
    # Check if there's a previous incomplete run to resume from
    START_PAGE=0
    if [ -f "$CHECKPOINT_FILE" ]; then
        START_PAGE=$(cat "$CHECKPOINT_FILE" 2>/dev/null || echo "0")
        if [ $START_PAGE -gt 0 ]; then
            log "INFO" "=== RESUMING DOWNLOAD ==="
            log "INFO" "Resuming from page $((START_PAGE + 1))/$TOTAL_PAGES (Full retry attempt: $((FULL_RETRY_COUNT + 1)))"
            log "INFO" "Already downloaded: $START_PAGE pages"
            log "INFO" "Remaining: $((TOTAL_PAGES - START_PAGE)) pages"
        else
            if [ $FULL_RETRY_COUNT -gt 0 ]; then
                log "INFO" "=== RESTARTING DOWNLOAD ==="
                log "INFO" "Starting from beginning (Full retry attempt: $((FULL_RETRY_COUNT + 1)))"
            fi
        fi
    fi

    CURRENT_INDEX=$((START_PAGE * RESULTS_PER_PAGE))
    TOTAL_DOWNLOADED=0
    DOWNLOAD_FAILED=0

    for ((PAGE=START_PAGE; PAGE<TOTAL_PAGES; PAGE++)); do
    PAGE_FILE="$TEMP_DIR/page_${PAGE}.json"
    RETRY_COUNT=0
    SUCCESS=0
    
    while [ $RETRY_COUNT -lt $MAX_RETRIES ] && [ $SUCCESS -eq 0 ]; do
        log "INFO" "Downloading page $((PAGE + 1))/$TOTAL_PAGES (startIndex: $CURRENT_INDEX, attempt: $((RETRY_COUNT + 1))/$MAX_RETRIES)"
        
        log "INFO" "Checkpoint: Attempting to download page $((PAGE + 1))/$TOTAL_PAGES"
        if download_page "${API_URL}?resultsPerPage=${RESULTS_PER_PAGE}&startIndex=${CURRENT_INDEX}" "$PAGE_FILE"; then
            # Verify the page has valid JSON and products
            PAGE_COUNT=$(python3 -c "import json; data=json.load(open('$PAGE_FILE')); print(len(data.get('products', [])))" 2>>"$ERR_TMP")
            
            if [ -z "$PAGE_COUNT" ] || [ "$PAGE_COUNT" = "0" ]; then
                err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
                log "ERROR" "=== JSON VALIDATION FAILURE ==="
                log "ERROR" "Page: $((PAGE + 1))/$TOTAL_PAGES"
                log "ERROR" "StartIndex: $CURRENT_INDEX"
                log "ERROR" "Reason: Invalid JSON or empty products array"
                log "ERROR" "Details: ${err_msg:-<none>}"
                RETRY_COUNT=$((RETRY_COUNT + 1))
                if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
                    WAIT_TIME=$((3 * RETRY_COUNT))
                    log "INFO" "Retrying in $WAIT_TIME seconds..."
                    sleep $WAIT_TIME
                fi
                continue
            fi
            
            log "INFO" "=== PAGE DOWNLOAD SUCCESS ==="
            log "INFO" "Page $((PAGE + 1))/$TOTAL_PAGES downloaded successfully"
            log "INFO" "Entries in this page: $PAGE_COUNT"
            log "INFO" "StartIndex: $CURRENT_INDEX"
            
            # Append CPE data to output files using python
            log "INFO" "Action: Appending $PAGE_COUNT entries to output files"
            
            python3 - "$PAGE_FILE" "$CPES_JSON" "$CPES_TXT" "$PAGE" "$TOTAL_DOWNLOADED" << 'PYTHON_SCRIPT' 2>>"$ERR_TMP"
import json
import sys

page_file = sys.argv[1]
output_json = sys.argv[2]
output_txt = sys.argv[3]
page_num = int(sys.argv[4])
total_downloaded = int(sys.argv[5])

try:
    with open(page_file, 'r') as f:
        data = json.load(f)
    
    products = data.get('products', [])
    
    # Append to JSON file
    with open(output_json, 'a') as f:
        for i, product in enumerate(products):
            if total_downloaded > 0 or (page_num == 0 and i > 0):
                f.write(',\n    ')
            elif i == 0 and page_num == 0:
                f.write('\n    ')
            else:
                f.write('\n    ')
            json.dump(product, f, separators=(',', ':'))
            total_downloaded += 1
    
    # Append to text file (one CPE per line)
    with open(output_txt, 'a') as f:
        for product in products:
            json.dump(product, f, separators=(',', ':'))
            f.write('\n')
except Exception as e:
    print(f"ERROR: {e}", file=sys.stderr)
    sys.exit(1)
PYTHON_SCRIPT
            
            if [ $? -ne 0 ]; then
                err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
                log "ERROR" "=== PYTHON PROCESSING FAILURE ==="
                log "ERROR" "Page: $((PAGE + 1))/$TOTAL_PAGES"
                log "ERROR" "Action: Failed to append page data to output files"
                log "ERROR" "Details: ${err_msg:-<none>}"
                RETRY_COUNT=$((RETRY_COUNT + 1))
                if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
                    log "INFO" "Retrying in 15 seconds..."
                    sleep 15
                fi
                continue
            fi
            
            TOTAL_DOWNLOADED=$((TOTAL_DOWNLOADED + PAGE_COUNT))
            log "INFO" "Total CPE entries written so far: $TOTAL_DOWNLOADED"
            
            # Save checkpoint for resume capability
            echo "$((PAGE + 1))" > "$CHECKPOINT_FILE"
            log "INFO" "Checkpoint saved: Page $((PAGE + 1))/$TOTAL_PAGES completed"
            
            # Progress tracking
            PROGRESS_PCT=$(( (PAGE + 1) * 100 / TOTAL_PAGES ))
            log "INFO" "Progress: ${PROGRESS_PCT}% complete ($((PAGE + 1))/$TOTAL_PAGES pages)"
            
            SUCCESS=1
        else
            RETRY_COUNT=$((RETRY_COUNT + 1))
            if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
                # Exponential backoff: wait longer on each retry
                WAIT_TIME=$((5 * RETRY_COUNT))
                log "INFO" "Retrying in $WAIT_TIME seconds..."
                sleep $WAIT_TIME
            fi
        fi
    done
    
        if [ $SUCCESS -eq 0 ]; then
            log "ERROR" "=== PAGE DOWNLOAD FAILED ==="
            log "ERROR" "Page: $((PAGE + 1))/$TOTAL_PAGES"
            log "ERROR" "StartIndex: $CURRENT_INDEX"
            log "ERROR" "Reason: Failed after $MAX_RETRIES retry attempts"
            log "ERROR" "Last checkpoint: Page $START_PAGE"
            DOWNLOAD_FAILED=1
            break
        fi
        
        CURRENT_INDEX=$((CURRENT_INDEX + RESULTS_PER_PAGE))
        
        # Rate limiting: wait between requests
        if [ $PAGE -lt $((TOTAL_PAGES - 1)) ]; then
            sleep $SLEEP_BETWEEN_REQUESTS
        fi
    done

    # Check if download completed successfully
    if [ $DOWNLOAD_FAILED -eq 0 ]; then
        DOWNLOAD_END_TIME=$(date +%s)
        DOWNLOAD_DURATION=$((DOWNLOAD_END_TIME - SCRIPT_START_TIME))
        DOWNLOAD_DURATION_MIN=$((DOWNLOAD_DURATION / 60))
        DOWNLOAD_DURATION_SEC=$((DOWNLOAD_DURATION % 60))
        
        log "INFO" "=== DOWNLOAD COMPLETE ==="
        log "INFO" "All $TOTAL_PAGES pages downloaded successfully"
        log "INFO" "Total CPE entries: $TOTAL_DOWNLOADED"
        log "INFO" "Full retry attempts: $FULL_RETRY_COUNT"
        log "INFO" "Download time: ${DOWNLOAD_DURATION_MIN} minutes ${DOWNLOAD_DURATION_SEC} seconds (${DOWNLOAD_DURATION} seconds total)"
        DOWNLOAD_COMPLETE=1
        rm -f "$CHECKPOINT_FILE"  # Remove checkpoint on success
        log "INFO" "Checkpoint file removed (download complete)"
    else
        FULL_RETRY_COUNT=$((FULL_RETRY_COUNT + 1))
        # Fixed 35 second wait to respect API rate limit (5 requests per 30 seconds)
        # 35 seconds ensures we don't hit rate limits when connection re-establishes
        WAIT_TIME=35
        log "WARNING" "=== DOWNLOAD INTERRUPTED ==="
        log "WARNING" "Full retry attempt: $FULL_RETRY_COUNT"
        log "WARNING" "Last successful page: $START_PAGE/$TOTAL_PAGES"
        log "WARNING" "Failed page: $((PAGE + 1))/$TOTAL_PAGES"
        log "WARNING" "Action: Waiting $WAIT_TIME seconds before retrying (respects API rate limit)"
        log "WARNING" "Note: Script will keep retrying indefinitely until all pages are downloaded"
        sleep $WAIT_TIME
        log "INFO" "Retrying download process..."
    fi
done

# Verify download completed before continuing
if [ $DOWNLOAD_COMPLETE -eq 0 ]; then
    log "ERROR" "Download did not complete successfully"
    exit 1
fi

# Close the JSON array and object
cat >> "$CPES_JSON" << EOF

  ]
}
EOF

FILE_SIZE=$(stat -c%s "$CPES_JSON" 2>/dev/null || stat -f%z "$CPES_JSON" 2>/dev/null)
log "INFO" "JSON file saved: $CPES_JSON (${FILE_SIZE} bytes)"

LINE_COUNT=$(wc -l < "$CPES_TXT")
log "INFO" "TXT file saved: $CPES_TXT ($LINE_COUNT entries)"

# Step 3: Wait before CSV conversion
log "INFO" "Waiting 10 seconds before CSV conversion..."
sleep 10

# Step 4: Create CSV file
CSV_START_TIME=$(date +%s)
log "INFO" "Converting CPE data to CSV format"

python3 - "$CPES_TXT" "$CPES_CSV" << 'PYTHON_SCRIPT' 2>>"$ERR_TMP"
import json
import sys
import csv

input_file = sys.argv[1]
output_file = sys.argv[2]

try:
    with open(input_file, 'r') as inf, open(output_file, 'w', newline='') as outf:
        writer = csv.writer(outf, quoting=csv.QUOTE_MINIMAL)
        
        # Write header
        writer.writerow(['deprecated', 'cpeName', 'cpeNameId', 'lastModified', 'created', 'title', 'refs'])
        
        for line in inf:
            try:
                data = json.loads(line.strip())
                cpe = data.get('cpe', {})
                
                # Extract fields
                deprecated = "true" if cpe.get('deprecated', False) else "false"
                cpeName = cpe.get('cpeName', '')
                cpeNameId = cpe.get('cpeNameId', '')
                lastModified = cpe.get('lastModified', '')
                created = cpe.get('created', '')
                
                # Get English title
                title = ''
                titles = cpe.get('titles', [])
                for t in titles:
                    if t.get('lang') == 'en':
                        title = t.get('title', '')
                        break
                
                # Get all refs
                refs = ''
                refs_list = cpe.get('refs', [])
                if refs_list:
                    refs = ' '.join([r.get('ref', '') for r in refs_list])
                
                # Write CSV row with proper escaping
                writer.writerow([deprecated, cpeName, cpeNameId, lastModified, created, title, refs])
            except Exception as e:
                print(f"Error processing line: {e}", file=sys.stderr)
                continue
except Exception as e:
    print(f"ERROR: {e}", file=sys.stderr)
    sys.exit(1)
PYTHON_SCRIPT

if [ $? -eq 0 ]; then
    CSV_END_TIME=$(date +%s)
    CSV_DURATION=$((CSV_END_TIME - CSV_START_TIME))
    CSV_DURATION_MIN=$((CSV_DURATION / 60))
    CSV_DURATION_SEC=$((CSV_DURATION % 60))
    
    CSV_LINE_COUNT=$(($(wc -l < "$CPES_CSV") - 1))
    CSV_SIZE=$(stat -c%s "$CPES_CSV" 2>/dev/null || stat -f%z "$CPES_CSV" 2>/dev/null)
    log "INFO" "CSV file saved: $CPES_CSV ($CSV_LINE_COUNT entries, ${CSV_SIZE} bytes)"
    log "INFO" "CSV creation time: ${CSV_DURATION_MIN} minutes ${CSV_DURATION_SEC} seconds (${CSV_DURATION} seconds total)"
else
    CSV_END_TIME=$(date +%s)
    CSV_DURATION=$((CSV_END_TIME - CSV_START_TIME))
    err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
    log "ERROR" "Failed to create CSV file. stderr=${err_msg:-<none>}"
    log "ERROR" "CSV creation attempted for ${CSV_DURATION} seconds before failure"
    exit 4
fi

# Step 5: Move CSV to canonical name and clean up JSON/TXT files
log "INFO" "Creating canonical CSV file..."

# Move CSV to canonical name
CANON_CSV="$DATA_DIR/cpes.csv"
TIMESTAMPED_CSV="$DATA_DIR/cpes_${TIMESTAMP}.csv"

if [ -f "$CANON_CSV" ]; then
    created="$(stat -c %w "$CANON_CSV" 2>/dev/null || true)"
    if [ -z "$created" ] || [ "$created" = "-" ]; then
        created="$(stat -c %y "$CANON_CSV" 2>/dev/null || true)"
    fi
    
    if rm -f "$CANON_CSV" 2>>"$ERR_TMP"; then
        log "INFO" "Removed old cpes.csv created at ${created:-<unknown>}"
    else
        err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
        log "WARNING" "Failed to remove old cpes.csv. stderr=${err_msg:-<none>}"
    fi
fi

if mv -f "$TIMESTAMPED_CSV" "$CANON_CSV" 2>>"$ERR_TMP"; then
    log "INFO" "Created canonical file: $CANON_CSV"
else
    err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
    log "ERROR" "Failed to create $CANON_CSV. stderr=${err_msg:-<none>}"
    exit 5
fi

# Step 6: Remove JSON and TXT files to save space
log "INFO" "Removing JSON and TXT files to save disk space..."

# Remove timestamped JSON and TXT files
TIMESTAMPED_JSON="$DATA_DIR/cpes_${TIMESTAMP}.json"
TIMESTAMPED_TXT="$DATA_DIR/cpes_${TIMESTAMP}.txt"

if [ -f "$TIMESTAMPED_JSON" ]; then
    if rm -f "$TIMESTAMPED_JSON" 2>>"$ERR_TMP"; then
        log "INFO" "Removed JSON file: $TIMESTAMPED_JSON"
    else
        err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
        log "WARNING" "Failed to remove $TIMESTAMPED_JSON. stderr=${err_msg:-<none>}"
    fi
fi

if [ -f "$TIMESTAMPED_TXT" ]; then
    if rm -f "$TIMESTAMPED_TXT" 2>>"$ERR_TMP"; then
        log "INFO" "Removed TXT file: $TIMESTAMPED_TXT"
    else
        err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
        log "WARNING" "Failed to remove $TIMESTAMPED_TXT. stderr=${err_msg:-<none>}"
    fi
fi

# Remove canonical JSON and TXT files if they exist
CANON_JSON="$DATA_DIR/cpes.json"
CANON_TXT="$DATA_DIR/cpes.txt"

if [ -f "$CANON_JSON" ]; then
    if rm -f "$CANON_JSON" 2>>"$ERR_TMP"; then
        log "INFO" "Removed canonical JSON file: $CANON_JSON"
    else
        err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
        log "WARNING" "Failed to remove $CANON_JSON. stderr=${err_msg:-<none>}"
    fi
fi

if [ -f "$CANON_TXT" ]; then
    if rm -f "$CANON_TXT" 2>>"$ERR_TMP"; then
        log "INFO" "Removed canonical TXT file: $CANON_TXT"
    else
        err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
        log "WARNING" "Failed to remove $CANON_TXT. stderr=${err_msg:-<none>}"
    fi
fi

# Clean up any other old timestamped files (JSON, TXT, CSV)
log "INFO" "Cleaning up any remaining old timestamped files..."
shopt -s nullglob
for ext in json txt csv; do
    old_files=("$DATA_DIR"/cpes_*."$ext")
    for old in "${old_files[@]:-}"; do
        if rm -f "$old" 2>>"$ERR_TMP"; then
            log "INFO" "Removed old timestamped file: $old"
        else
            err_msg="$(cat "$ERR_TMP" 2>/dev/null || true)"
            log "WARNING" "Failed to remove $old. stderr=${err_msg:-<none>}"
        fi
    done
done

# Step 7: Clean logs directory: keep only latest 3 log files
mapfile -t recent_logs < <(ls -1t "$LOG_DIR"/cpes_download_*.log 2>/dev/null || true)
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

SCRIPT_END_TIME=$(date +%s)
SCRIPT_END_TIMESTAMP=$(date --iso-8601=seconds)
TOTAL_DURATION=$((SCRIPT_END_TIME - SCRIPT_START_TIME))
TOTAL_DURATION_HOURS=$((TOTAL_DURATION / 3600))
TOTAL_DURATION_MIN=$(((TOTAL_DURATION % 3600) / 60))
TOTAL_DURATION_SEC=$((TOTAL_DURATION % 60))

log "INFO" "=== SCRIPT EXECUTION SUMMARY ==="
log "INFO" "Script started at: $SCRIPT_START_TIMESTAMP"
log "INFO" "Script ended at: $SCRIPT_END_TIMESTAMP"
log "INFO" "Total execution time: ${TOTAL_DURATION_HOURS}h ${TOTAL_DURATION_MIN}m ${TOTAL_DURATION_SEC}s (${TOTAL_DURATION} seconds total)"
log "INFO" "Download time: ${DOWNLOAD_DURATION_MIN} minutes ${DOWNLOAD_DURATION_SEC} seconds"
log "INFO" "CSV creation time: ${CSV_DURATION_MIN} minutes ${CSV_DURATION_SEC} seconds"
log "INFO" "Total pages downloaded: $TOTAL_PAGES"
log "INFO" "Total CPE entries: $TOTAL_DOWNLOADED"
log "INFO" "Script finished successfully"
exit 0
