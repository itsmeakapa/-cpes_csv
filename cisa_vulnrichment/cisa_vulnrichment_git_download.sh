#!/usr/bin/env bash
# CISA Vulnrichment Git Downloader
# This script maintains a local git repository synchronized with GitHub
# Ensures local repo is always in sync with origin (recovers deleted files)
# CSV generation script works directly on the git repo folder
# Usage: ./cisa_vulnrichment_git_download.sh
#   No arguments needed - always syncs and updates CSV

set -euo pipefail

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
DATA_DIR="$SCRIPT_DIR/data"
LOG_DIR="$SCRIPT_DIR/logs"
REPO_URL="https://github.com/cisagov/vulnrichment.git"
BRANCH="develop"
GIT_REPO_DIR="$DATA_DIR/vulnrichment_repo"

# Create directories
mkdir -p "$LOG_DIR"
mkdir -p "$DATA_DIR"

TIMESTAMP="$(date +"%Y-%m-%d_%H%M%S")"
LOG_FILE="$LOG_DIR/vulnrichment_git_download_${TIMESTAMP}.log"
DOWNLOAD_DIR="$DATA_DIR/vulnrichment_files"
TEMP_CLONE_DIR="$DATA_DIR/temp_clone_$$"

# Ensure temporary directory is removed on exit
trap 'rm -rf "$TEMP_CLONE_DIR"' EXIT

# Logger function
log() {
    local level="$1"; shift
    local msg="$*"
    local now="$(date --iso-8601=seconds)"
    printf '%s %s %s\n' "$now" "$level" "$msg" | tee -a "$LOG_FILE"
}

SCRIPT_START_TIME=$(date +%s)
log "INFO" "=== CISA VULNRICHMENT GIT SYNC STARTED ==="
log "INFO" "Repository: $REPO_URL"
log "INFO" "Branch: $BRANCH"
log "INFO" "Local path: $GIT_REPO_DIR"
log "INFO" "Log file: $LOG_FILE"

# Check if git is available
if ! command -v git >/dev/null 2>&1; then
    log "ERROR" "git is required but not installed"
    exit 1
fi

# Test connectivity to GitHub
log "INFO" "Testing connectivity to GitHub..."
if curl -sS --connect-timeout 10 --max-time 15 -I "https://github.com" >/dev/null 2>&1; then
    log "INFO" "GitHub connectivity test: SUCCESS"
else
    log "ERROR" "Cannot connect to github.com"
    log "ERROR" "Please check your internet connection and firewall settings"
    exit 1
fi

if [ -d "$GIT_REPO_DIR/.git" ]; then
    log "INFO" "Local repository exists - syncing with origin"
    echo "Syncing repository with origin..."
    
    cd "$GIT_REPO_DIR"
    
    # Fetch latest from origin
    log "INFO" "Fetching latest changes from origin..."
    if ! git fetch origin "$BRANCH" 2>&1 | tee -a "$LOG_FILE"; then
        log "ERROR" "Failed to fetch from origin"
        cd "$SCRIPT_DIR"
        exit 1
    fi
    
    # Count changes before reset
    CHANGES_DETECTED=0
    if git diff --name-only HEAD "origin/$BRANCH" &>/dev/null; then
        CHANGED_FILES=$(git diff --name-only HEAD "origin/$BRANCH" 2>/dev/null | grep "\.json$" | wc -l || echo "0")
        if [ "$CHANGED_FILES" -gt 0 ]; then
            CHANGES_DETECTED=1
            log "INFO" "Detected $CHANGED_FILES changed CVE files"
            echo "Found $CHANGED_FILES file changes to sync"
        fi
    fi
    
    # Hard reset to match origin exactly (recovers any deleted files)
    log "INFO" "Resetting local repo to match origin (ensures full sync)..."
    if ! git reset --hard "origin/$BRANCH" 2>&1 | tee -a "$LOG_FILE"; then
        log "ERROR" "Failed to reset to origin/$BRANCH"
        cd "$SCRIPT_DIR"
        exit 1
    fi
    
    # Clean any untracked files/directories
    log "INFO" "Cleaning untracked files..."
    git clean -fd 2>&1 | tee -a "$LOG_FILE"
    
    if [ $CHANGES_DETECTED -eq 1 ]; then
        log "INFO" "Repository synced successfully - $CHANGED_FILES files updated"
        echo "✓ Sync complete: $CHANGED_FILES files updated"
    else
        log "INFO" "Repository already up to date - no changes"
        echo "✓ Repository is up to date"
    fi
    
    cd "$SCRIPT_DIR"
else
    log "INFO" "Local repository not found - performing initial clone"
    echo "Cloning repository (first time - this may take 2-3 minutes)..."
    
    # Clone with depth 1 (only latest commit) for speed
    if git clone --depth 1 --branch "$BRANCH" "$REPO_URL" "$GIT_REPO_DIR" 2>&1 | tee -a "$LOG_FILE"; then
        log "INFO" "Initial clone completed successfully"
        echo "✓ Clone complete"
    else
        log "ERROR" "Failed to clone repository"
        exit 1
    fi
fi

# Verify repository contents
log "INFO" "Verifying repository contents..."
TOTAL_FILES=$(find "$GIT_REPO_DIR" -type f -name "*.json" -path "*/[12][09][0-9][0-9]/*" 2>/dev/null | wc -l)
log "INFO" "Total CVE JSON files: $TOTAL_FILES"

# Show summary by year
YEAR_SUMMARY=""
for year_dir in "$GIT_REPO_DIR"/[12][09][0-9][0-9]; do
    if [ -d "$year_dir" ]; then
        year=$(basename "$year_dir")
        count=$(find "$year_dir" -type f -name "*.json" 2>/dev/null | wc -l)
        log "INFO" "  $year: $count files"
        YEAR_SUMMARY="$YEAR_SUMMARY$year($count) "
    fi
done

log "INFO" "Repository verification complete"
echo "Repository contains $TOTAL_FILES CVE files"

SCRIPT_END_TIME=$(date +%s)
DURATION=$((SCRIPT_END_TIME - SCRIPT_START_TIME))
DURATION_MIN=$((DURATION / 60))
DURATION_SEC=$((DURATION % 60))

log "INFO" "=== GIT SYNC SUMMARY ==="
log "INFO" "Repository path: $GIT_REPO_DIR"
log "INFO" "Total CVE files: $TOTAL_FILES"
log "INFO" "Sync duration: ${DURATION_MIN}m ${DURATION_SEC}s"

# Generate CSV from the git repository
CSV_SCRIPT="$SCRIPT_DIR/create_vulnrichment_csv.sh"
if [ -f "$CSV_SCRIPT" ]; then
    log "INFO" "Starting CSV generation from repository..."
    echo "Generating CSV file..."
    
    # Run CSV generation script
    if bash "$CSV_SCRIPT" 2>&1 | tee -a "$LOG_FILE"; then
        log "INFO" "CSV generation completed successfully"
        
        # Show CSV info if it exists 
        CSV_FILE="$DATA_DIR/cisa_vulnrichment.csv"
        if [ -f "$CSV_FILE" ]; then
            CSV_LINES=$(wc -l < "$CSV_FILE")
            CSV_SIZE=$(du -h "$CSV_FILE" | cut -f1)
            log "INFO" "CSV file: $CSV_FILE"
            log "INFO" "CSV records: $((CSV_LINES - 1)) (excluding header)"
            log "INFO" "CSV size: $CSV_SIZE"
            echo "✓ CSV created: $((CSV_LINES - 1)) records, $CSV_SIZE"
        fi
    else
        log "ERROR" "CSV generation failed"
        echo "✗ CSV generation failed - check logs"
    fi
else
    log "WARNING" "CSV script not found: $CSV_SCRIPT"
    echo "⚠ CSV script not found - skipping CSV generation"
fi

# Clean old logs (keep latest 5)
log "INFO" "Rotating old log files..."
mapfile -t old_logs < <(ls -1t "$LOG_DIR"/vulnrichment_git_download_*.log 2>/dev/null | tail -n +6)
if [ ${#old_logs[@]} -gt 0 ]; then
    for old_log in "${old_logs[@]}"; do
        rm -f "$old_log"
        log "INFO" "Removed old log: $(basename "$old_log")"
    done
    echo "Cleaned up ${#old_logs[@]} old log file(s)"
fi

log "INFO" "=== SCRIPT COMPLETED SUCCESSFULLY ==="
echo "✓ All done! Repository synced and CSV updated."
exit 0
