#!/usr/bin/env bash
# Create CSV file from downloaded CISA Vulnrichment JSON files
# Usage: ./create_vulnrichment_csv.sh

set -euo pipefail

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
DATA_DIR="$SCRIPT_DIR/data"
LOG_DIR="$SCRIPT_DIR/logs"
VULNRICHMENT_DIR="$DATA_DIR/vulnrichment_repo"

# Ensure log dir exists
mkdir -p "$LOG_DIR"

TIMESTAMP="$(date +"%Y-%m-%d_%H%M%S")"
LOG_FILE="$LOG_DIR/csv_creation_${TIMESTAMP}.log"

# logger: writes to both log file and stdout
log() {
    local level="$1"; shift
    local msg="$*"
    local now
    now="$(date --iso-8601=seconds)"
    local log_line="$now $level $msg"
    echo "$log_line" | tee -a "$LOG_FILE" >/dev/null
}

CSV_START_TIME=$(date +%s)
CSV_START_TIMESTAMP=$(date --iso-8601=seconds)

log "INFO" "=== CSV CREATION STARTED ==="
log "INFO" "Start time: $CSV_START_TIMESTAMP"
log "INFO" "Source: $VULNRICHMENT_DIR"
log "INFO" "Log file: $LOG_FILE"

# Check if git repository exists
if [ ! -d "$VULNRICHMENT_DIR" ]; then
    log "ERROR" "Git repository not found: $VULNRICHMENT_DIR"
    echo "ERROR: Repository not found: $VULNRICHMENT_DIR"
    echo "Please run ./cisa_vulnrichment_git_download.sh first to sync the repository"
    exit 1
fi

# Verify it's a git repository
if [ ! -d "$VULNRICHMENT_DIR/.git" ]; then
    log "WARNING" "Directory exists but is not a git repository: $VULNRICHMENT_DIR"
fi

# Check if python3 is available
if ! command -v python3 >/dev/null 2>&1; then
    log "ERROR" "python3 is not installed"
    echo "ERROR: python3 is required but not installed"
    exit 3
fi

OUTPUT_CSV="$DATA_DIR/cisa_vulnrichment_${TIMESTAMP}.csv"
CANON_CSV="$DATA_DIR/cisa_vulnrichment.csv"

log "INFO" "Output CSV: $OUTPUT_CSV"

# Create Python script to parse JSON and generate CSV
python3 - "$VULNRICHMENT_DIR" "$OUTPUT_CSV" "$LOG_FILE" << 'PYTHON_SCRIPT'
import json
import os
import sys
import csv
from pathlib import Path
from datetime import datetime

def log_message(log_file, level, message):
    """Write log message to log file"""
    timestamp = datetime.now().isoformat()
    with open(log_file, 'a') as f:
        f.write(f"{timestamp} {level} {message}\n")

def safe_get(data, *keys, default=""):
    """Safely navigate nested dictionary/list structure"""
    result = data
    for key in keys:
        if isinstance(result, dict):
            result = result.get(key, default)
        elif isinstance(result, list) and isinstance(key, int) and len(result) > key:
            result = result[key]
        else:
            return default
        if result == default:
            return default
    return result if result is not None else default

def find_cvss_data(metrics_list):
    """Find CVSS data from metrics list, handling different CVSS versions"""
    if not metrics_list or not isinstance(metrics_list, list):
        return None, None
    
    for metric in metrics_list:
        if not isinstance(metric, dict):
            continue
        # Look for cvssVx_x patterns (cvssV3_1, cvssV3_0, cvssV4_0, etc.)
        for key in metric.keys():
            if key.startswith('cvssV') and '_' in key:
                return key, metric[key]
    return None, None

def find_ssvc_data(metrics_list):
    """Find SSVC data from metrics list"""
    if not metrics_list or not isinstance(metrics_list, list):
        return {}
    
    ssvc_data = {}
    for metric in metrics_list:
        if not isinstance(metric, dict):
            continue
        other = metric.get('other', {})
        if isinstance(other, dict):
            content = other.get('content', {})
            if isinstance(content, dict):
                # Extract SSVC options
                options = content.get('options', [])
                if isinstance(options, list):
                    for option in options:
                        if isinstance(option, dict):
                            for key, value in option.items():
                                if key == 'Exploitation':
                                    ssvc_data['exploitation'] = value
                                elif key == 'Automatable':
                                    ssvc_data['automatable'] = value
                                elif key == 'Technical Impact':
                                    ssvc_data['impact'] = value
                
                # Extract CVE ID if present
                if 'id' in content:
                    ssvc_data['cve_id'] = content['id']
                
                # Check for KEV data
                if other.get('type') == 'kev':
                    ssvc_data['kev_entry'] = 'kev'
                    ssvc_data['kev_date'] = content.get('dateAdded', '')
    
    return ssvc_data

def find_cwe_data(problem_types):
    """Find CWE data from problemTypes list"""
    if not problem_types or not isinstance(problem_types, list):
        return "", ""
    
    for problem_type in problem_types:
        if not isinstance(problem_type, dict):
            continue
        descriptions = problem_type.get('descriptions', [])
        if isinstance(descriptions, list):
            for desc in descriptions:
                if isinstance(desc, dict):
                    cwe_id = desc.get('cweId', '')
                    cwe_desc = desc.get('description', '')
                    if cwe_id:
                        return cwe_id, cwe_desc
    
    return "", ""

def parse_cve_json(file_path):
    """Parse a single CVE JSON file and extract required fields"""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Extract CVE ID from metadata
        cve_id = safe_get(data, 'cveMetadata', 'cveId', default='')
        
        # Initialize result dictionary
        result = {
            'cve_id': cve_id,
            'cisa_cvss_base_score': '',
            'cisa_cvss_base_severity': '',
            'cisa_cvss_vector_string': '',
            'cisa_cvss_version': '',
            'cwe_id': '',
            'cwe_description': '',
            'ssvc_exploitation': '',
            'ssvc_automatable': '',
            'ssvc_impact': '',
            'kev_entry': '',
            'kev_date': '',
            'cna_cvss_base_score': '',
            'cna_cvss_base_severity': '',
            'cna_cvss_vector_string': '',
            'cna_cvss_version': '',
            'cisa_adp': 'CISA ADP',
            'vulnrichment': 'Vulnrichment'
        }
        
        containers = safe_get(data, 'containers', default={})
        
        # Process ADP (CISA) data
        adp_list = safe_get(containers, 'adp', default=[])
        if isinstance(adp_list, list):
            for adp in adp_list:
                if not isinstance(adp, dict):
                    continue
                
                # Get CISA CVSS data
                metrics = adp.get('metrics', [])
                cvss_key, cvss_data = find_cvss_data(metrics)
                if cvss_data:
                    result['cisa_cvss_base_score'] = str(safe_get(cvss_data, 'baseScore', default=''))
                    result['cisa_cvss_base_severity'] = safe_get(cvss_data, 'baseSeverity', default='')
                    result['cisa_cvss_vector_string'] = safe_get(cvss_data, 'vectorString', default='')
                    result['cisa_cvss_version'] = safe_get(cvss_data, 'version', default='')
                
                # Get SSVC data
                ssvc_data = find_ssvc_data(metrics)
                result['ssvc_exploitation'] = ssvc_data.get('exploitation', '')
                result['ssvc_automatable'] = ssvc_data.get('automatable', '')
                result['ssvc_impact'] = ssvc_data.get('impact', '')
                result['kev_entry'] = ssvc_data.get('kev_entry', '')
                result['kev_date'] = ssvc_data.get('kev_date', '')
                
                # Get CWE data from ADP
                problem_types = adp.get('problemTypes', [])
                cwe_id, cwe_desc = find_cwe_data(problem_types)
                if cwe_id:
                    result['cwe_id'] = cwe_id
                    result['cwe_description'] = cwe_desc
        
        # Process CNA data
        cna = safe_get(containers, 'cna', default={})
        if isinstance(cna, dict):
            # Get CNA CVSS data
            metrics = cna.get('metrics', [])
            cvss_key, cvss_data = find_cvss_data(metrics)
            if cvss_data:
                result['cna_cvss_base_score'] = str(safe_get(cvss_data, 'baseScore', default=''))
                result['cna_cvss_base_severity'] = safe_get(cvss_data, 'baseSeverity', default='')
                result['cna_cvss_vector_string'] = safe_get(cvss_data, 'vectorString', default='')
                result['cna_cvss_version'] = safe_get(cvss_data, 'version', default='')
            
            # Get CWE data from CNA if not found in ADP
            if not result['cwe_id']:
                problem_types = cna.get('problemTypes', [])
                cwe_id, cwe_desc = find_cwe_data(problem_types)
                result['cwe_id'] = cwe_id
                result['cwe_description'] = cwe_desc
        
        return result
        
    except Exception as e:
        return {'error': str(e), 'file': file_path}

def main():
    vulnrichment_dir = sys.argv[1]
    output_csv = sys.argv[2]
    log_file = sys.argv[3]
    
    log_message(log_file, "INFO", "Starting CSV generation")
    
    # CSV header
    header = [
        'cve_id',
        'cisa_cvss_base_score',
        'cisa_cvss_base_severity',
        'cisa_cvss_vector_string',
        'cisa_cvss_version',
        'cwe_id',
        'cwe_description',
        'ssvc_exploitation',
        'ssvc_automatable',
        'ssvc_impact',
        'kev_entry',
        'kev_date',
        'cna_cvss_base_score',
        'cna_cvss_base_severity',
        'cna_cvss_vector_string',
        'cna_cvss_version',
        'cisa_adp',
        'vulnrichment'
    ]
    
    # Open output CSV file with csv.writer for proper escaping
    with open(output_csv, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL)
        
        # Write header
        writer.writerow(header)
        
        total_files = 0
        processed_files = 0
        error_files = 0
        
        # Walk through all year directories
        vulnrichment_path = Path(vulnrichment_dir)
        
        for year_dir in sorted(vulnrichment_path.iterdir()):
            if not year_dir.is_dir():
                continue
            
            log_message(log_file, "INFO", f"Processing year: {year_dir.name}")
            
            # Walk through subdirectories
            for subdir in sorted(year_dir.iterdir()):
                if not subdir.is_dir():
                    continue
                
                log_message(log_file, "INFO", f"Processing subdirectory: {year_dir.name}/{subdir.name}")
                
                # Process all JSON files in subdirectory
                json_files = sorted(subdir.glob('CVE-*.json'))
                total_in_subdir = len(json_files)
                
                for json_file in json_files:
                    total_files += 1
                    
                    try:
                        result = parse_cve_json(json_file)
                        
                        if 'error' in result:
                            error_files += 1
                            log_message(log_file, "ERROR", f"Failed to parse {json_file}: {result['error']}")
                            continue
                        
                        # Write CSV row with proper escaping
                        row = [
                            result['cve_id'],
                            result['cisa_cvss_base_score'],
                            result['cisa_cvss_base_severity'],
                            result['cisa_cvss_vector_string'],
                            result['cisa_cvss_version'],
                            result['cwe_id'],
                            result['cwe_description'],
                            result['ssvc_exploitation'],
                            result['ssvc_automatable'],
                            result['ssvc_impact'],
                            result['kev_entry'],
                            result['kev_date'],
                            result['cna_cvss_base_score'],
                            result['cna_cvss_base_severity'],
                            result['cna_cvss_vector_string'],
                            result['cna_cvss_version'],
                            result['cisa_adp'],
                            result['vulnrichment']
                        ]
                        writer.writerow(row)
                        processed_files += 1
                        
                        if processed_files % 100 == 0:
                            log_message(log_file, "INFO", f"Processed {processed_files} files...")
                        
                    except Exception as e:
                        error_files += 1
                        log_message(log_file, "ERROR", f"Error processing {json_file}: {str(e)}")
                
                log_message(log_file, "INFO", f"Completed {year_dir.name}/{subdir.name}: {total_in_subdir} files")
    
    log_message(log_file, "INFO", f"CSV generation complete")
    log_message(log_file, "INFO", f"Total files found: {total_files}")
    log_message(log_file, "INFO", f"Successfully processed: {processed_files}")
    log_message(log_file, "INFO", f"Errors: {error_files}")
    
    print(f"Total files: {total_files}")
    print(f"Processed: {processed_files}")
    print(f"Errors: {error_files}")

if __name__ == '__main__':
    main()
PYTHON_SCRIPT

PYTHON_EXIT=$?

if [ $PYTHON_EXIT -ne 0 ]; then
    log "ERROR" "Python script failed with exit code: $PYTHON_EXIT"
    echo "ERROR: CSV generation failed"
    exit 4
fi

CSV_END_TIME=$(date +%s)
CSV_DURATION=$((CSV_END_TIME - CSV_START_TIME))
CSV_DURATION_MIN=$((CSV_DURATION / 60))
CSV_DURATION_SEC=$((CSV_DURATION % 60))

CSV_SIZE=$(stat -c%s "$OUTPUT_CSV" 2>/dev/null || stat -f%z "$OUTPUT_CSV" 2>/dev/null)
CSV_SIZE_MB=$(echo "scale=2; $CSV_SIZE / 1024 / 1024" | bc 2>/dev/null || echo "?")
CSV_LINE_COUNT=$(($(wc -l < "$OUTPUT_CSV") - 1))

log "INFO" "CSV file generated: $OUTPUT_CSV"
log "INFO" "File size: ${CSV_SIZE} bytes (${CSV_SIZE_MB} MB)"
log "INFO" "Total records: ${CSV_LINE_COUNT}"
log "INFO" "Generation time: ${CSV_DURATION_MIN}m ${CSV_DURATION_SEC}s"

# Move to canonical name
log "INFO" "Updating canonical CSV file..."

if [ -f "$CANON_CSV" ]; then
    OLD_SIZE=$(stat -c%s "$CANON_CSV" 2>/dev/null || stat -f%z "$CANON_CSV" 2>/dev/null || echo "0")
    OLD_LINES=$(($(wc -l < "$CANON_CSV" 2>/dev/null || echo "1") - 1))
    
    log "INFO" "Replacing old CSV (${OLD_LINES} records, ${OLD_SIZE} bytes)"
    rm -f "$CANON_CSV"
fi

if mv "$OUTPUT_CSV" "$CANON_CSV"; then
    log "INFO" "Canonical CSV updated: $CANON_CSV"
    echo "✓ CSV file: $CANON_CSV"
    echo "  Records: $CSV_LINE_COUNT"
    echo "  Size: ${CSV_SIZE_MB} MB"
else
    log "ERROR" "Failed to move CSV to canonical location"
    echo "✗ ERROR: Failed to create $CANON_CSV"
    exit 5
fi

# Clean up old timestamped CSV files
shopt -s nullglob
old_csvs=("$DATA_DIR"/cisa_vulnrichment_*.csv)
if [ ${#old_csvs[@]} -gt 0 ]; then
    log "INFO" "Cleaning up old timestamped CSV files..."
    for old in "${old_csvs[@]}"; do
        rm -f "$old"
        log "INFO" "Removed: $(basename "$old")"
    done
fi

SCRIPT_END_TIME=$(date +%s)
SCRIPT_END_TIMESTAMP=$(date --iso-8601=seconds)
TOTAL_DURATION=$((SCRIPT_END_TIME - CSV_START_TIME))
TOTAL_DURATION_MIN=$((TOTAL_DURATION / 60))
TOTAL_DURATION_SEC=$((TOTAL_DURATION % 60))

log "INFO" "=== CSV CREATION SUMMARY ==="
log "INFO" "Started: $CSV_START_TIMESTAMP"
log "INFO" "Ended: $SCRIPT_END_TIMESTAMP"
log "INFO" "Duration: ${TOTAL_DURATION_MIN}m ${TOTAL_DURATION_SEC}s"
log "INFO" "Source: $VULNRICHMENT_DIR"
log "INFO" "Output: $CANON_CSV"
log "INFO" "Records: $CSV_LINE_COUNT"
log "INFO" "=== CSV CREATION COMPLETED SUCCESSFULLY ==="

# Clean logs directory after successful completion: keep only latest 3 log files
log "INFO" "Rotating log files (keeping latest 3)..."
mapfile -t all_logs < <(ls -1t "$LOG_DIR"/csv_creation_*.log 2>/dev/null || true)
if [ "${#all_logs[@]}" -gt 3 ]; then
    deleted_count=0
    for old in "${all_logs[@]:3}"; do
        rm -f "$old"
        deleted_count=$((deleted_count + 1))
    done
    log "INFO" "Removed $deleted_count old log file(s)"
    echo "Cleaned up $deleted_count old log file(s)"
fi

echo "✓ Completed in ${TOTAL_DURATION_MIN}m ${TOTAL_DURATION_SEC}s"

exit 0
