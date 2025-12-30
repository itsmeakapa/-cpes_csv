#!/usr/bin/env python3
"""
epss_scores_download.py - Download today's EPSs CSV.GZ, log progress, and output canonical CSV
Replacement for epss_scores_download.sh
"""
import os
import sys
import time
import argparse
import shutil
import gzip
import requests
from datetime import datetime, timedelta

# --- Config ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, 'data')
LOG_DIR = os.path.join(SCRIPT_DIR, 'logs')

# --- Logging ---
def get_timestamp():
    return datetime.now().strftime('%Y-%m-%d_%H%M%S')

def log(msg, level='INFO', log_file=None):
    now = datetime.now().isoformat()
    line = f"{now} {level} {msg}\n"
    if log_file:
        with open(log_file, 'a') as f:
            f.write(line)

# --- Main ---
def main():
    parser = argparse.ArgumentParser(description='Download EPSs CSV.GZ from empiricalsecurity.com')
    parser.add_argument('date', nargs='?', default=None, help='Date in YYYY-MM-DD format (default: yesterday)')
    args = parser.parse_args()
    if args.date:
        date_str = args.date
    else:
        date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)
    timestamp = get_timestamp()
    log_file = os.path.join(LOG_DIR, f'epss_download_{timestamp}.log')
    log(f"Script started", log_file=log_file)
    log(f"Date requested: {date_str}", log_file=log_file)
    log(f"Log file: {log_file}", log_file=log_file)

    url = f"https://epss.empiricalsecurity.com/epss_scores-{date_str}.csv.gz"
    out_gz = os.path.join(DATA_DIR, f'epss_scores-{date_str}.csv.gz')
    tmp_gz = out_gz + '.part'
    log(f"Target URL: {url}", log_file=log_file)

    if os.path.exists(out_gz):
        log(f"Already downloaded: {out_gz}", log_file=log_file)
        sys.exit(0)

    # Download file
    try:
        r = requests.get(url, stream=True, timeout=180)
        if r.status_code == 404:
            log(f"ERROR: HTTP 404 Not Found for {url}", level='ERROR', log_file=log_file)
            sys.exit(1)
        r.raise_for_status()
        with open(tmp_gz, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        shutil.move(tmp_gz, out_gz)
        log(f"Downloaded and saved to {out_gz}", log_file=log_file)
    except Exception as e:
        if os.path.exists(tmp_gz):
            os.remove(tmp_gz)
        log(f"ERROR: Download failed for {url}: {e}", level='ERROR', log_file=log_file)
        sys.exit(2)

    # Wait and unzip to .csv
    log(f"Waiting 3s before extracting {out_gz}", log_file=log_file)
    time.sleep(3)
    csv_out = out_gz[:-3]
    try:
        with gzip.open(out_gz, 'rt') as gz, open(csv_out, 'w') as csvf:
            for line in gz:
                csvf.write(line)
        log(f"Extracted to {csv_out}", log_file=log_file)
        log(f"csv file unzipped successfully", log_file=log_file)
    except Exception as e:
        log(f"ERROR: Extraction failed for {out_gz}: {e}", level='ERROR', log_file=log_file)
        sys.exit(4)

    # Wait a second, then remove commented rows starting with '#'
    time.sleep(1)
    try:
        with open(csv_out, 'r') as f:
            lines = [line for line in f if not line.startswith('#')]
        with open(csv_out, 'w') as f:
            f.writelines(lines)
        log(f"Removed the commented rows from the csv file that started with #", log_file=log_file)
    except Exception as e:
        log(f"ERROR: Failed to remove commented rows from {csv_out}: {e}", level='ERROR', log_file=log_file)
        sys.exit(5)

    # Move to canonical name
    canon = os.path.join(DATA_DIR, 'epss_scores.csv')
    canon_tar_gz = os.path.join(DATA_DIR, 'epss_scores.csv.tar.gz')
    if os.path.exists(canon):
        try:
            os.remove(canon)
            log(f"Removed old epss_scores.csv", log_file=log_file)
        except Exception as e:
            log(f"WARNING: Failed to remove old epss_scores.csv: {e}", level='WARNING', log_file=log_file)
    try:
        shutil.move(csv_out, canon)
        log(f"Created {canon}", log_file=log_file)
    except Exception as e:
        log(f"ERROR: Failed to create {canon}: {e}", level='ERROR', log_file=log_file)
        sys.exit(6)

    # Compress the canonical CSV to tar.gz
    import tarfile
    with tarfile.open(canon_tar_gz, "w:gz") as tar:
        tar.add(canon, arcname=os.path.basename(canon))
    log(f"Compressed CSV to: {canon_tar_gz}", log_file=log_file)

    # Remove the uncompressed canonical CSV
    try:
        os.remove(canon)
        log(f"Removed uncompressed CSV: {canon}", log_file=log_file)
    except Exception:
        log(f"WARNING: Could not remove {canon}", level='WARNING', log_file=log_file)

    # Remove all .csv.gz files so only epss_scores.csv.tar.gz remains
    for fname in os.listdir(DATA_DIR):
        if fname.endswith('.csv.gz'):
            try:
                os.remove(os.path.join(DATA_DIR, fname))
                log(f"Removed gzip file: {fname}", log_file=log_file)
            except Exception as e:
                log(f"WARNING: Failed to remove gzip file {fname}: {e}", level='WARNING', log_file=log_file)

    # Clean logs directory: keep only latest 3 log files
    logs = sorted([f for f in os.listdir(LOG_DIR) if f.startswith('epss_download_') and f.endswith('.log')], reverse=True)
    for old in logs[3:]:
        try:
            os.remove(os.path.join(LOG_DIR, old))
            log(f"Removed old log file: {old}", log_file=log_file)
        except Exception as e:
            log(f"WARNING: Failed to remove old log file {old}: {e}", level='WARNING', log_file=log_file)
    log("Script finished successfully", log_file=log_file)

if __name__ == '__main__':
    main()
