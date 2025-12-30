#!/usr/bin/env python3
"""
download_cpes.py - Download CPE data from NVD API, log progress, and output CSV
Replacement for download_cpes.sh
"""
import os
import sys
import time
import json
import csv
import argparse
import shutil
import tempfile
import requests
from datetime import datetime

# --- Config ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, 'data')
LOG_DIR = os.path.join(SCRIPT_DIR, 'logs')
API_URL = 'https://services.nvd.nist.gov/rest/json/cpes/2.0'
RESULTS_PER_PAGE = 10000
MAX_RETRIES = 5
SLEEP_BETWEEN_REQUESTS = 7

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
    parser = argparse.ArgumentParser(description='Download CPE data from NVD API')
    parser.add_argument('mode', nargs='?', default='', help='test: Download only 5 pages')
    args = parser.parse_args()
    test_mode = args.mode == 'test'

    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)
    timestamp = get_timestamp()
    log_file = os.path.join(LOG_DIR, f'cpes_download_{timestamp}.log')
    log(f"Script started at {timestamp}", log_file=log_file)
    log(f"Running in {'TEST' if test_mode else 'FULL'} mode", log_file=log_file)
    log(f"Log file: {log_file}", log_file=log_file)

    # Output files
    cpes_json = os.path.join(DATA_DIR, f'cpes_{timestamp}.json')
    cpes_txt = os.path.join(DATA_DIR, f'cpes_{timestamp}.txt')
    cpes_csv = os.path.join(DATA_DIR, f'cpes_{timestamp}.csv')
    canon_json = os.path.join(DATA_DIR, 'cpes.json')
    canon_txt = os.path.join(DATA_DIR, 'cpes.txt')
    canon_csv = os.path.join(DATA_DIR, 'cpes.csv')
    canon_tar_gz = os.path.join(DATA_DIR, 'cpes.csv.tar.gz')

    # Step 1: Get total results
    log("Fetching initial page to determine total CPE count...", log_file=log_file)
    try:
        r = requests.get(f"{API_URL}?resultsPerPage=1&startIndex=0", timeout=30)
        r.raise_for_status()
        meta = r.json()
    except Exception as e:
        log(f"ERROR: Failed to fetch initial page: {e}", level='ERROR', log_file=log_file)
        sys.exit(1)
    total_results = meta.get('totalResults', 0)
    format_ = meta.get('format', '')
    version = meta.get('version', '')
    timestamp_meta = meta.get('timestamp', '')
    if not total_results:
        log("ERROR: Failed to get total results count from API", level='ERROR', log_file=log_file)
        sys.exit(1)
    log(f"Total CPE entries available: {total_results}", log_file=log_file)
    total_pages = (total_results + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE
    if test_mode and total_pages > 5:
        total_pages = 5
        log("TEST MODE: Limiting to 5 pages", log_file=log_file)
    log(f"Number of pages to download: {total_pages}", log_file=log_file)

    # Step 2: Download all pages with retry
    temp_dir = tempfile.mkdtemp(dir=DATA_DIR)
    checkpoint_file = os.path.join(temp_dir, 'checkpoint.txt')
    start_page = 0
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file) as f:
            try:
                start_page = int(f.read().strip())
            except Exception:
                start_page = 0
    total_downloaded = 0
    products = []
    for page in range(start_page, total_pages):
        page_file = os.path.join(temp_dir, f'page_{page}.json')
        current_index = page * RESULTS_PER_PAGE
        for attempt in range(MAX_RETRIES):
            try:
                log(f"Downloading page {page+1}/{total_pages} (startIndex: {current_index}, attempt: {attempt+1}/{MAX_RETRIES})", log_file=log_file)
                r = requests.get(f"{API_URL}?resultsPerPage={RESULTS_PER_PAGE}&startIndex={current_index}", timeout=180)
                r.raise_for_status()
                data = r.json()
                page_products = data.get('products', [])
                if not page_products:
                    raise ValueError('No products in page')
                with open(page_file, 'w') as f:
                    json.dump(data, f)
                products.extend(page_products)
                total_downloaded += len(page_products)
                log(f"Page {page+1}/{total_pages} downloaded successfully, entries: {len(page_products)}", log_file=log_file)
                with open(checkpoint_file, 'w') as f:
                    f.write(str(page+1))
                break
            except Exception as e:
                log(f"ERROR: Download failed for page {page+1}: {e}", level='ERROR', log_file=log_file)
                if attempt < MAX_RETRIES - 1:
                    wait = 5 * (attempt + 1)
                    log(f"Retrying in {wait} seconds...", log_file=log_file)
                    time.sleep(wait)
                else:
                    log(f"ERROR: Failed after {MAX_RETRIES} attempts", level='ERROR', log_file=log_file)
                    shutil.rmtree(temp_dir)
                    sys.exit(2)
        time.sleep(SLEEP_BETWEEN_REQUESTS)
    # Write JSON
    with open(cpes_json, 'w') as f:
        json.dump({
            'resultsPerPage': RESULTS_PER_PAGE,
            'startIndex': 0,
            'totalResults': total_results,
            'format': format_,
            'version': version,
            'timestamp': timestamp_meta,
            'products': products
        }, f)
    log(f"JSON file saved: {cpes_json} ({os.path.getsize(cpes_json)} bytes)", log_file=log_file)
    # Write TXT
    with open(cpes_txt, 'w') as f:
        for prod in products:
            f.write(json.dumps(prod) + '\n')
    log(f"TXT file saved: {cpes_txt} ({len(products)} entries)", log_file=log_file)
    # Step 3: Wait before CSV conversion
    log("Waiting 10 seconds before CSV conversion...", log_file=log_file)
    time.sleep(10)
    # Step 4: Create CSV
    log("Converting CPE data to CSV format", log_file=log_file)
    with open(cpes_csv, 'w', newline='') as outf:
        writer = csv.writer(outf, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['deprecated', 'cpeName', 'cpeNameId', 'lastModified', 'created', 'title', 'refs'])
        for prod in products:
            cpe = prod.get('cpe', {})
            deprecated = 'true' if cpe.get('deprecated', False) else 'false'
            cpeName = cpe.get('cpeName', '')
            cpeNameId = cpe.get('cpeNameId', '')
            lastModified = cpe.get('lastModified', '')
            created = cpe.get('created', '')
            title = ''
            for t in cpe.get('titles', []):
                if t.get('lang') == 'en':
                    title = t.get('title', '')
                    break
            refs = ' '.join([r.get('ref', '') for r in cpe.get('refs', [])])
            writer.writerow([deprecated, cpeName, cpeNameId, lastModified, created, title, refs])
    log(f"CSV file saved: {cpes_csv} ({os.path.getsize(cpes_csv)} bytes)", log_file=log_file)
    # Step 5: Move CSV to canonical name
    if os.path.exists(canon_csv):
        os.remove(canon_csv)
    shutil.move(cpes_csv, canon_csv)
    log(f"Created canonical file: {canon_csv}", log_file=log_file)

    # Step 6: Compress the canonical CSV to tar.gz
    import tarfile
    with tarfile.open(canon_tar_gz, "w:gz") as tar:
        tar.add(canon_csv, arcname=os.path.basename(canon_csv))
    log(f"Compressed CSV to: {canon_tar_gz}", log_file=log_file)

    # Remove the uncompressed canonical CSV
    try:
        os.remove(canon_csv)
        log(f"Removed uncompressed CSV: {canon_csv}", log_file=log_file)
    except Exception:
        log(f"WARNING: Could not remove {canon_csv}", level='WARNING', log_file=log_file)

    # Step 7: Remove JSON and TXT files to save space
    for fpath in [cpes_json, cpes_txt, canon_json, canon_txt]:
        if os.path.exists(fpath):
            os.remove(fpath)
    # Step 8: Clean up old timestamped files
    for ext in ['json', 'txt', 'csv']:
        for old in os.listdir(DATA_DIR):
            if old.startswith('cpes_') and old.endswith('.' + ext):
                try:
                    os.remove(os.path.join(DATA_DIR, old))
                except Exception:
                    pass
    # Step 9: Clean logs directory: keep only latest 3 log files
    logs = sorted([f for f in os.listdir(LOG_DIR) if f.startswith('cpes_download_') and f.endswith('.log')], reverse=True)
    for old in logs[3:]:
        try:
            os.remove(os.path.join(LOG_DIR, old))
        except Exception:
            pass
    shutil.rmtree(temp_dir)
    log("Script finished successfully", log_file=log_file)

if __name__ == '__main__':
    main()
