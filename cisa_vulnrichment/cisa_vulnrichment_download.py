#!/usr/bin/env python3
"""
cisa_vulnrichment_download.py - Clone/sync CISA vulnrichment repo and generate CSV
"""
import os
import sys
import time
import shutil
import argparse
import subprocess
import requests
import json
import csv
from pathlib import Path
from datetime import datetime

def get_timestamp():
    return datetime.now().strftime('%Y-%m-%d_%H%M%S')

def log(msg, level='INFO', log_file=None, print_also=False):
    now = datetime.now().isoformat()
    line = f"{now} {level} {msg}\n"
    if log_file:
        with open(log_file, 'a') as f:
            f.write(line)
    if print_also:
        print(line, end='')

def check_git_available():
    if shutil.which('git') is None:
        raise RuntimeError('git is required but not installed')

def check_github_connectivity():
    try:
        r = requests.head('https://github.com', timeout=15)
        return r.status_code == 200
    except Exception:
        return False

def sync_or_clone_repo(repo_url, branch, repo_dir, log_file):
    if os.path.isdir(os.path.join(repo_dir, '.git')):
        log('Local repository exists - syncing with origin', log_file=log_file, print_also=True)
        try:
            subprocess.run(['git', '-C', repo_dir, 'fetch', 'origin', branch], check=True, capture_output=True)
            subprocess.run(['git', '-C', repo_dir, 'reset', '--hard', f'origin/{branch}'], check=True, capture_output=True)
            subprocess.run(['git', '-C', repo_dir, 'clean', '-fd'], check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            log(f'ERROR: git sync failed: {e}', level='ERROR', log_file=log_file, print_also=True)
            raise
    else:
        log('Local repository not found - performing initial clone', log_file=log_file, print_also=True)
        try:
            subprocess.run(['git', 'clone', '--depth', '1', '--branch', branch, repo_url, repo_dir], check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            log(f'ERROR: git clone failed: {e}', level='ERROR', log_file=log_file, print_also=True)
            raise

def count_json_files(repo_dir):
    return sum(1 for _ in Path(repo_dir).rglob('CVE-*.json'))

def csv_from_vulnrichment(repo_dir, output_csv, log_file):
    # --- CSV generation logic from create_vulnrichment_csv.sh ---
    def safe_get(data, *keys, default=""):
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
        if not metrics_list or not isinstance(metrics_list, list):
            return None, None
        for metric in metrics_list:
            if not isinstance(metric, dict):
                continue
            for key in metric.keys():
                if key.startswith('cvssV') and '_' in key:
                    return key, metric[key]
        return None, None

    def find_ssvc_data(metrics_list):
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
                    if 'id' in content:
                        ssvc_data['cve_id'] = content['id']
                    if other.get('type') == 'kev':
                        ssvc_data['kev_entry'] = 'kev'
                        ssvc_data['kev_date'] = content.get('dateAdded', '')
        return ssvc_data

    def find_cwe_data(problem_types):
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
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            cve_id = safe_get(data, 'cveMetadata', 'cveId', default='')
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
            adp_list = safe_get(containers, 'adp', default=[])
            if isinstance(adp_list, list):
                for adp in adp_list:
                    if not isinstance(adp, dict):
                        continue
                    metrics = adp.get('metrics', [])
                    cvss_key, cvss_data = find_cvss_data(metrics)
                    if cvss_data:
                        result['cisa_cvss_base_score'] = str(safe_get(cvss_data, 'baseScore', default=''))
                        result['cisa_cvss_base_severity'] = safe_get(cvss_data, 'baseSeverity', default='')
                        result['cisa_cvss_vector_string'] = safe_get(cvss_data, 'vectorString', default='')
                        result['cisa_cvss_version'] = safe_get(cvss_data, 'version', default='')
                    ssvc_data = find_ssvc_data(metrics)
                    result['ssvc_exploitation'] = ssvc_data.get('exploitation', '')
                    result['ssvc_automatable'] = ssvc_data.get('automatable', '')
                    result['ssvc_impact'] = ssvc_data.get('impact', '')
                    result['kev_entry'] = ssvc_data.get('kev_entry', '')
                    result['kev_date'] = ssvc_data.get('kev_date', '')
                    problem_types = adp.get('problemTypes', [])
                    cwe_id, cwe_desc = find_cwe_data(problem_types)
                    if cwe_id:
                        result['cwe_id'] = cwe_id
                        result['cwe_description'] = cwe_desc
            cna = safe_get(containers, 'cna', default={})
            if isinstance(cna, dict):
                metrics = cna.get('metrics', [])
                cvss_key, cvss_data = find_cvss_data(metrics)
                if cvss_data:
                    result['cna_cvss_base_score'] = str(safe_get(cvss_data, 'baseScore', default=''))
                    result['cna_cvss_base_severity'] = safe_get(cvss_data, 'baseSeverity', default='')
                    result['cna_cvss_vector_string'] = safe_get(cvss_data, 'vectorString', default='')
                    result['cna_cvss_version'] = safe_get(cvss_data, 'version', default='')
                if not result['cwe_id']:
                    problem_types = cna.get('problemTypes', [])
                    cwe_id, cwe_desc = find_cwe_data(problem_types)
                    result['cwe_id'] = cwe_id
                    result['cwe_description'] = cwe_desc
            return result
        except Exception as e:
            return {'error': str(e), 'file': file_path}

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
    with open(output_csv, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(header)
        processed_files = 0
        error_files = 0
        for year_dir in sorted(Path(repo_dir).iterdir()):
            if not year_dir.is_dir():
                continue
            for subdir in sorted(year_dir.iterdir()):
                if not subdir.is_dir():
                    continue
                json_files = sorted(subdir.glob('CVE-*.json'))
                for json_file in json_files:
                    try:
                        result = parse_cve_json(json_file)
                        if 'error' in result:
                            error_files += 1
                            log(f"ERROR: Failed to parse {json_file}: {result['error']}", level='ERROR', log_file=log_file)
                            continue
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
                            log(f"Processed {processed_files} files...", log_file=log_file)
                    except Exception as e:
                        error_files += 1
                        log(f"ERROR: Error processing {json_file}: {str(e)}", level='ERROR', log_file=log_file)
        log(f"CSV generation complete. Processed: {processed_files}, Errors: {error_files}", log_file=log_file)


def main():
    parser = argparse.ArgumentParser(description='Sync CISA vulnrichment repo and generate CSV')
    parser.add_argument('--repo-url', default='https://github.com/cisagov/vulnrichment.git', help='GitHub repo URL')
    parser.add_argument('--branch', default='develop', help='Branch to use')
    parser.add_argument('--data-dir', default=None, help='Data directory (default: ./data)')
    parser.add_argument('--log-dir', default=None, help='Log directory (default: ./logs)')
    args = parser.parse_args()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = args.data_dir or os.path.join(script_dir, 'data')
    log_dir = args.log_dir or os.path.join(script_dir, 'logs')
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)
    timestamp = get_timestamp()
    log_file = os.path.join(log_dir, f'vulnrichment_git_download_{timestamp}.log')
    repo_dir = os.path.join(data_dir, 'vulnrichment_repo')
    output_csv = os.path.join(data_dir, f'cisa_vulnrichment_{timestamp}.csv')
    canon_csv = os.path.join(data_dir, 'cisa_vulnrichment.csv')
    canon_tar_gz = os.path.join(data_dir, 'cisa_vulnrichment.csv.tar.gz')
    log('=== CISA VULNRICHMENT GIT SYNC STARTED ===', log_file=log_file, print_also=True)
    log(f'Repository: {args.repo_url}', log_file=log_file)
    log(f'Branch: {args.branch}', log_file=log_file)
    log(f'Local path: {repo_dir}', log_file=log_file)
    log(f'Log file: {log_file}', log_file=log_file)
    check_git_available()
    if not check_github_connectivity():
        log('ERROR: Cannot connect to github.com', level='ERROR', log_file=log_file, print_also=True)
        sys.exit(1)
    try:
        sync_or_clone_repo(args.repo_url, args.branch, repo_dir, log_file)
    except Exception:
        sys.exit(1)
    total_files = count_json_files(repo_dir)
    log(f'Total CVE JSON files: {total_files}', log_file=log_file)
    csv_from_vulnrichment(repo_dir, output_csv, log_file)
    # Move to canonical name
    if os.path.exists(canon_csv):
        os.remove(canon_csv)
    shutil.move(output_csv, canon_csv)
    log(f'Canonical CSV updated: {canon_csv}', log_file=log_file, print_also=True)

    # Compress the canonical CSV to tar.gz
    import tarfile
    with tarfile.open(canon_tar_gz, "w:gz") as tar:
        tar.add(canon_csv, arcname=os.path.basename(canon_csv))
    log(f'Compressed CSV to: {canon_tar_gz}', log_file=log_file, print_also=True)

    # Remove the uncompressed canonical CSV
    try:
        os.remove(canon_csv)
        log(f'Removed uncompressed CSV: {canon_csv}', log_file=log_file)
    except Exception:
        log(f'WARNING: Could not remove {canon_csv}', level='WARNING', log_file=log_file)

    # Clean up old timestamped CSV files
    for fname in os.listdir(data_dir):
        if fname.startswith('cisa_vulnrichment_') and fname.endswith('.csv'):
            try:
                os.remove(os.path.join(data_dir, fname))
            except Exception:
                pass
    # Clean logs directory: keep only latest 5 log files
    logs = sorted([f for f in os.listdir(log_dir) if f.startswith('vulnrichment_git_download_') and f.endswith('.log')], reverse=True)
    for old in logs[5:]:
        try:
            os.remove(os.path.join(log_dir, old))
        except Exception:
            pass
    log('=== SCRIPT COMPLETED SUCCESSFULLY ===', log_file=log_file, print_also=True)
    print('All done! Repository synced and CSV updated and compressed.')

if __name__ == '__main__':
    main()
