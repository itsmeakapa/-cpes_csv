# CPE Data Download and Conversion Tool

## Overview

This repository contains a robust bash script that downloads Common Platform Enumeration (CPE) data from the National Vulnerability Database (NVD) API and converts it into multiple formats (JSON, TXT, CSV) for easy consumption and analysis.

## What is CPE?

Common Platform Enumeration (CPE) is a standardized method of describing and identifying classes of applications, operating systems, and hardware devices present among an enterprise's computing assets. The NVD maintains a comprehensive dictionary of CPE entries used for vulnerability management and security assessments.

## Features

### Core Functionality

- **Complete Data Download**: Downloads all CPE entries from the NVD API (1.5+ million entries across 151+ pages)
- **Pagination Handling**: Automatically handles API pagination with proper rate limiting (5 requests per 30 seconds)
- **Multiple Output Formats**: 
  - **JSON**: Complete structured data with all metadata
  - **TXT**: One CPE entry per line in JSON format
  - **CSV**: Flat file format for spreadsheet analysis
- **Automatic Retry Logic**: Infinite retry capability with detailed error logging
- **Connection Resilience**: Handles network interruptions, connection resets, and API timeouts
- **Progress Tracking**: Real-time progress updates with percentage completion
- **Checkpoint/Resume**: Automatically resumes from last successful page on connection failures

### Robust Error Handling

- **Infinite Retry**: Script never gives up - retries every 35 seconds until all data is downloaded
- **Detailed Logging**: Comprehensive logs at every checkpoint with:
  - Download progress and success/failure details
  - Connection errors and retry attempts
  - JSON validation failures
  - Timing information for each phase
- **Exponential Backoff**: Smart retry delays respecting API rate limits
- **Checkpoint System**: Saves progress after each page for seamless resume

### Performance Features

- **Optimized Connection Handling**:
  - TCP no-delay for immediate data transmission
  - Fresh connections to avoid stale connection issues
  - Built-in curl retries (2 attempts per request)
  - Configurable timeouts (20s connect, 180s max)
- **Timing Metrics**:
  - Download time tracking
  - CSV conversion time tracking
  - Total script execution time
  - Start and end timestamps

### File Management

- **Canonical Files**: Latest versions saved as `cpes.json`, `cpes.txt`, `cpes.csv`
- **Timestamped Files**: Intermediate files with timestamps for tracking
- **Log Rotation**: Keeps only the latest 3 log files automatically
- **Automatic Cleanup**: Removes old timestamped files after successful completion

## Script Location

```
cpes/
  ├── download_cpes.sh     # Main download script
  ├── data/                # Output directory (created automatically)
  │   ├── cpes.json       # Complete CPE data in JSON format
  │   ├── cpes.txt        # One CPE per line (JSON)
  │   └── cpes.csv        # Flat CSV format for analysis
  └── logs/                # Log directory (created automatically)
      └── cpes_download_YYYY-MM-DD_HHMMSS.log
```

## Usage

### Full Download (All Pages)

Download all CPE entries from the NVD API:

```bash
cd cpes
./download_cpes.sh
```

**Estimated Time**: 15-20 minutes for ~1.5M CPE entries (151+ pages)

### Test Mode (5 Pages Only)

For testing purposes, download only the first 5 pages (50,000 entries):

```bash
cd cpes
./download_cpes.sh test
```

**Estimated Time**: ~1 minute

## Output Format Details

### CSV Format

The CSV file contains the following columns:

| Column | Description | Example |
|--------|-------------|---------|
| `deprecated` | Whether the CPE is deprecated | `false` |
| `cpeName` | CPE 2.3 formatted name | `cpe:2.3:a:amanda:amanda:-:*:*:*:*:*:*:*` |
| `cpeNameId` | Unique identifier (UUID) | `2E62A08B-038C-46CC-BE2B-A5697DB71B8F` |
| `lastModified` | Last modification timestamp | `2023-05-11T13:58:21.000` |
| `created` | Creation timestamp | `2007-08-23T21:05:57.937` |
| `title` | English language title | `AMANDA AMANDA` |
| `refs` | Space-separated reference URLs | `https://github.com/... http://www...` |

### JSON Format

Complete structured data with all metadata including:
- Results per page
- Total results count
- API format and version
- Timestamp
- Full products array with all CPE details

### TXT Format

One CPE entry per line in compact JSON format:
```json
{"cpe":{"deprecated":false,"cpeName":"cpe:2.3:a:amanda:amanda:-:*:*:*:*:*:*:*","cpeNameId":"2E62A08B-038C-46CC-BE2B-A5697DB71B8F",...}}
```

## How It Works

### 1. **Initialization Phase**
   - Validates required tools (python3, curl/wget)
   - Creates necessary directories (`data/`, `logs/`)
   - Initializes log file with timestamp

### 2. **Metadata Retrieval**
   - Queries NVD API for total CPE count
   - Calculates number of pages needed (10,000 entries per page)
   - Initializes JSON file with metadata

### 3. **Paginated Download Loop**
   - Downloads each page with retry logic (up to 5 attempts per page)
   - Validates JSON structure and entry count
   - Appends data to output files incrementally
   - Saves checkpoint after each successful page
   - Implements 7-second delay between requests (respects rate limits)

### 4. **Automatic Retry System**
   - On failure, waits 35 seconds and resumes from last checkpoint
   - Retries indefinitely until all pages are downloaded
   - Logs detailed error information at each failure point

### 5. **CSV Conversion**
   - Waits 10 seconds after download completion
   - Processes TXT file using Python to extract fields
   - Creates CSV with proper headers and formatting
   - Handles missing fields gracefully

### 6. **File Management**
   - Moves timestamped files to canonical names
   - Removes old timestamped files
   - Rotates log files (keeps latest 3)
   - Logs execution summary with timing details

## API Rate Limits

The NVD API enforces the following limits:

- **Unauthenticated**: 5 requests per 30 seconds per IP address
- **Maximum Results**: 10,000 results per page

The script respects these limits with:
- 7-second delay between successful requests
- 35-second delay between full retry attempts
- Built-in retry logic to handle rate limit errors

## Connection Handling

The script implements robust connection handling to deal with:

- **Network Timeouts**: 20s connect timeout, 180s max time
- **Connection Resets**: Fresh connections for each request (no keep-alive reuse)
- **TCP Optimization**: TCP no-delay for immediate transmission
- **Automatic Recovery**: Infinite retry with checkpoint resume

## Log Files

Logs are stored in `logs/cpes_download_YYYY-MM-DD_HHMMSS.log` and include:

- Script start/end timestamps
- Download progress with page numbers and percentages
- Success/failure details for each page
- Error messages with full context
- Checkpoint save confirmations
- Retry attempt information
- Timing metrics (download time, CSV time, total time)
- Execution summary on completion

## Requirements

- **bash**: Version 4.0+ (uses array features)
- **python3**: For JSON parsing and CSV conversion
- **curl** or **wget**: For HTTP requests (curl preferred)
- **Standard utilities**: `date`, `stat`, `wc`, `grep`, `sed`

## Dependencies

The script automatically checks for required tools and exits with a clear error if any are missing.

## Error Recovery

If the script is interrupted (connection drop, system restart, etc.):

1. Simply rerun the script with the same command
2. It will automatically detect the checkpoint
3. Resume from the last successfully downloaded page
4. Continue until all pages are downloaded

The script will **never lose progress** and will **retry indefinitely** until successful.

## Performance Optimization

- **Incremental Writing**: Appends data to files as pages are downloaded (no memory bloat)
- **Efficient JSON Processing**: Uses Python's native JSON library
- **Minimal Disk I/O**: Writes checkpoint only after successful page downloads
- **Connection Pooling**: Reuses HTTP connections where safe (via curl)

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success - all data downloaded and converted |
| 1 | Failed to download initial metadata page |
| 3 | Python3 not installed |
| 4 | Failed to create CSV file |
| 5 | Failed to create canonical files |

## Use Cases

- **Vulnerability Management**: Cross-reference CVE data with CPE identifiers
- **Asset Inventory**: Match discovered software/hardware to standardized CPE names
- **Security Analysis**: Analyze deprecated CPEs and version histories
- **Compliance Reporting**: Generate reports on covered software/hardware platforms
- **Research**: Study CPE naming patterns and coverage across vendors

## Troubleshooting

### Connection Refused Errors
- Check firewall rules for outbound HTTPS (port 443)
- Verify DNS resolution for `services.nvd.nist.gov`
- Check if proxy configuration is needed

### Rate Limit Errors
- Script handles these automatically with retries
- Errors are logged with full details
- Wait times respect API limits

### Incomplete Downloads
- Script automatically resumes from checkpoint
- Check log file for specific error details
- Ensure sufficient disk space

## Contributing

Feel free to submit issues or pull requests for improvements.

## License

This tool is provided as-is for downloading public NVD data. Please respect the NVD API terms of service.

## API Source

Data is downloaded from: `https://services.nvd.nist.gov/rest/json/cpes/2.0`

Official NVD API Documentation: https://nvd.nist.gov/developers