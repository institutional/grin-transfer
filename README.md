# GRIN-to-S3

An async pipeline for extracting Google Books data from GRIN (Google Return Interface) to local databases and S3-compatible storage, designed for academic libraries.

## Requirements

- **Python 3.12+** (required for modern language features)
- Dependencies managed via `pyproject.toml`

## Installation

```bash
# Install in development mode (includes all dependencies and dev tools)
pip install -e ".[dev]"

# Set up OAuth2 credentials (interactive)
python auth.py setup
```

## Core Scripts

The pipeline provides four main command-line scripts:

### 1. Book Collection: `collect_books.py`

Collects book metadata from GRIN into local SQLite database with run configuration.

```bash
# Basic collection with three-bucket storage
python collect_books.py --run-name "harvard_2024" \
  --storage r2 \
  --bucket-raw grin-raw \
  --bucket-meta grin-meta \
  --bucket-full grin-full

# Local storage (no buckets required)
python collect_books.py --run-name "local_test" --storage local

# Test mode with mock data
python collect_books.py --test-mode --limit 100 --storage local
```

**Key options:**
- `--run-name`: Named run for organized output (defaults to timestamp)
- `--storage`: Storage backend (`local`, `minio`, `r2`, `s3`)
- `--bucket-raw`: Raw data bucket (sync archives)
- `--bucket-meta`: Metadata bucket (CSV/database outputs)
- `--bucket-full`: Full-text bucket (OCR outputs)
- `--limit`: Limit books for testing
- `--rate-limit`: API requests per second (default: 5.0)

### 2. Processing Management: `processing.py`

Request and monitor book processing via GRIN conversion system.

```bash
# Request processing for books in database
python processing.py request output/harvard_2024/books.db --limit 1000

# Monitor processing status
python processing.py monitor output/harvard_2024/books.db
```

**Commands:**
- `request`: Submit books for GRIN processing
- `monitor`: Check processing status and progress

### 3. Sync Pipeline: `sync.py`

Download converted books from GRIN to storage with database tracking.

```bash
# Sync converted books to storage (auto-detects config from database)
python sync.py pipeline output/harvard_2024/books.db

# Sync with explicit storage configuration
python sync.py pipeline output/harvard_2024/books.db \
  --storage r2 \
  --bucket-raw grin-raw \
  --bucket-meta grin-meta \
  --bucket-full grin-full

# Check sync status
python sync.py status output/harvard_2024/books.db

# Retry failed syncs only
python sync.py pipeline output/harvard_2024/books.db --status failed
```

**Pipeline options:**
- `--concurrent`: Parallel downloads (default: 3)
- `--batch-size`: Books per batch (default: 100)
- `--limit`: Limit number of books to sync
- `--status`: Filter by sync status (`pending`, `failed`)
- `--force`: Overwrite existing files

### 4. GRIN Enrichment: `grin_enrichment.py`

Add detailed GRIN metadata to collected books and export to CSV.

```bash
python grin_enrichment.py status --run-name harvard_2024
python grin_enrichment.py enrich --run-name harvard_2024
python grin_enrichment.py export-csv --run-name harvard_2024 --output books.csv
```

**Commands:**
- `status`: Show enrichment progress and statistics
- `enrich`: Add detailed GRIN metadata to collected books
- `export-csv`: Export enriched data to CSV file

## Storage Configuration

The system uses a three-bucket architecture for different data types:

- **Raw Bucket** (`--bucket-raw`): Encrypted archives from GRIN sync
- **Metadata Bucket** (`--bucket-meta`): CSV files and database exports  
- **Full-text Bucket** (`--bucket-full`): OCR text extraction outputs

### Storage Backends

**Local Storage:**
```bash
python collect_books.py --run-name "local" --storage local
```

**Cloudflare R2:**
```bash
python collect_books.py --run-name "r2" --storage r2 \
  --bucket-raw my-raw --bucket-meta my-meta --bucket-full my-full
```

**MinIO:**
```bash
python collect_books.py --run-name "minio" --storage minio \
  --bucket-raw grin-raw --bucket-meta grin-meta --bucket-full grin-full \
  --storage-config endpoint_url=localhost:9000
```

**AWS S3:**
```bash
python collect_books.py --run-name "s3" --storage s3 \
  --bucket-raw my-raw --bucket-meta my-meta --bucket-full my-full
```

## Run Configuration System

The first script (`collect_books.py`) writes configuration to the run directory. Other scripts automatically detect and use this configuration:

```bash
# Initial collection creates run config
python collect_books.py --run-name "my_run" --storage r2 \
  --bucket-raw raw --bucket-meta meta --bucket-full full

# Other scripts auto-detect config from run name
python processing.py request output/my_run/books.db
python sync.py pipeline output/my_run/books.db
python grin_enrichment.py enrich --run-name my_run
```

Configuration is stored in `output/{run_name}/run_config.json` and includes storage settings, GRIN directory, and other parameters.

## Typical Workflow

1. **Collect books** with storage configuration:
   ```bash
   python collect_books.py --run-name "collection_2024" \
     --storage r2 --bucket-raw raw --bucket-meta meta --bucket-full full
   ```

2. **Request processing** for conversion:
   ```bash
   python processing.py request output/collection_2024/books.db --limit 1000
   ```

3. **Monitor processing** until books are converted:
   ```bash
   python processing.py monitor output/collection_2024/books.db
   ```

4. **Sync converted books** to storage:
   ```bash
   python sync.py pipeline output/collection_2024/books.db
   ```

5. **Enrich with metadata** and export:
   ```bash
   python grin_enrichment.py enrich --run-name collection_2024
   python grin_enrichment.py export-csv --run-name collection_2024 --output final.csv
   ```

## Authentication Setup

Run the OAuth2 setup to authenticate with GRIN:

```bash
python auth.py setup
```

This creates credential files in `~/.config/grin-to-s3/`:
- `client_secret.json` - OAuth2 client configuration (from Google Cloud Console)
- `credentials.json` - User authentication tokens (generated by setup)

## Development

```bash
# Install with development dependencies
pip install -e ".[dev]"

# Run linting and formatting
ruff check --fix && ruff format

# Run type checking
mypy --explicit-package-bases *.py

# Run tests
python -m pytest
```

## File Outputs

Each run creates organized output in `output/{run_name}/`:
- `books.db` - SQLite database with all book records and sync status
- `books_{timestamp}.csv` - Timestamped CSV export
- `progress.json` - Collection progress for resume capability
- `run_config.json` - Configuration for other scripts to auto-detect settings