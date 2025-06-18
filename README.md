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
python grin.py auth setup
```

## Core Commands

The pipeline provides a unified command-line interface via `grin.py` with four main subcommands:

### 1. Book Collection: `grin.py collect`

Collects book metadata from GRIN into local SQLite database with run configuration.

```bash
# Basic collection with three-bucket storage
python grin.py collect --run-name "harvard_2024" \
  --storage r2 \
  --bucket-raw grin-raw \
  --bucket-meta grin-meta \
  --bucket-full grin-full

# Local storage (no buckets required)
python grin.py collect --run-name "local_test" --storage local

# Test mode with mock data
python grin.py collect --test-mode --limit 100 --storage local
```

**Key options:**
- `--run-name`: Named run for organized output (defaults to timestamp)
- `--storage`: Storage backend (`local`, `minio`, `r2`, `s3`)
- `--bucket-raw`: Raw data bucket (sync archives)
- `--bucket-meta`: Metadata bucket (CSV/database outputs)
- `--bucket-full`: Full-text bucket (OCR outputs)
- `--limit`: Limit books for testing
- `--rate-limit`: API requests per second (default: 5.0)

### 2. Processing Management: `grin.py process`

Request and monitor book processing via GRIN conversion system.

```bash
python grin.py process request --run-name harvard_2024 --limit 1000
python grin.py process monitor --run-name harvard_2024
```

**Commands:**
- `request`: Submit books for GRIN processing
- `monitor`: Check processing status and progress

### 3. Sync Pipeline: `grin.py sync`

Download converted books from GRIN to storage with database tracking.

```bash
# Sync converted books to storage (auto-detects config from run)
python grin.py sync pipeline --run-name harvard_2024

# Sync with explicit storage configuration
python grin.py sync pipeline --run-name harvard_2024 \
  --storage r2 \
  --bucket-raw grin-raw \
  --bucket-meta grin-meta \
  --bucket-full grin-full

# Check sync status
python grin.py sync status --run-name harvard_2024

# Retry failed syncs only
python grin.py sync pipeline --run-name harvard_2024 --status failed
```

**Pipeline options:**
- `--concurrent`: Parallel downloads (default: 3)
- `--batch-size`: Books per batch (default: 100)
- `--limit`: Limit number of books to sync
- `--status`: Filter by sync status (`pending`, `failed`)
- `--force`: Overwrite existing files

### 4. GRIN Enrichment: `grin.py enrich/export-csv/status`

Add detailed GRIN metadata to collected books and export to CSV.

```bash
python grin.py status --run-name harvard_2024
python grin.py enrich --run-name harvard_2024
python grin.py export-csv --run-name harvard_2024 --output books.csv
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
python grin.py collect --run-name "local" --storage local
```

**Cloudflare R2:**
```bash
python grin.py collect --run-name "r2" --storage r2 \
  --bucket-raw my-raw --bucket-meta my-meta --bucket-full my-full
```

**MinIO:**
```bash
python grin.py collect --run-name "minio" --storage minio \
  --bucket-raw grin-raw --bucket-meta grin-meta --bucket-full grin-full \
  --storage-config endpoint_url=localhost:9000
```

**AWS S3:**
```bash
python grin.py collect --run-name "s3" --storage s3 \
  --bucket-raw my-raw --bucket-meta my-meta --bucket-full my-full
```

## Run Configuration System

The first command (`grin.py collect`) writes configuration to the run directory. Other commands automatically detect and use this configuration:

```bash
# Initial collection creates run config
python grin.py collect --run-name "my_run" --storage r2 \
  --bucket-raw raw --bucket-meta meta --bucket-full full

# Other commands auto-detect config from run name
python grin.py process request --run-name my_run
python grin.py sync pipeline --run-name my_run
python grin.py enrich --run-name my_run
```

Configuration is stored in `output/{run_name}/run_config.json` and includes storage settings, GRIN directory, and other parameters.

## Typical Workflow

1. **Collect books** with storage configuration:
   ```bash
   python grin.py collect --run-name "collection_2024" \
     --storage r2 --bucket-raw raw --bucket-meta meta --bucket-full full
   ```

2. **Request processing** for conversion:
   ```bash
   python grin.py process request --run-name collection_2024 --limit 1000
   ```

3. **Monitor processing** until books are converted:
   ```bash
   python grin.py process monitor --run-name collection_2024
   ```

4. **Sync converted books** to storage:
   ```bash
   python grin.py sync pipeline --run-name collection_2024
   ```

5. **Enrich with metadata** and export:
   ```bash
   python grin.py enrich --run-name collection_2024
   python grin.py export-csv --run-name collection_2024 --output final.csv
   ```

## Authentication Setup

Run the OAuth2 setup to authenticate with GRIN:

```bash
python grin.py auth setup
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
mypy src/grin_to_s3/*.py --explicit-package-bases

# Run tests
python -m pytest
```

## Shell Completion (Optional)

For enhanced command-line experience with auto-completion of commands, arguments, and run names:

```bash
# Install zsh completions
./install-completions.sh

# Restart terminal or reload shell
source ~/.zshrc
```

**Features:**
- Tab completion for all script commands and subcommands
- Auto-completion of run names from `output/` directory
- Argument validation and help text
- Storage type and log level suggestions

**Examples:**
```bash
python grin.py <TAB>                    # Shows: auth, collect, process, sync, enrich, export-csv, status
python grin.py sync <TAB>               # Shows: pipeline, status, catchup
python grin.py process --run-name <TAB> # Shows available run names
```

## File Outputs

Each run creates organized output in `output/{run_name}/`:
- `books.db` - SQLite database with all book records and sync status
- `books_{timestamp}.csv` - Timestamped CSV export
- `progress.json` - Collection progress for resume capability
- `run_config.json` - Configuration for other scripts to auto-detect settings