# GRIN-to-S3

A pipeline for extracting page scans, metadata, and OCR from Google Books GRIN (Google Return Interface) by academic libraries who have partnered with Google Books. The tool will programmatically download and store book archives to local or S3-compatible storage. 

## Prerequisites

1. You should know your "library directory", or the path in GRIN where your books are made available. For example, given the URL https://books.google.com/libraries/Harvard/_all_books, the "library directory" is "Harvard." This value is case-sensitive.
2. You should have credentials for the Google account that was given to a Google partner manager for access to GRIN. This will be the same account that you use to log in to GRIN as a human.
3. Book archives stored in GRIN are encrypted. You'll need your GPG decryption passphrase from Google. It will be a short text file containing random words.
4. (Optional) We recommend you set up S3-like cloud storage to extract large collections. GRIN-to-S3 will use those credentials to transfer decrypted book archives and metadata. For smaller collections, you can use any filesystem reachable from where you run this tool.

## Installation requirements

The GRIN-to-S3 pipeline can be installed via docker, or directly on a target Linux-like or MacOS machine.

### Quick start with docker

We've provided a `grin-docker` wrapper script to make accessing the tool straightforward.

Assuming Docker is running on your local or host machine:

```bash

# Build the docker environment and configure authentication with GRIN
./grin-docker auth setup

# Then run a sample collection using the default storage option available in the docker container
./grin-docker collect --run-name test_run --library-directory YOUR_LIBRARY_DIRECTORY --storage minio --limit 10

# You can also connect directly to the docker container if needed
./grin-docker bash 
```

### Local installation

- Requires Python 3.12+
- Dependencies are managed via `pyproject.toml`

```bash
# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate 

# Install dependencies
pip install -e "."

# Interactively configure authentication with the GRIN interface
python grin.py auth setup

# Then run a sample collection 
python grin.py collect --run-name test_run --library-directory YOUR_LIBRARY_DIRECTORY --storage local --storage-config base_path=/tmp/grin-to-s3-storage --limit 10

# And sync those 10 files
python grin.py sync pipeline --run-name test_run
```
## Pipeline concepts

### Runs
Discrete sessions of pipeline operations are called "runs" and are referenced via the `--run-name` argument. If you don't specify a run name, it will default to a timestamped value like `run_20250721_103242`. 

### Storage
Downloaded archives are ultimately synced to your **storage**, which may be a cloud-based block storage like S3, GCS, or R2, or a local filesystem. Configuring storage is required.

For block storage, the default configuration is to use three buckets: 

1. `raw`: The decrypted book archives in their entirety
2. `full`: Only the OCR exports from the books
3. `meta`: Complete book metadata and run information


### Staging
During processing, book archives and metadata are saved in the **staging** area on your local filesystem until they are fully uploaded to storage. By default,  `staging/` is created in the repo directory, but can be overridden with `--sync-staging-dir`. To avoid running out of local disk space, `--sync-disk-space-threshold` is used to keep the staging area at capacity; when capacity is exceeded, downloads are paused and usually will resume once files are fully uploaded.

## Pipeline steps
Each run begins with the **collection** step, where book metadata is first downloaded from GRIN and stored locally for evaluation and processing. Collecting all book metadata for very large (1 million+ book) collections usually takes about an hour.

GRIN needs to "convert" archives to make them available for download. This pipeline can **request** conversions up to Google's maximum of 50,000 queued.

The next step is to start the **sync pipeline**, in which the original book archives are downloaded from GRIN, decrypted, **enriched** with additional GRIN metadata, and uploaded or copied to storage. 

Finally, there are utilities for checking progress and gathering statistics about the run. 

## Core Commands

The pipeline provides a unified command-line interface via `grin.py`, with  subcommands that map to the pipeline steps. Note that installed via docker, for convenience these can all be called from outside the contanier as `grin-docker <subcommand>`

### 1. Book Collection: `grin.py collect`

Collects book metadata from GRIN into the local SQLite database and writes the run configuration with relevant parameters.

```bash
# Basic collection using CloudFlare R2 as the remote storage
python grin.py collect --run-name "harvard_2024" \
  --library-directory Harvard \
  --storage r2 

# Local storage (no buckets required, but necessary to specify the storage path)
python grin.py collect --run-name "local_test" --library-directory Harvard --storage local --storage-config base_path=/tmp/storage

```

**Key options:**
- `--library-directory`: GRIN library directory name (required, e.g., Harvard, MIT, Yale)
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

# Check sync status
python grin.py sync status --run-name harvard_2024

# Retry failed syncs only
python grin.py sync pipeline --run-name harvard_2024 --status failed

```

**Pipeline options:**
- `--concurrent`: Parallel downloads (default: 5)
- `--concurrent-uploads`: Parallel uploads (default: 10)
- `--batch-size`: Books per batch (default: 100)
- `--limit`: Limit number of books to sync
- `--status`: Filter by sync status (`pending`, `failed`)
- `--force`: Overwrite existing files
- `--staging-dir`: Custom staging directory path (default: output/run-name/staging)

### 4. Storage Management: `grin.py storage`

Manage storage buckets with fast listing and deletion.

```bash
# List all buckets (summary)
python grin.py storage ls --run-name harvard_2024

# Detailed listing with file sizes
python grin.py storage ls --run-name harvard_2024 -l

# Remove all files from a bucket
python grin.py storage rm raw --run-name harvard_2024

# Remove with auto-confirm (dangerous!)
python grin.py storage rm meta --run-name harvard_2024 --yes

# Dry run to see what would be deleted
python grin.py storage rm full --run-name harvard_2024 --dry-run
```

**Commands:**
- `ls`: List contents of all storage buckets with file counts and sizes
- `rm`: Remove all contents from specified bucket (raw, meta, or full)



### Storage Backends

**Local Storage:**
```bash
python grin.py collect --run-name "local" --library-directory Harvard --storage local --storage-config base_path=/tmp/grin-books
```

**Cloudflare R2:**
```bash
python grin.py collect --run-name "r2" --library-directory Harvard --storage r2
```

**MinIO (Docker only):**
```bash
# MinIO is auto-configured inside Docker containers
docker-compose --profile minio exec grin-to-s3 python grin.py collect \
  --run-name "minio" --library-directory Harvard --storage minio 
```

**AWS S3:**
```bash
python grin.py collect --run-name "s3" --library-directory Harvard --storage s3 
```

## Run Configuration System

The first command (`grin.py collect`) writes configuration to the run directory. Other commands automatically detect and use this configuration:

```bash
# Initial collection creates run config
python grin.py collect --run-name "my_run" --library-directory Harvard --storage r2 

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
     --library-directory Harvard --storage r2 --bucket-raw raw --bucket-meta meta --bucket-full full
   ```

2. **Request processing** for conversion:
   ```bash
   python grin.py process request --run-name collection_2024 --limit 1000
   ```

3. **Monitor processing** until books are converted:
   ```bash
   python grin.py process monitor --run-name collection_2024
   ```

4. **Sync converted books** to storage (with automatic OCR extraction):
   ```bash
   python grin.py sync pipeline --run-name collection_2024
   ```

5. **Extract OCR text** separately if needed (alternative to automatic extraction):
   ```bash
   python grin.py extract output/collection_2024/staging/*.tar.gz --output-dir output/collection_2024/text/
   ```

6. **Enrich with metadata** and export:
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
python grin.py <TAB>                    # Shows: auth, collect, process, sync, extract, enrich, export-csv, reports
python grin.py sync <TAB>               # Shows: pipeline, status, catchup
python grin.py process --run-name <TAB> # Shows available run names
```

## File Outputs

Each run creates organized output in `output/{run_name}/`:
- `books.db` - SQLite database with all book records and sync status
- `books_{timestamp}.csv` - Timestamped CSV export
- `progress.json` - Collection progress for resume capability
- `run_config.json` - Configuration for other scripts to auto-detect settings


**Production with Cloudflare R2:**
```bash
# 1. Configure credentials
cp examples/docker/r2-credentials-template.json ~/.config/grin-to-s3/r2_credentials.json
# Edit r2_credentials.json with your actual credentials

# 2. Set up OAuth2 credentials (run once)
./grin-docker python grin.py auth setup

# 3. Start the application and run collection
docker-compose up -d
./grin-docker python grin.py collect --storage r2 --run-name production
```

### Docker Features

- **Multi-stage builds** for optimized image size
- **Non-root user** for security
- **Persistent volumes** for data, configuration, and logs
- **Multiple storage backends** (R2, S3, MinIO, local)
- **Credential management** via file mounts or environment variables
- **Development environment** with MinIO integration

### Available Docker Configurations

- `docker-compose.yml` - Universal configuration supporting all storage backends
- `examples/docker/` - Credential templates for different storage providers

### Common Docker Commands

```bash
# Build the image
docker build -t grin-to-s3 .

# Run individual commands
docker run --rm -v $(pwd)/docker-data:/app/data grin-to-s3 --help

# Complete pipeline workflow
docker-compose exec grin-to-s3 python grin.py collect --run-name docker_test --library-directory Harvard --storage minio --limit 5
docker-compose exec grin-to-s3 python grin.py process request --run-name docker_test --limit 5
docker-compose exec grin-to-s3 python grin.py sync pipeline --run-name docker_test
docker-compose exec grin-to-s3 python grin.py export --run-name docker_test --output /app/output/books.csv
```

### Data Persistence

All Docker configurations use volumes for persistent data:
- `./docker-data/data/` - SQLite databases and progress files
- `./docker-data/output/` - Run outputs and results
- `./docker-data/config/` - Authentication and configuration files
- `./docker-data/logs/` - Application logs
- `./docker-data/staging/` - Temporary processing files

See `examples/docker/README.md` for comprehensive Docker documentation and credential management details.