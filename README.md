# GRIN-to-S3

A pipeline for extracting page scans, metadata, and OCR from Google Books GRIN (Google Return Interface) by academic libraries who have partnered with Google Books. The tool will programmatically download and store book archives to local or S3-compatible storage. 

## Prerequisites

1. **GRIN Library Directory** You should know your "library directory", or the path in GRIN where your books are made available. For example, given the URL https://books.google.com/libraries/Harvard/_all_books, the "library directory" is "Harvard." This value is case-sensitive.
2. **GRIN Google Account** You should have credentials for the Google account that was given to a Google partner manager for access to GRIN. This will be the same account that you use to log in to GRIN as a human. 
3. **GRIN archive decryption key** Book archives stored in GRIN are encrypted. You'll need your GPG decryption passphrase from Google. It will be a short text file containing random words.
4. (Optional) We recommend you set up S3-like cloud storage to extract large collections. GRIN-to-S3 will use those credentials to transfer decrypted book archives and metadata. For smaller collections, you can use any filesystem reachable from where you run this tool.

## Installation requirements

The GRIN-to-S3 pipeline can be installed via docker, or directly on a target Linux-like or MacOS machine.

### Quick start with docker (recommended)

We've provided a `grin-docker` wrapper script to make accessing the tool straightforward. 

The first time you run `auth setup`, you will be prompted to log in via your web browser to your **GRIN Google Account**. After that the pipeline will be able to run using those credentials. 

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
Downloaded archives are ultimately synced to your **storage**, which may be a cloud-based block storage like AWS S3, Google Cloud Storage (GCS), Cloudflare R2, or a local filesystem. Configuring storage is required.

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
- `--storage`: Storage backend (`local`, `minio`, `r2`, `s3`, `gcs`)
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



## File Outputs

Each run creates organized output in `output/{run_name}/`:
- `books.db` - SQLite database with all book records and sync status
- `books_{timestamp}.csv` - Timestamped CSV export
- `progress.json` - Collection progress for resume capability
- `run_config.json` - Configuration for other scripts to auto-detect settings


### Storage Backends

**Local Storage:**
```bash
python grin.py collect --run-name "local" --library-directory Harvard --storage local --storage-config base_path=/tmp/grin-books
```

**Cloudflare R2:**
```bash 
cp examples/docker/r2-credentials-template.json ~/.config/grin-to-s3/r2_credentials.json

# Edit `r2_credentials.json` with your access information and bucket names, then test with:
python grin.py collect --run-name "r2" --library-directory Harvard --storage r2
```


**AWS S3:**
```bash
# Configure AWS credentials (choose one method):

# Method 1: AWS credentials file
mkdir -p ~/.aws
cat > ~/.aws/credentials << EOF
[default]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
EOF

# Method 2: Environment variables
export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY
export AWS_DEFAULT_REGION=us-east-1

# Method 3: AWS CLI configuration
aws configure

# Then run collection
python grin.py collect --run-name "s3" --library-directory Harvard --storage s3 
```

**Google Cloud Storage (GCS):**
```bash
# Configure Google Cloud authentication
gcloud auth application-default login

# Create GCS buckets (optional - will be created automatically if they don't exist)
gsutil mb gs://your-grin-raw
gsutil mb gs://your-grin-meta  
gsutil mb gs://your-grin-full

# Run collection with GCS storage
python grin.py collect --run-name "gcs" --library-directory Harvard \
  --storage gcs \
  --bucket-raw your-grin-raw \
  --bucket-meta your-grin-meta \
  --bucket-full your-grin-full \
  --storage-config project=your-google-cloud-project-id
```

## Development

```bash
# Install with development dependencies
pip install -e ".[dev]"

# Run linting and formatting
ruff check --fix && ruff format

# Run type checking
mypy src/grin_to_s3 --explicit-package-bases

# Run tests
python -m pytest
```



