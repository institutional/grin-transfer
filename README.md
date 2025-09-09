# GRIN-to-S3

A pipeline for extracting page scans, metadata, and OCR from Google Books GRIN (Google Return Interface) by academic libraries who have partnered with Google Books. The tool will programmatically download and store book archives to local or S3-compatible storage. 

In typical usage, you will **collect** a local copy of your library's Google Books metadata, then **request** that titles be made available for download. Once processed, you can **sync** those archives to your local storage solution. When fully synced, you should have a complete set of all decrypted book scans, OCR, and metadata.

## Table of Contents

- [Prerequisites](#prerequisites)
  - [Set up OAuth2 client credentials](#set-up-oauth2-client-credentials)
- [Installation](#installation-requirements)
  - [Docker (recommended)](#quick-start-with-docker-recommended)
  - [Local](#local-installation)
- [Pipeline concepts](#pipeline-concepts)
  - [Runs](#runs)
  - [Storage](#storage)
  - [Staging](#staging)
- [Pipeline steps](#pipeline-steps)
- [Core commands](#core-commands)
  - [1. Book Collection: `grin.py collect`](#1-book-collection-grinpy-collect)
  - [2. Processing Management: `grin.py process`](#2-processing-management-grinpy-process)
  - [3. Sync Pipeline: `grin.py sync pipeline`](#3-sync-pipeline-grinpy-sync-pipeline)
  - [4. Metadata enrichment: `grin.py enrich`](#4-metadata-enrichment-grinpy-enrich)
  - [5. Storage Management: `grin.py storage`](#5-storage-management-grinpy-storage)
- [File outputs](#file-outputs)
  - [Data dictionary](#data-dictionary)
- [Database administration](#database-administration)
  - [SQL queries for admin tasks](#sql-queries-for-admin-tasks)
- [Storage backends](#storage-backends)
  - [Local filesystem](#local-storage)
  - [AWS S3](#aws-s3)
  - [Cloudflare R2](#cloudflare-r2)
  - [Google Cloud Storage (GCS)](#google-cloud-storage-gcs)
- [Development](#development)

## Prerequisites

1. **GRIN Library Directory**: You should know your "library directory", or the path in GRIN where your books are made available. For example, given the URL https://books.google.com/libraries/Harvard/_all_books, the "library directory" is "Harvard." This value is case-sensitive.
2. **GRIN Google Account**: You should have credentials for the Google account that was given to a Google partner manager for access to GRIN. This will be the same account that you use to log in to GRIN as a human.
3. **GRIN archive decryption key**: Book archives stored in GRIN are encrypted. You'll need your GPG decryption passphrase from Google. It will be a short text file containing random words.

Save this file as `gpg_passphrase.asc` in `~/.config/grin-to-s3/` (the tool will search this location automatically).

4. _(Optional)_ We recommend you set up S3-like cloud storage to extract large collections. GRIN-to-S3 will use those credentials to transfer decrypted book archives and metadata. For smaller collections, you can use any filesystem reachable from where you run this tool.

### Set up OAuth2 client credentials

The pipeline uses OAuth2 to authenticate with Google's GRIN interface. Before running any pipeline commands, you must configure authentication using your Google account credentials that have GRIN access.

Using your **GRIN Google Account**:

1. Go to https://console.developers.google.com and create a new project.
2. Click the 'Credentials' tab.
3. Select 'OAuth client ID' in the credential type dropdown.
4. Click 'Configure consent screen'.
5. Enter any name in the 'Product name' box, e.g. 'Scripted Access to GRIN'.
6. Click 'Save'.
7. There should now be a radio list titled 'Application type'. Click 'Desktop App'. Enter
any name. Click 'Create'.
8. A dialog box should appear that has 'Here is your client ID' and 'Here is
your client secret' boxes. Ignore this, just click 'OK'.
9. Click the 'download' button to the right of the credentials. The button is a
down arrow with a line under it.
10. Move that file into your home directory at `.config/grin-to-s3/client_secret.json`

[↑ Back to the top](#grin-to-s3)

## Installation

The GRIN-to-S3 pipeline can be installed via docker, or directly on a target Linux-like or MacOS machine.

### Quick start with docker (recommended)

We've provided a `grin-docker` wrapper script to make accessing the tool straightforward. 

The first time you run `auth setup`, you will be prompted to log in via your web browser to your **GRIN Google Account** using the **OAuth2 client credentials** that you downloaded to your home directory. After that the pipeline will be able to run using those credentials. 

Assuming Docker is running on your local or host machine:

```bash

# Build the docker environment and configure authentication with GRIN
./grin-docker auth setup

# Then run a sample collection using filesystem storage
./grin-docker collect --run-name test_run --library-directory YOUR_LIBRARY_DIRECTORY --storage local --storage-config base_path=docker-data/storage --limit 5

# Then run a sample sync using that collection (assuming GRIN has titles available for download)
./grin-docker sync pipeline --run-name test_run --queue converted

# ...synced book archives and other metadata will be in docker-data/storage

# You can also connect directly to the docker container to explore it
./grin-docker bash 
```

**MacOS note**: Docker on MacOS will be comparatively slow to decode encrypted archives because it does not have full access to the CPU. We recommend that you install the application directly on Macs when running in any kind of production capacity. This limitation does not apply to running under Docker on Linux.

### Local installation

- Requires Python 3.12+
- Python dependencies are managed via `pyproject.toml`
- System dependencies: **gpg** for decrypting book archives

```bash
# Create and activate a virtual environment
source ./activate.sh

# Install dependencies
pip install -e "."

# Interactively configure authentication with the GRIN interface
python grin.py auth setup

# For remote servers, SSH sessions, or cloud instances, use:
python grin.py auth setup --remote-auth

# Then run a sample collection 
python grin.py collect --run-name test_run --library-directory YOUR_LIBRARY_DIRECTORY --storage local --storage-config base_path=/tmp/grin-to-s3-storage --limit 10

# And sync those 10 files
python grin.py sync pipeline --run-name test_run --queue converted
```

[↑ Back to the top](#grin-to-s3)
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

[↑ Back to the top](#grin-to-s3)
   

## Pipeline steps
Each run begins with the **collection** step, where book metadata is first downloaded from GRIN and stored locally for evaluation and processing. Collecting all book metadata for very large (1 million+ book) collections usually takes under an hour.

GRIN needs to "convert" archives to make them available for download. This pipeline can **request** conversions up to Google's maximum of 50,000 queued.

The next step is to start the **sync pipeline**, in which the original book archives are downloaded from GRIN, decrypted, and uploaded or copied to storage. 

If the data is valuable to you, there is a command to **enrich** local metadata with additional fields that GRIN makes available only through direct querying of the TSV endpoint, mostly related to the OCR quality.

Finally, there are utilities for managing your storage and gathering statistics about the run.

[↑ Back to the top](#grin-to-s3) 

## Core commands

The pipeline provides a unified command-line interface via `grin.py`, with subcommands that map to the pipeline steps. When installed via docker, for convenience these can all be called from outside the contanier as `grin-docker <subcommand>`.

### 1. Book Collection: `grin.py collect`

Collects book metadata from GRIN into the local SQLite database and writes the run configuration with relevant parameters.

```bash
# Basic collection using CloudFlare R2 as the remote storage
python grin.py collect --run-name RUN_NAME \
  --library-directory YOUR_LIBRARY_DIRECTORY \
  --storage r2 

# Local storage (no buckets required, but you must specify the path to the directory where files will go)
python grin.py collect --run-name "local_test" --library-directory YOUR_LIBRARY_DIRECTORY --storage local --storage-config base_path=/tmp/storage

```

**Key options:**
- `--library-directory`: GRIN library directory name (required, e.g., Harvard, MIT, Yale)
- `--run-name`: Named run for organized output (defaults to timestamp)
- `--storage`: Storage backend (`local`, `minio`, `r2`, `s3`, `gcs`)
- `--limit`: Limit books for testing

### 2. Processing Management: `grin.py process`

Request book processing via GRIN conversion system.

```bash
python grin.py process request --run-name RUN_NAME --limit 1000
```

### 3. Sync Pipeline: `grin.py sync pipeline`

Download converted books from GRIN to storage with database tracking. You must either specify a `queue`, or a list of books by derived status, or `--barcodes`, to pass a short list of barcodes on the command line, or `--barcodes-file`, a path to a file containing barcodes.

Unless you specify `--force`, archives which are in storage and are known to be identical to those in GRIN will be skipped.

#### Sync books based on GRIN status
Provide either `--queue converted` (sync all books in GRIN's `_converted` list which are ready to download) or `--queue previous` (books with GRIN's `PREVIOUSLY_DOWNLOADED` status). 

If `--queue previous` is supplied, all books marked as unsynced with the GRIN status `PREVIOUSLY_DOWNLOADED` will attempt to be synced. If a book is found ready to download in GRIN, it will be synced. If it is not in the converted state, the pipeline will request that it be added to GRIN's conversion queue. Typically you will use `--queue previous` if you have downloaded archives that predate this tool but would like to gather a unified collection.

You can specify multiple queues and they will be run in order.

#### Sync books by barcode

Alternatively, regardless of GRIN status, you can request specific barcodes be synced. For small numbers of barcodes, you can pass them on the command line as `--barcodes`. For larger sets, pass `--barcodes-files` with a path to a file containing one barcode per line.

If a barcode is specified but not immediately available from GRIN, the software will issue a request to convert it.

```bash
# Sync converted books to storage 
python grin.py sync pipeline --run-name RUN_NAME --queue converted
```
**Required options**

One of these must be specified
- `--queue`: Which selection of books to pull from, either `converted` or `previous`
- `--barcodes` or `--barcodes-file`: lists of specific barcodes to sync

**Other common pipeline options:**
- `--limit`: Limit number of books to sync
- `--force`: Overwrite existing files even if checks think the versions are identical
- `--dry-run`: Show what would happen in a sync with the provided options, but do no work

### 4. Metadata enrichment: `grin.py enrich` 

Iterate over un-enriched titles and download additional metadata.

```bash
python grin.py enrich --run-name RUN_NAME
```

### 5. Storage Management: `grin.py storage`

Manage storage buckets with fast listing and deletion.

**Commands:**
- `ls`: List contents of all storage buckets with file counts and sizes
- `rm`: Remove all contents from specified bucket (raw, meta, or full)
- `cp`: Copy files from a bucket to your local filesystem

[↑ Back to the top](#grin-to-s3)

## File outputs

Each run creates organized output in `output/{run_name}/`:
- `books.db` - SQLite database with all book records and sync status
- `books_export.csv` - Local CSV export with all book metadata
- `process_summary.json` - Previous run history, counts, and timestamps
- `run_config.json` - Configuration file (generated as part of the collect step)

### Data Dictionary

<details>
<summary><strong>CSV Metadata Fields</strong></summary>

The CSV export contains comprehensive metadata drawn from multiple sources through a three-step process:

1. **Collection step** (`grin.py collect`) - Gathers basic metadata from GRIN's HTML listing pages, including barcode, timestamps, and processing state
2. **Enrichment step** (`grin.py enrich`) - Makes additional API calls to retrieve detailed quality metrics, conditions, and analysis scores from GRIN's TSV data export
3. **Sync pipeline** (`grin.py sync`) - Extracts detailed bibliographic metadata from MARC records embedded in METS XML files during archive syncing

**Data availability timing:**
- Collection fields are populated immediately after running the collection step.
- Enrichment fields remain empty until you run the enrichment step.
- MARC fields are populated during sync when METS XML files are available.

Each book is represented as a single row with the following fields:

#### Core Identification
- **Barcode** - String. The unique barcode identifier used by your institution and Google Books to identify each volume *(Collection step)*
- **Title** - String. Book title as reported by GRIN *(Collection step)*

#### GRIN Processing Timestamps
Dates from Google's GRIN system are in YYYY/MM/DD HH:MM format:

- **Scanned Date** - Datetime (YYYY/MM/DD HH:MM). Date when physical book was scanned by Google *(Collection step)*
- **Converted Date** - Datetime (YYYY/MM/DD HH:MM). Date when scan was converted to searchable format *(Collection step)*
- **Downloaded Date** - Datetime (YYYY/MM/DD HH:MM). Date when archive became available for download *(Collection step)*
- **Processed Date** - Datetime (YYYY/MM/DD HH:MM). Date when Google completed processing *(Collection step)*
- **Analyzed Date** - Datetime (YYYY/MM/DD HH:MM). Date when Google completed content analysis *(Collection step)*
- **OCR Date** - Datetime (YYYY/MM/DD HH:MM). Date when optical character recognition was last performed on the book images *(Collection step)*
- **Google Books Link** - URL. Public-facing Google Books URL for this volume *(Collection step)*
- **Processing Request Timestamp** - ISO8601 Datetime (UTC). When this book was submitted to Google's conversion queue by this tool *(Internal)*

#### GRIN State and Quality Metrics
Most fields populated from Google's detailed enrichment TSV data accessed via the GRIN API:

- **GRIN State** - String/Enum. Current processing state. Common values: `CONVERTED`, `CHECKED_IN`, `NEW`, `PREVIOUSLY_DOWNLOADED`, `NOT_AVAILABLE_FOR_DOWNLOAD` *(Collection step - basic state, enhanced by enrichment step)*
- **GRIN Viewability** - String/Enum. How Google Books displays this title. Values: `VIEW_FULL`, `VIEW_METADATA`, `VIEW_NONE`, `VIEW_SNIPPET` *(Enrichment step)*
- **GRIN Opted Out** - String ("true"/"false"/empty). Whether volume was opted out post-scan *(Enrichment step)*
- **GRIN Conditions** - String. Comma-separated integers describing physical condition at check-in (see Google's GRIN Overview.pdf) *(Enrichment step)*
- **GRIN Scannable** - String ("true"/"false"/empty). Whether volume was deemed scannable (note: some non-scannable books still have scans) *(Enrichment step)*
- **GRIN Tagging** - String ("true"/"false"/empty). Internal Google tagging status *(Enrichment step)*
- **GRIN Audit** - String. Quality review status - may contain percentage values (e.g., "0%") or be empty *(Enrichment step)*
- **GRIN Material Error %** - String. Percentage format (e.g., "5%") indicating material scanning errors *(Enrichment step)*
- **GRIN Overall Error %** - String. Percentage format indicating overall processing errors *(Enrichment step)*
- **GRIN Claimed** - String ("true"/"false"/empty). Whether volume was claimed in Google's system *(Enrichment step)*
- **GRIN OCR Analysis Score** - String. Quality score for optical character recognition [0-100] *(Enrichment step)*
- **GRIN OCR GTD Score** - String. Google's Garbage Text Detection score for OCR quality [0-100] *(Enrichment step)*
- **GRIN Digitization Method** - String/Enum. Scanning method used. Values: `NON_DESTRUCTIVE`, `SHEETFED`, `DIGIFEED` *(Enrichment step)*
- **GRIN Check-In Date** - Datetime (YYYY/MM/DD HH:MM). When volume was checked in to Google's facility *(Enrichment step)*
- **GRIN Source Library Bibkey** - String. Bibliographic key from originating library *(Enrichment step)*
- **GRIN Rubbish** - String ("true"/"false"/empty). Whether volume was flagged as unsuitable for processing *(Enrichment step)*
- **GRIN Allow Download Updated Date** - Datetime (YYYY/MM/DD HH:MM). When download permissions were last updated *(Enrichment step)*
- **GRIN Viewability Updated Date** - Datetime (YYYY/MM/DD HH:MM). When viewing permissions were last updated *(Enrichment step)*
- **Enrichment Timestamp** - ISO8601 Datetime (UTC). When GRIN enrichment data was last retrieved *(Internal)*

#### MARC Bibliographic Metadata
Extracted from Google Books' METS XML files containing institutional MARC records:

- **MARC Control Number** - String. MARC control number from institutional catalog (MARC field 001) *(Sync pipeline - METS extraction)*
- **MARC Date Type** - String. MARC date type field indicating how dates should be interpreted (e.g., "Publication date and copyright date", "Range of years") (MARC 008 position 06) *(Sync pipeline - METS extraction)*
- **MARC Date 1** - String. Four-digit year, "9999", or empty (MARC 008 positions 07-10) *(Sync pipeline - METS extraction)*
- **MARC Date 2** - String. Four-digit year, "9999", or empty (MARC 008 positions 11-14) *(Sync pipeline - METS extraction)*
- **MARC Language** - String. Three-letter language code (MARC 008 positions 35-37, [ISO 639-2](https://www.loc.gov/marc/languages/)) *(Sync pipeline - METS extraction)*
- **MARC LCCN** - String. Library of Congress Control Number (MARC field 010) *(Sync pipeline - METS extraction)*
- **MARC LC Call Number** - String. Library of Congress call number classification (MARC field 050) *(Sync pipeline - METS extraction)*
- **MARC ISBN** - String. Comma-separated ISBN values (format varies: may include additional publication info) (MARC field 020) *(Sync pipeline - METS extraction)*
- **MARC OCLC Numbers** - String. Comma-separated OCLC numbers, typically formatted as "(OCoLC)00055898" (MARC field 035) *(Sync pipeline - METS extraction)*
- **MARC Title** - String. Main title from MARC title statement (MARC field 245 subfield $a) *(Sync pipeline - METS extraction)*
- **MARC Title Remainder** - String. Subtitle/remainder from MARC title statement (MARC field 245 subfield $b) *(Sync pipeline - METS extraction)*
- **MARC Author Personal** - String. Personal name from MARC author field (MARC field 100) *(Sync pipeline - METS extraction)*
- **MARC Author Corporate** - String. Corporate/organizational author (may include government entities) (MARC field 110) *(Sync pipeline - METS extraction)*
- **MARC Author Meeting** - String. Meeting/conference name as author (MARC field 111) *(Sync pipeline - METS extraction)*
- **MARC Subjects** - String. Comma-separated subject headings (e.g., "English language", "Botany") (MARC field 650) *(Sync pipeline - METS extraction)*
- **MARC Genres** - String. Comma-separated genre/form terms (e.g., "Fiction", "History") (MARC field 655) *(Sync pipeline - METS extraction)*
- **MARC General Note** - String. General notes field from MARC record (MARC field 500) *(Sync pipeline - METS extraction)*
- **MARC Extraction Timestamp** - ISO8601 Datetime (UTC). When MARC metadata was extracted from METS XML *(Internal)*

*Note: MARC dates of "9999" indicate ongoing publications or uncertain end dates. Do not use simple "greater than" comparisons without filtering these values.*

#### Storage and Sync Tracking
Pipeline-managed fields for tracking storage backend operations:

- **Storage Path** - String. Full path to decrypted archive in storage (e.g., `bucket-raw/BARCODE/BARCODE.tar.gz`) *(Internal)*
- **Last ETag Check** - ISO8601 Datetime (UTC). When storage ETag was last verified for integrity *(Internal)*
- **Encrypted ETag** - String. ETag hash of encrypted archive for integrity verification *(Internal)*
- **Is Decrypted** - Integer (0/1). Whether archive has been decrypted locally *(Internal)*
- **Sync Timestamp** - ISO8601 Datetime (UTC). When storage sync was last completed successfully *(Internal)*

#### Record Keeping
- **Created At** - ISO8601 Datetime (UTC). When book record was first created in local database *(Internal)*
- **Updated At** - ISO8601 Datetime (UTC). When book record was last modified *(Internal)*

</details>

<details>
<summary><strong>JSONL Full-Text Files</strong></summary>

The pipeline generates JSONL files containing page-by-page OCR text for full-text search and analysis.

#### File Structure
- **Filename**: `{barcode}_ocr.jsonl[.gz]`
- **Format**: One JSON-encoded string per line (JSON Lines format)
- **Compression**: Optional gzip compression based on configuration

#### Content Format
Each line contains a JSON-encoded string with the OCR text for one page:

```jsonl
"This is the OCR text content from page 1 of the book..."
"This is the OCR text content from page 2 of the book..."
"This is the OCR text content from page 3 of the book..."
```

</details>

<details>
<summary><strong>Data Sources</strong></summary>

The metadata combines information from multiple authoritative sources:

#### Google Books GRIN API
- Core book identification (barcode, Google Books links)
- Physical book processing timestamps (scanned, converted, downloaded, processed, analyzed, OCR dates)
- OCR state and quality metrics (viewability, error percentages, OCR scores)

#### Google Books GRIN Enrichment TSV
- Detailed quality and condition metadata
- Digitization method and facility information

#### Institutional MARC Records
- Extracted from Google's METS XML files
- Author, title, subject, and classification information

#### Pipeline Processing
- Storage backend tracking and sync status
- Export timestamps and processing metadata

</details>

## Database administration

### SQL queries for admin tasks

The pipeline uses SQLite to track book metadata, processing status, and sync progress. For direct database queries and administrative tasks, see the [SQL queries documentation](docs/sql-queries.md).

This documentation includes:
- Status breakdown queries for GRIN fields and pipeline stages
- Barcode list export queries for batch operations
- Integration with the sync pipeline using exported barcode lists
- Troubleshooting queries for finding errors and bottlenecks
- Performance analysis queries for monitoring pipeline throughput

### Storage backends

<details>
<summary><strong>Local filesystem</strong></summary>

If you have a large enough locally-mounted filesystem, you can sync directly to it. This process is significantly faster than using a block storage system, though typically the limiting factor on syncs is GRIN, not network egress.

When using local storage, you must specify a `base_path` as the directory where books and metadata will be stored. The pipeline will organize the raw, full, and metadata files underneath that.

```bash
python grin.py collect --run-name "local" --library-directory YOUR_LIBRARY_DIRECTORY --storage local --storage-config base_path=/var/grin-books
```
</details>

<details>
<summary><strong>AWS S3</strong></summary>

In AWS, create three buckets for raw archives, full-text, and metadata. Create an IAM user with access to those buckets, and an access key and ID associated with that user.

**Sample IAM policy**

```
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Action": [
				"s3:GetObject",
				"s3:PutObject",
				"s3:DeleteObject",
				"s3:ListBucket",
				"s3:GetBucketLocation",
				"s3:AbortMultipartUpload",
				"s3:ListMultipartUploadParts"
			],
			"Resource": [
				"arn:aws:s3:::lizadaly-grin-raw",
				"arn:aws:s3:::lizadaly-grin-raw/*",
				"arn:aws:s3:::lizadaly-grin-meta",
				"arn:aws:s3:::lizadaly-grin-meta/*",
				"arn:aws:s3:::lizadaly-grin-full",
				"arn:aws:s3:::lizadaly-grin-full/*"
			]
		},
		{
			"Effect": "Allow",
			"Action": [
				"s3:ListAllMyBuckets"
			],
			"Resource": "*"
		}
	]
}
```

Then on the host running grin-to-s3, use an AWS credentials file:

```bash
mkdir -p ~/.aws
cat > ~/.aws/credentials << EOF
[default]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
EOF
```


Then run collection with the appropriate bucket names
```
python grin.py collect --run-name "s3" --library-directory YOUR_LIBRARY_DIRECTORY --storage s3 --bucket-raw YOUR_RAW_BUCKET --bucket-full YOUR_FULL_BUCKET --bucket-meta YOUR_META_BUCKET
```

Any other boto-compatible access model should also work; for example if run on an EC2 with an instance role that has write access to the bucket.
</details>

<details>
<summary><strong>Cloudflare R2</strong></summary>

For convenience, a sample configuration file has been provided where you can specify credentials and configuration.

```bash 
cp examples/auth/r2-credentials-template.json ~/.config/grin-to-s3/r2_credentials.json

# Edit `r2_credentials.json` with your access information and bucket names, then test with:
python grin.py collect --run-name "r2" --library-directory YOUR_LIBRARY_DIRECTORY --storage r2
```
</details>

<details>
<summary><strong>Google Cloud Storage (GCS)</strong></summary>

You will need the three buckets, plus your GCS project ID.

```bash
# Configure Google Cloud authentication
gcloud auth application-default login

# Create GCS buckets
gsutil mb gs://your-grin-raw
gsutil mb gs://your-grin-meta  
gsutil mb gs://your-grin-full

# Run collection with GCS storage
python grin.py collect --run-name "gcs" --library-directory YOUR_LIBRARY_DIRECTORY \
  --storage gcs \
  --bucket-raw your-grin-raw \
  --bucket-meta your-grin-meta \
  --bucket-full your-grin-full \
  --storage-config project=YOUR_GCS_PROJECT_ID
```
</details>

[↑ Back to the top](#grin-to-s3)

## Development

If you would like to contribute enhancements or bug-fixes to the tool, you can work on it locally:

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

[↑ Back to the top](#grin-to-s3)
