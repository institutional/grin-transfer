# Manual Testing Scripts

This directory contains scripts for manual testing that require real credentials and external services.

## End-to-End Testing

### `e2e_test.py`

Comprehensive end-to-end testing script that validates the entire pipeline with real credentials.

**What it does:**
- Checks out the repository in a temporary directory
- Performs fresh installation from scratch
- Runs `python grin.py sync pipeline --queue converted --limit 20` on the local machine
- Builds Docker image from scratch
- Runs `./grin-docker sync pipeline --queue converted --limit 20` in Docker

**Requirements:**
- Real GRIN credentials in `~/.config/grin-to-s3/client_secret.json`
- Docker installed and running
- Active internet connection
- Storage backend configured

**Usage:**
```bash
# Run E2E tests with a specific run name
python tests/manual/e2e_test.py --run-name test_e2e_20240125

# Run with custom limit
python tests/manual/e2e_test.py --run-name test_custom --limit 10

# Keep temporary directory for debugging
python tests/manual/e2e_test.py --run-name test_debug --no-cleanup
```

### Manual Testing Only

This E2E test is designed for manual execution to validate functionality across multiple configurations before releases or major changes.