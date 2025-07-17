# Docker Guide for GRIN-to-S3

This guide provides comprehensive instructions for running GRIN-to-S3 in Docker containers, designed for users with minimal Docker experience.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Quick Start](#quick-start)
4. [Storage Configuration](#storage-configuration)
5. [Credential Management](#credential-management)
6. [Common Workflows](#common-workflows)
7. [Troubleshooting](#troubleshooting)
8. [Advanced Usage](#advanced-usage)
9. [Security Best Practices](#security-best-practices)

## Prerequisites

### Docker Installation

**For macOS:**
```bash
# Install Docker Desktop
# Download from: https://www.docker.com/products/docker-desktop/

# Verify installation
docker --version
docker-compose --version
```

**For Linux (Ubuntu/Debian):**
```bash
# Install Docker
sudo apt-get update
sudo apt-get install docker.io docker-compose

# Add user to docker group
sudo usermod -aG docker $USER
newgrp docker

# Verify installation
docker --version
docker-compose --version
```

**For Windows:**
Install Docker Desktop from the official website and enable WSL2 backend.

### System Requirements

- **Memory**: 2GB minimum, 4GB recommended
- **Disk Space**: 10GB minimum for Docker images and data
- **Network**: Internet connection for downloading images and accessing GRIN

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/harvard-idi/grin-to-s3-wip.git
cd grin-to-s3-wip
```

### 2. Build the Docker Image

```bash
# Build the image
docker build -t grin-to-s3 .

# Verify the build
docker images | grep grin-to-s3
```

### 3. Create Data Directories

```bash
# Create directories for persistent data
mkdir -p docker-data/{data,output,config,logs,staging}

# Set proper permissions (Linux/macOS)
chmod -R 755 docker-data/
```

## Quick Start

### Convenience Script

For easier command execution, use the included `grin-docker` wrapper script:

```bash
# Make the script executable
chmod +x grin-docker

# Now you can run commands more easily
./grin-docker python grin.py --help
./grin-docker python grin.py collect --run-name test --storage minio --limit 5
./grin-docker bash  # Interactive shell
```

The script automatically starts services and passes all arguments to the container.

### Option 1: Development with MinIO (Recommended for First-Time Users)

MinIO provides a local S3-compatible storage service that's perfect for testing.

```bash
# Start MinIO and the application
docker-compose -f docker-compose.dev.yml up -d

# Wait for services to start (about 30 seconds)
docker-compose -f docker-compose.dev.yml logs -f

# Access MinIO console at http://localhost:9001
# Username: minioadmin
# Password: minioadmin123
```

**Run your first collection:**
```bash
# Set up OAuth2 credentials (run once)
./grin-docker python grin.py auth setup

# Collect a small set of books for testing
./grin-docker python grin.py collect \
  --run-name my_first_run \
  --library-directory Harvard \
  --storage minio \
  --bucket-raw grin-raw \
  --bucket-meta grin-meta \
  --bucket-full grin-full \
  --limit 5
```

**Alternative using docker-compose directly:**
```bash
# Set up OAuth2 credentials (run once)
docker-compose -f docker-compose.dev.yml exec grin-to-s3 python grin.py auth setup

# Collect a small set of books for testing
docker-compose -f docker-compose.dev.yml exec grin-to-s3 python grin.py collect \
  --run-name my_first_run \
  --library-directory Harvard \
  --storage minio \
  --bucket-raw grin-raw \
  --bucket-meta grin-meta \
  --bucket-full grin-full \
  --limit 5
```

### Option 2: Production with Cloud Storage

For production use with Cloudflare R2 or AWS S3:

```bash
# 1. Configure credentials (see Credential Management section)
cp examples/docker/r2-credentials-template.json examples/docker/r2-credentials.json
# Edit the file with your actual credentials

# 2. Set up OAuth2 credentials
docker-compose -f examples/docker/docker-compose.r2.yml run --rm grin-to-s3 auth setup

# 3. Start the application
docker-compose -f examples/docker/docker-compose.r2.yml up -d
```

## Storage Configuration

GRIN-to-S3 supports multiple storage backends. Each requires different configuration:

### Local Storage (Development Only)

```yaml
# docker-compose.yml
environment:
  - GRIN_STORAGE_TYPE=local
  - GRIN_BUCKET_RAW=raw-data
  - GRIN_BUCKET_META=metadata
  - GRIN_BUCKET_FULL=fulltext
```

### MinIO (Development)

MinIO is auto-configured inside Docker containers. Simply use `--storage minio` with bucket names:

```bash
# MinIO is automatically configured to use http://minio:9000
docker-compose -f docker-compose.dev.yml exec grin-to-s3 python grin.py collect \
  --storage minio \
  --bucket-raw grin-raw \
  --bucket-meta grin-meta \
  --bucket-full grin-full
```

### Cloudflare R2 (Production)

```yaml
# examples/docker/docker-compose.r2.yml
environment:
  - GRIN_STORAGE_TYPE=r2
  - GRIN_BUCKET_RAW=my-raw-bucket
  - GRIN_BUCKET_META=my-meta-bucket
  - GRIN_BUCKET_FULL=my-full-bucket
```

### AWS S3 (Production)

```yaml
# examples/docker/docker-compose.s3.yml
environment:
  - GRIN_STORAGE_TYPE=s3
  - AWS_DEFAULT_REGION=us-east-1
  - GRIN_BUCKET_RAW=my-raw-bucket
  - GRIN_BUCKET_META=my-meta-bucket
  - GRIN_BUCKET_FULL=my-full-bucket
```

## Credential Management

### OAuth2 Credentials (Required)

GRIN-to-S3 requires OAuth2 authentication with Google:

1. **Get OAuth2 credentials from Google Cloud Console:**
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select existing
   - Enable Google Books API
   - Create OAuth2 credentials
   - Download as `client_secret.json`

2. **Place credentials in secure location:**
   ```bash
   # Create the configuration directory
   mkdir -p ~/.config/grin-to-s3
   
   # Copy your client_secret.json to the standard location
   cp /path/to/client_secret.json ~/.config/grin-to-s3/client_secret.json
   ```

3. **Run initial setup with port forwarding:**
   ```bash
   # Default port (58432)
   docker-compose -f <your-config>.yml run --rm --service-ports grin-to-s3 python grin.py auth setup
   
   # Custom port
   GRIN_OAUTH_PORT=59999 docker-compose -f <your-config>.yml run --rm --service-ports grin-to-s3 python grin.py auth setup
   ```
   
   **Important**: The `--service-ports` flag is required to forward the OAuth2 server port.

### Storage Credentials

#### Cloudflare R2

Create `examples/docker/r2-credentials.json`:
```json
{
  "endpoint_url": "https://your-account-id.r2.cloudflarestorage.com",
  "access_key": "your-r2-access-key-id",
  "secret_key": "your-r2-secret-access-key",
  "bucket_raw": "your-raw-bucket-name",
  "bucket_meta": "your-metadata-bucket-name",
  "bucket_full": "your-fulltext-bucket-name"
}
```

#### AWS S3

Create `examples/docker/aws-credentials.json`:
```json
{
  "access_key_id": "your-aws-access-key-id",
  "secret_access_key": "your-aws-secret-access-key",
  "region": "us-east-1",
  "bucket_raw": "your-raw-bucket-name",
  "bucket_meta": "your-metadata-bucket-name",
  "bucket_full": "your-fulltext-bucket-name"
}
```

#### Environment Variables (Alternative)

Instead of credential files, you can use environment variables:

```yaml
# docker-compose.yml
environment:
  # R2 credentials
  - R2_ENDPOINT_URL=https://your-account.r2.cloudflarestorage.com
  - R2_ACCESS_KEY_ID=your-access-key
  - R2_SECRET_ACCESS_KEY=your-secret-key
  
  # AWS credentials
  - AWS_ACCESS_KEY_ID=your-access-key
  - AWS_SECRET_ACCESS_KEY=your-secret-key
  - AWS_DEFAULT_REGION=us-east-1
```

## Common Workflows

### Complete Pipeline Workflow

This example shows a complete workflow from collection to export:

```bash
# 1. Start the environment
docker-compose -f docker-compose.dev.yml up -d

# 2. Set up authentication (run once)
docker-compose -f docker-compose.dev.yml exec grin-to-s3 python grin.py auth setup

# 3. Collect books
docker-compose -f docker-compose.dev.yml exec grin-to-s3 python grin.py collect \
  --run-name production_run \
  --library-directory Harvard \
  --storage minio \
  --limit 1000

# 4. Request processing
docker-compose -f docker-compose.dev.yml exec grin-to-s3 python grin.py process request \
  --run-name production_run \
  --limit 500

# 5. Monitor processing (run periodically)
docker-compose -f docker-compose.dev.yml exec grin-to-s3 python grin.py process monitor \
  --run-name production_run

# 6. Sync converted books
docker-compose -f docker-compose.dev.yml exec grin-to-s3 python grin.py sync pipeline \
  --run-name production_run

# 7. Enrich with metadata
docker-compose -f docker-compose.dev.yml exec grin-to-s3 python grin.py enrich \
  --run-name production_run

# 8. Export results
docker-compose -f docker-compose.dev.yml exec grin-to-s3 python grin.py export \
  --run-name production_run \
  --output /app/output/final_books.csv

# 9. Copy results to host
docker cp grin-to-s3-dev:/app/output/final_books.csv ./final_books.csv
```

### Monitoring and Logs

```bash
# View application logs
docker-compose logs grin-to-s3

# Follow logs in real-time
docker-compose logs -f grin-to-s3

# View specific log files
docker-compose exec grin-to-s3 tail -f /app/logs/grin_pipeline_*.log

# Check storage status
docker-compose exec grin-to-s3 python grin.py storage ls --run-name production_run
```

### Data Management

```bash
# View run status
docker-compose exec grin-to-s3 python grin.py reports view --run-name production_run

# Check sync status
docker-compose exec grin-to-s3 python grin.py sync status --run-name production_run

# Access SQLite database
docker-compose exec grin-to-s3 sqlite3 /app/data/production_run/books.db

# Copy data to host
docker cp grin-to-s3-dev:/app/data ./backup-data
```

## Troubleshooting

### Common Issues

#### 1. Permission Denied Errors

**Problem:** Files cannot be written to mounted volumes.

**Solution:**
```bash
# Fix permissions on host
sudo chown -R $USER:$USER docker-data/
chmod -R 755 docker-data/

# Or use Docker user mapping
docker-compose run --user $(id -u):$(id -g) grin-to-s3 <command>
```

#### 2. Container Cannot Start

**Problem:** Docker container fails to start.

**Solution:**
```bash
# Check logs
docker-compose logs grin-to-s3

# Verify image build
docker build -t grin-to-s3 .

# Check Docker daemon
docker info
```

#### 3. MinIO Connection Issues

**Problem:** Cannot connect to MinIO storage.

**Solution:**
```bash
# Check MinIO status
docker-compose exec minio mc admin info local

# Verify network connectivity
docker-compose exec grin-to-s3 ping minio

# Check MinIO logs
docker-compose logs minio
```

#### 4. OAuth2 Authentication Errors

**Problem:** Authentication with GRIN fails.

**Solution:**
```bash
# Verify credentials file exists
docker-compose exec grin-to-s3 ls -la /app/config/

# Re-run authentication setup
docker-compose exec grin-to-s3 python grin.py auth setup

# Check credentials validity
docker-compose exec grin-to-s3 python grin.py auth validate
```

#### 5. Storage Credential Issues

**Problem:** Cannot access cloud storage.

**Solution:**
```bash
# Test storage connection
docker-compose exec grin-to-s3 python grin.py storage ls

# Verify credentials format
docker-compose exec grin-to-s3 cat /app/config/r2-credentials.json

# Check environment variables
docker-compose exec grin-to-s3 env | grep -E "(R2|AWS|MINIO)"
```

### Debug Mode

Enable debug logging for detailed troubleshooting:

```yaml
# docker-compose.yml
environment:
  - GRIN_LOG_LEVEL=DEBUG
```

### Health Checks

```bash
# Check container health
docker-compose ps

# Test application responsiveness
docker-compose exec grin-to-s3 python grin.py --help

# Verify database accessibility
docker-compose exec grin-to-s3 sqlite3 /app/data/test.db ".tables"
```

## Advanced Usage

### Custom Docker Images

Build custom images with additional dependencies:

```dockerfile
# Dockerfile.custom
FROM grin-to-s3:latest

# Install additional tools
RUN apt-get update && apt-get install -y \
    vim \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Add custom scripts
COPY custom-scripts/ /app/custom-scripts/
```

### Multi-Stage Deployments

Use different configurations for development, staging, and production:

```bash
# Development
docker-compose -f docker-compose.dev.yml up -d

# Staging with R2
docker-compose -f docker-compose.staging.yml up -d

# Production with S3
docker-compose -f docker-compose.prod.yml up -d
```

### Scaling and Performance

For large-scale deployments:

```yaml
# docker-compose.yml
services:
  grin-to-s3:
    deploy:
      resources:
        limits:
          memory: 4G
          cpus: '2.0'
    environment:
      - GRIN_CONCURRENT_DOWNLOADS=10
      - GRIN_CONCURRENT_UPLOADS=20
      - GRIN_BATCH_SIZE=200
```

### Backup and Recovery

```bash
# Backup data
docker run --rm -v grin-data:/data -v $(pwd):/backup alpine tar czf /backup/grin-backup.tar.gz /data

# Restore data
docker run --rm -v grin-data:/data -v $(pwd):/backup alpine tar xzf /backup/grin-backup.tar.gz -C /
```

## Security Best Practices

### Credential Security

The Docker configuration uses secure credential mounting practices:

1. **Home directory mounting**: OAuth2 credentials are mounted from `~/.config/grin-to-s3/` on the host
2. **Read-only mounts**: All credential files are mounted as read-only (`:ro`)
3. **No project directory secrets**: Credentials are never stored in the project directory
4. **Standard locations**: Uses XDG Base Directory specification for credential storage

### Credential File Locations

| Credential Type | Host Location | Container Location |
|---|---|---|
| OAuth2 Client Secret | `~/.config/grin-to-s3/client_secret.json` | `/app/.config/grin-to-s3/client_secret.json` |
| OAuth2 Token | `~/.config/grin-to-s3/credentials.json` | `/app/.config/grin-to-s3/credentials.json` |
| R2 Storage | `~/.config/grin-to-s3/r2_credentials.json` | `/app/.config/grin-to-s3/r2_credentials.json` |
| GPG Passphrase | `~/.config/grin-to-s3/gpg_passphrase.asc` | `/app/.config/grin-to-s3/gpg_passphrase.asc` |

### Volume Security

```yaml
volumes:
  # Secure credential mounting (read-only)
  - ~/.config/grin-to-s3:/app/.config/grin-to-s3:ro
  
  # Application data (read-write)
  - ./docker-data/data:/app/data
  - ./docker-data/output:/app/output
```

### OAuth2 Port Configuration

The OAuth2 authentication server uses port 58432 by default. This port is configurable:

```bash
# Default port
docker-compose -f docker-compose.dev.yml run --rm --service-ports grin-to-s3 python grin.py auth setup

# Custom port
export GRIN_OAUTH_PORT=59999
docker-compose -f docker-compose.dev.yml run --rm --service-ports grin-to-s3 python grin.py auth setup
```

**Important Notes**:
- The `--service-ports` flag is required to forward the OAuth2 server port
- The redirect URI in your OAuth2 client configuration must match the port
- Default redirect URI: `http://localhost:58432`
- For custom ports, update the redirect_uris in your client_secret.json

### GPG Setup for Archive Decryption

To decrypt `.tar.gz.gpg` files during sync, you need to provide the GPG passphrase:

1. **Create passphrase file**:
   ```bash
   echo "your-gpg-passphrase" > ~/.config/grin-to-s3/gpg_passphrase.asc
   chmod 600 ~/.config/grin-to-s3/gpg_passphrase.asc
   ```

**Note**: The GPG private key should already be available in your system. The sync process will use the passphrase file for automated decryption without interactive prompts.

### Additional Security Notes

- Never commit credential files to version control
- Use read-only mounts for credential files
- Rotate credentials regularly
- Use environment variables for CI/CD pipelines

### 2. Network Security

```yaml
# docker-compose.yml
networks:
  grin-network:
    driver: bridge
    internal: true  # Isolate from external networks
```

### 3. Container Security

- Run containers as non-root user (already configured)
- Use specific image tags, not `latest`
- Regularly update base images
- Scan images for vulnerabilities

### 4. Data Security

- Use encrypted storage for sensitive data
- Implement backup encryption
- Monitor access logs
- Use least-privilege storage permissions

### 5. Production Deployment

For production environments:

```yaml
# docker-compose.prod.yml
services:
  grin-to-s3:
    restart: unless-stopped
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '1.0'
```

## Getting Help

### Documentation

- Main README: [README.md](../README.md)
- Docker Examples: [examples/docker/README.md](../examples/docker/README.md)
- Project Issues: [GitHub Issues](https://github.com/harvard-idi/grin-to-s3-wip/issues)

### Support

For issues specific to Docker deployment:

1. Check the troubleshooting section above
2. Review Docker logs: `docker-compose logs grin-to-s3`
3. Verify configuration files and credentials
4. Create a GitHub issue with:
   - Docker version (`docker --version`)
   - Compose version (`docker-compose --version`)
   - Configuration files (with credentials redacted)
   - Error logs

### Community Resources

- [Docker Documentation](https://docs.docker.com/)
- [Docker Compose Reference](https://docs.docker.com/compose/)
- [MinIO Documentation](https://min.io/docs/)
- [Cloudflare R2 Documentation](https://developers.cloudflare.com/r2/)
- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)