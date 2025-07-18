# Build stage
FROM python:3.12-alpine as builder

# Install system dependencies for building Python packages
RUN apk add --no-cache \
    build-base \
    git \
    libffi-dev \
    openssl-dev

# Set working directory
WORKDIR /app

# Copy dependency files first for better caching
COPY pyproject.toml ./

# Install Python dependencies (cached layer)
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Copy source code
COPY src/ ./src/
COPY grin.py ./

# Install the package
RUN pip install --no-cache-dir .

# Production stage  
FROM python:3.12-alpine

# Install only essential runtime dependencies
RUN apk add --no-cache \
    gnupg \
    curl \
    ca-certificates

# Create non-root user (Alpine version)
RUN addgroup -g 1000 -S grin && \
    adduser -u 1000 -S grin -G grin -h /app -s /bin/sh

# Set working directory
WORKDIR /app

# Copy Python packages from builder stage
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages

# Copy application code
COPY . .

# Copy and set up entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Create data directories with proper permissions
RUN mkdir -p /app/data /app/output /app/config /app/logs /app/staging /app/.gnupg && \
    chown -R grin:grin /app

# Set environment variables
ENV PYTHONPATH="/app/src:${PYTHONPATH}"
ENV GRIN_CONFIG_DIR="/app/config"
ENV GRIN_DATA_DIR="/app/data"
ENV GRIN_OUTPUT_DIR="/app/output"
ENV GRIN_LOG_DIR="/app/logs"
ENV GRIN_STAGING_DIR="/app/staging"

# Switch to non-root user
USER grin

# Create volumes for persistent data
VOLUME ["/app/data", "/app/output", "/app/config", "/app/logs", "/app/staging"]

# Set default command
ENTRYPOINT ["python", "grin.py"]
CMD ["--help"]