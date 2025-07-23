# Build stage
FROM python:3.12-slim as builder

# Install system dependencies for building Python packages
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy dependency files and source code
COPY pyproject.toml ./
COPY src/ ./src/
COPY grin.py ./

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir .

# Production stage
FROM python:3.12-slim

# Install system dependencies required at runtime
RUN apt-get update && apt-get install -y \
    gnupg \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r grin && useradd -r -g grin -d /app -s /bin/bash grin

# Set working directory
WORKDIR /app

# Copy Python packages from builder stage
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages

# Copy application code
COPY . .

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
