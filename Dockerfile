# Build stage
FROM python:3.12-slim as builder

# Install system dependencies for building Python packages
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Set working directory
WORKDIR /app

# Copy dependency files and README (needed by hatchling)
COPY pyproject.toml uv.lock .python-version README.md ./

# Copy source code (needed for build)
COPY src/ ./src/
COPY grin.py ./

# Install dependencies with uv
RUN uv sync --frozen --no-dev

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

# Install uv in production stage
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Copy pyproject.toml and uv.lock for uv run to work
COPY pyproject.toml uv.lock .python-version ./

# Copy application code
COPY . .

# Install production dependencies only
RUN uv sync --frozen --no-dev

# Create data directories with permissions
RUN mkdir -p /app/data /app/output /app/config /app/logs /app/staging /app/.gnupg /app/secrets /app/.config/grin-transfer && \
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

# Set default command using uv run to ensure virtual environment is used
ENTRYPOINT ["uv", "run", "python", "grin.py"]
CMD ["--help"]
