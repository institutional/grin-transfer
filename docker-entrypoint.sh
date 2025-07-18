#!/bin/bash

# Docker entrypoint script for grin-to-s3

# Copy GPG keys from read-only mount if available
if [ -d "/app/.gnupg-readonly" ] && [ "$(ls -A /app/.gnupg-readonly 2>/dev/null)" ]; then
    echo "Copying GPG keys from read-only mount to writable location..."
    cp -r /app/.gnupg-readonly/* /app/.gnupg/ 2>/dev/null || true
    chmod 700 /app/.gnupg
    chmod 600 /app/.gnupg/* 2>/dev/null || true
fi

# Execute the command
exec "$@"