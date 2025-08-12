#!/usr/bin/env python3
"""
Constants for grin-to-s3 application.

Centralized constants to eliminate duplication across the codebase.
"""

# GRIN API rate limiting
GRIN_RATE_LIMIT_QPS = 5.0
GRIN_RATE_LIMIT_DELAY = 0.2  # 1/5 = 0.2 seconds for 5 QPS

# GRIN queue and processing limits
GRIN_MAX_QUEUE_SIZE = 50000
GRIN_DEFAULT_BATCH_SIZE = 200

# Conversion request defaults
DEFAULT_CONVERSION_REQUEST_LIMIT = 100
