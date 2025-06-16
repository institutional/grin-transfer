#!/usr/bin/env python3
"""
Book Collection Wrapper Script

Convenient top-level script for running the book collection pipeline.
This is equivalent to running `python -m collect_books` but provides
a simpler command interface.

Usage:
    python collect_books.py [arguments]

Examples:
    python collect_books.py books.csv
    python collect_books.py --test-mode --limit 100
    python collect_books.py --run-name "harvard_2024" books.csv
"""

import asyncio
import sys
from pathlib import Path

# Add the current directory to the path so we can import the module
sys.path.insert(0, str(Path(__file__).parent))

from collect_books.__main__ import main

if __name__ == "__main__":
    # Run the main function from the collect_books module
    exit(asyncio.run(main()))
