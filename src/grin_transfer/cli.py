"""CLI entry point for grin_transfer."""

import sys
from pathlib import Path

# Add project root to path to import grin.py
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from grin import entry_point as _entry_point


def entry_point():
    """Console script entry point that delegates to grin.py."""
    return _entry_point()
