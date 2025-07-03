"""
Centralized database connection utilities with consistent WAL mode configuration.
"""

import sqlite3
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path

import aiosqlite


def _configure_connection(conn) -> None:
    """Configure database connection with WAL mode and performance settings."""
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=1000")
    conn.execute("PRAGMA temp_store=memory")
    conn.execute("PRAGMA busy_timeout=5000")  # Wait up to 5 seconds for locks


@contextmanager
def connect_sync(db_path: str | Path, timeout: float = 30.0) -> Generator[sqlite3.Connection, None, None]:
    """Create a synchronous database connection with WAL mode enabled."""
    with sqlite3.connect(str(db_path), timeout=timeout) as conn:
        _configure_connection(conn)
        yield conn


@asynccontextmanager
async def connect_async(db_path: str | Path, timeout: float = 30.0) -> AsyncGenerator[aiosqlite.Connection, None]:
    """Create an asynchronous database connection with WAL mode enabled."""
    async with aiosqlite.connect(str(db_path), timeout=timeout) as conn:
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        await conn.execute("PRAGMA cache_size=1000")
        await conn.execute("PRAGMA temp_store=memory")
        await conn.execute("PRAGMA busy_timeout=5000")  # Wait up to 5 seconds for locks
        yield conn
