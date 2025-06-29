#!/usr/bin/env python3
"""
Unit tests for database_utils module.

Tests the shared database validation function with various error scenarios.
"""

import sqlite3
import sys
import tempfile
import unittest.mock
from pathlib import Path

import pytest

from grin_to_s3.database_utils import validate_database_file


class TestValidateDatabaseFile:
    """Test database validation function."""

    def test_nonexistent_file_exits(self, capsys):
        """Test that validation exits when database file doesn't exist."""
        with pytest.raises(SystemExit):
            validate_database_file("/nonexistent/path/to/db.db")
        
        captured = capsys.readouterr()
        assert "Database file does not exist" in captured.out
        assert "Make sure you've run a book collection first" in captured.out

    def test_valid_database_with_tables_check(self, tmp_path):
        """Test validation passes for valid database with required tables."""
        db_path = tmp_path / "test.db"
        
        # Create a valid database with required tables
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE books (barcode TEXT PRIMARY KEY)")
            cursor.execute("CREATE TABLE processed (barcode TEXT PRIMARY KEY)")
            cursor.execute("CREATE TABLE failed (barcode TEXT PRIMARY KEY)")
            conn.commit()
        
        # Should not raise an exception
        validate_database_file(str(db_path), check_tables=True)

    def test_valid_database_with_books_count_check(self, tmp_path):
        """Test validation passes for database with books when checking count."""
        db_path = tmp_path / "test.db"
        
        # Create a valid database with books
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE books (barcode TEXT PRIMARY KEY)")
            cursor.execute("INSERT INTO books (barcode) VALUES ('test123')")
            conn.commit()
        
        # Should not raise an exception
        validate_database_file(str(db_path), check_books_count=True)

    def test_missing_tables_exits(self, tmp_path, capsys):
        """Test that validation exits when required tables are missing."""
        db_path = tmp_path / "test.db"
        
        # Create a database without required tables
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE other_table (id INTEGER)")
            conn.commit()
        
        with pytest.raises(SystemExit):
            validate_database_file(str(db_path), check_tables=True)
        
        captured = capsys.readouterr()
        assert "Database is missing required tables" in captured.out
        assert "books" in captured.out
        assert "processed" in captured.out
        assert "failed" in captured.out

    def test_empty_books_table_exits(self, tmp_path, capsys):
        """Test that validation exits when books table is empty and count check is enabled."""
        db_path = tmp_path / "test.db"
        
        # Create a database with empty books table
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE books (barcode TEXT PRIMARY KEY)")
            conn.commit()
        
        with pytest.raises(SystemExit):
            validate_database_file(str(db_path), check_books_count=True)
        
        captured = capsys.readouterr()
        assert "Database contains no books" in captured.out

    def test_corrupted_database_exits(self, tmp_path, capsys):
        """Test that validation exits when database file is corrupted."""
        db_path = tmp_path / "corrupted.db"
        
        # Create a file with non-SQLite content
        with open(db_path, 'w') as f:
            f.write("This is not a SQLite database")
        
        with pytest.raises(SystemExit):
            validate_database_file(str(db_path))
        
        captured = capsys.readouterr()
        assert "Cannot read SQLite database" in captured.out
        assert "corrupted or not a valid SQLite database" in captured.out

    def test_default_parameters(self, tmp_path):
        """Test validation with default parameters (no checks)."""
        db_path = tmp_path / "test.db"
        
        # Create a minimal valid database
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE dummy (id INTEGER)")
            conn.commit()
        
        # Should not raise an exception with default parameters
        validate_database_file(str(db_path))

    def test_shows_available_databases_when_missing(self, tmp_path, capsys, monkeypatch):
        """Test that validation shows available databases when file is missing."""
        # Change to temp directory
        monkeypatch.chdir(tmp_path)
        
        # Create output directory structure
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        run1_dir = output_dir / "run1"
        run1_dir.mkdir()
        run1_db = run1_dir / "books.db"
        
        # Create a valid database in run1
        with sqlite3.connect(str(run1_db)) as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE books (barcode TEXT PRIMARY KEY)")
            conn.commit()
        
        # Also create a directory without a database
        run2_dir = output_dir / "run2"
        run2_dir.mkdir()
        
        with pytest.raises(SystemExit):
            validate_database_file("nonexistent.db")
        
        captured = capsys.readouterr()
        assert "Available run directories:" in captured.out
        assert "output/run1/books.db" in captured.out
        # run2 should not appear since it has no books.db

    def test_sqlite_error_handling(self, tmp_path, capsys):
        """Test handling of SQLite errors during validation."""
        db_path = tmp_path / "test.db"
        
        # Create a valid database
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE books (barcode TEXT PRIMARY KEY)")
            conn.commit()
        
        # Mock sqlite3.connect to raise an error
        with unittest.mock.patch('grin_to_s3.database_utils.sqlite3.connect', side_effect=sqlite3.Error("Test error")):
            with pytest.raises(SystemExit):
                validate_database_file(str(db_path))
            
            captured = capsys.readouterr()
            assert "Cannot read SQLite database: Test error" in captured.out

    def test_partial_missing_tables(self, tmp_path, capsys):
        """Test validation when only some required tables are missing."""
        db_path = tmp_path / "test.db"
        
        # Create a database with only some required tables
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE books (barcode TEXT PRIMARY KEY)")
            cursor.execute("CREATE TABLE processed (barcode TEXT PRIMARY KEY)")
            # Missing: failed table
            conn.commit()
        
        with pytest.raises(SystemExit):
            validate_database_file(str(db_path), check_tables=True)
        
        captured = capsys.readouterr()
        assert "Database is missing required tables: ['failed']" in captured.out