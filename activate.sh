#!/bin/bash
# Quick activation script for development
echo "Activating development environment (Python 3.12+)..."

# Check if we have Python 3.12+ available
if command -v python3.12 &> /dev/null; then
    PYTHON_CMD="python3.12"
elif command -v python3 &> /dev/null && python3 -c "import sys; exit(0 if sys.version_info >= (3, 12) else 1)" 2>/dev/null; then
    PYTHON_CMD="python3"
elif command -v python &> /dev/null && python -c "import sys; exit(0 if sys.version_info >= (3, 12) else 1)" 2>/dev/null; then
    PYTHON_CMD="python"
else
    echo "❌ Error: Python 3.12+ required but not found"
    echo "Please install Python 3.12+ or use pyenv to manage versions"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment with $PYTHON_CMD..."
    $PYTHON_CMD -m venv venv
fi

source venv/bin/activate
echo "✅ Virtual environment activated"
echo "Python version: $(python --version)"

# Verify we have the right Python version in the venv
if ! python -c "import sys; exit(0 if sys.version_info >= (3, 12) else 1)" 2>/dev/null; then
    echo "⚠️  Warning: Virtual environment has Python < 3.12"
    echo "Consider recreating: rm -rf venv && ./activate.sh"
fi
echo ""
echo "Install dependencies with:"
echo "pip install -e \".[dev]\""
echo
echo "Available commands:"
echo "  Auth setup:      python auth.py setup"
echo "  Test suite:      pytest tests/"
echo "  Book collection: python -m collect_books --help"
echo "  Enrichment:      python grin_enrichment.py --help"
echo "  Download data:   python download.py --help"
echo "  Storage ops:     python storage.py --help"
echo ""
echo "Requirements: Python 3.12+"