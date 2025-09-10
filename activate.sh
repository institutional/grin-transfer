#!/bin/bash
# Quick activation script for development with uv
echo "Setting up development environment with uv (Python 3.12+)..."

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "❌ Error: uv is not installed"
    echo "Install uv with: curl -LsSf https://astral.sh/uv/install.sh | sh"
    echo "Then restart your shell or run: source ~/.local/bin/env"
    exit 1
fi

# Sync dependencies and activate environment
echo "Syncing dependencies with uv..."
uv sync

# Activate the virtual environment
source .venv/bin/activate
echo "✅ Virtual environment activated with uv"
echo "Python version: $(python --version)"
echo ""
echo "Dependencies are managed by uv. Use these commands:"
echo "  Add dependency:    uv add <package>"
echo "  Add dev dependency: uv add --dev <package>"
echo "  Update deps:       uv sync"
echo "  Run commands:      uv run <command>"
echo
echo "Available commands:"
echo "  Auth setup:      uv run grin auth setup"
echo "  Test suite:      uv run pytest src/tests/"
echo "  Book collection: uv run grin collect --help"
echo "  Processing:      uv run grin process --help"
echo "  Sync pipeline:   uv run grin sync pipeline --help"
echo "  Storage mgmt:    uv run grin storage --help"
echo "  Enrichment:      uv run grin enrich --help"
echo "  Export CSV:      uv run grin export --help"
