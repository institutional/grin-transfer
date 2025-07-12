"""Test the grin.py entrypoint by exercising all subcommands."""

import subprocess
import sys
from pathlib import Path

import pytest

# Path to the grin.py script
GRIN_SCRIPT = Path(__file__).parent.parent.parent / "grin.py"


def get_subcommands():
    """Introspect the grin.py script to get all subcommands."""
    # Import the grin module to get the parser
    import importlib.util
    spec = importlib.util.spec_from_file_location("grin", GRIN_SCRIPT)
    grin_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(grin_module)

    # Get the parser and extract subcommands
    parser, _ = grin_module.create_parser()
    subcommands = []
    for action in parser._actions:
        if hasattr(action, "choices") and action.choices:
            subcommands.extend(action.choices.keys())

    return subcommands


def test_grin_help():
    """Test that grin.py --help works."""
    result = subprocess.run(
        [sys.executable, str(GRIN_SCRIPT), "--help"],
        capture_output=True,
        text=True
    )
    assert result.returncode == 0
    assert "GRIN-to-S3" in result.stdout
    assert "Available commands" in result.stdout


@pytest.mark.parametrize("subcommand", get_subcommands())
def test_subcommand_help(subcommand):
    """Test that each subcommand --help works without import errors."""
    result = subprocess.run(
        [sys.executable, str(GRIN_SCRIPT), subcommand, "--help"],
        capture_output=True,
        text=True
    )
    assert result.returncode == 0, f"Subcommand {subcommand} failed: {result.stderr}"
    assert "usage:" in result.stdout.lower()




def test_invalid_command():
    """Test that invalid commands are handled gracefully."""
    result = subprocess.run(
        [sys.executable, str(GRIN_SCRIPT), "invalid-command"],
        capture_output=True,
        text=True
    )
    assert result.returncode == 1
    assert "Unknown command" in result.stdout


def test_no_command():
    """Test that running grin.py with no arguments shows help."""
    result = subprocess.run(
        [sys.executable, str(GRIN_SCRIPT)],
        capture_output=True,
        text=True
    )
    assert result.returncode == 1
    assert "GRIN-to-S3" in result.stdout


if __name__ == "__main__":
    # Run tests directly
    test_grin_help()

    subcommands = get_subcommands()
    for cmd in subcommands:
        test_subcommand_help(cmd)

    test_invalid_command()
    test_no_command()

    print("All entrypoint tests passed")
