#!/usr/bin/env python3
"""
Test script for remote shell OAuth2 authentication functionality.
Can be run in Docker containers or remote environments to validate detection.
"""

import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from grin_to_s3.auth import detect_remote_shell
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure you're running from the project root and have dependencies installed")
    sys.exit(1)


def test_environment_detection():
    """Test remote shell environment detection."""
    print("🔍 Testing Remote Shell Detection")
    print("=" * 40)

    print("Current environment variables:")
    env_vars = ["SSH_CLIENT", "SSH_TTY", "SSH_CONNECTION", "TERM", "DISPLAY",
                "AWS_EXECUTION_ENV", "GOOGLE_CLOUD_PROJECT", "AZURE_FUNCTIONS_ENVIRONMENT"]

    for var in env_vars:
        value = os.environ.get(var, "Not set")
        print(f"  {var}: {value}")

    print(f"\n🎯 Current detection result: {detect_remote_shell()}")
    print()


def test_simulated_environments():
    """Test detection with simulated environments."""
    print("🧪 Testing Simulated Environments")
    print("=" * 40)

    # Save original environment
    original_env = {var: os.environ.get(var) for var in os.environ}

    test_cases = [
        {
            "name": "SSH session",
            "env": {"SSH_CLIENT": "192.168.1.100 54321 22"},
            "expected": True
        },
        {
            "name": "SSH TTY",
            "env": {"SSH_TTY": "/dev/pts/0"},
            "expected": True
        },
        {
            "name": "tmux session",
            "env": {"TERM": "tmux-256color"},
            "expected": True
        },
        {
            "name": "AWS cloud instance",
            "env": {"AWS_EXECUTION_ENV": "AWS_ECS_FARGATE", "DISPLAY": ""},
            "expected": True
        },
        {
            "name": "Local desktop",
            "env": {"TERM": "xterm-256color", "DISPLAY": ":0"},
            "expected": False
        }
    ]

    for test_case in test_cases:
        # Clear relevant environment variables
        for var in ["SSH_CLIENT", "SSH_TTY", "SSH_CONNECTION", "TERM", "DISPLAY",
                   "AWS_EXECUTION_ENV", "GOOGLE_CLOUD_PROJECT", "AZURE_FUNCTIONS_ENVIRONMENT"]:
            os.environ.pop(var, None)

        # Set test environment
        for key, value in test_case["env"].items():
            if value == "":
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

        result = detect_remote_shell()
        status = "✅" if result == test_case["expected"] else "❌"
        print(f"{status} {test_case['name']}: {result} (expected {test_case['expected']})")

    # Restore original environment
    os.environ.clear()
    for key, value in original_env.items():
        if value is not None:
            os.environ[key] = value

    print()


def test_manual_flow_simulation():
    """Test manual authorization flow with mocked components."""
    print("🔐 Testing Manual Authorization Flow")
    print("=" * 40)

    try:
        # Create a mock flow object (won't actually work without real secrets)
        print("Creating mock OAuth2 flow...")

        # This would normally require real client secrets
        print("Note: Full manual flow testing requires:")
        print("  1. Valid client_secret.json file")
        print("  2. Network connectivity to Google OAuth2 servers")
        print("  3. Interactive user input")
        print("  4. Browser access on user's local machine")
        print()
        print("For actual testing, run:")
        print("  python grin.py auth setup --remote-auth")
        print()

    except Exception as e:
        print(f"Mock flow test completed (expected): {e}")
        print()


def test_docker_environment():
    """Test if we're running in Docker and how it's detected."""
    print("🐳 Docker Environment Detection")
    print("=" * 40)

    # Check common Docker indicators
    docker_indicators = [
        ("/.dockerenv file", Path("/.dockerenv").exists()),
        ("Container ID in /proc/1/cgroup", False),  # Will check below
        ("HOSTNAME pattern", len(os.environ.get("HOSTNAME", "")) == 12)
    ]

    # Check /proc/1/cgroup for container ID
    try:
        with open("/proc/1/cgroup") as f:
            cgroup_content = f.read()
            docker_indicators[1] = ("Container ID in /proc/1/cgroup", "docker" in cgroup_content or "containerd" in cgroup_content)
    except (FileNotFoundError, PermissionError):
        pass

    print("Docker environment indicators:")
    for indicator, present in docker_indicators:
        status = "✅" if present else "❌"
        print(f"  {status} {indicator}")

    print()
    print("Note: This test script doesn't import is_docker_environment() to avoid")
    print("      importing the full grin_to_s3.common module with its dependencies.")
    print()


def main():
    """Run all tests."""
    print("🚀 Remote Authentication Test Suite")
    print("=" * 50)
    print(f"Python version: {sys.version}")
    print(f"Platform: {sys.platform}")
    print(f"Working directory: {os.getcwd()}")
    print()

    test_environment_detection()
    test_simulated_environments()
    test_manual_flow_simulation()
    test_docker_environment()

    print("✨ Test suite completed!")
    print()
    print("To test the actual OAuth2 setup:")
    print("  1. Ensure you have client_secret.json configured")
    print("  2. Run: python grin.py auth setup --remote-auth")
    print("  3. Follow the displayed instructions")


if __name__ == "__main__":
    main()
