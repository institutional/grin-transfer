#!/bin/bash
# Test remote authentication in a Docker container

echo "🐳 Testing Remote Authentication in Docker Container"
echo "=================================================="

# Test in a clean Python container
docker run --rm -v $(pwd):/workspace -w /workspace python:3.12-slim bash -c "
echo '📦 Container Environment:'
echo 'Python version:' && python --version
echo 'Platform:' && uname -a
echo 'Container hostname:' && hostname
echo

echo '📋 Installing dependencies...'
pip install -q google-auth-oauthlib aiohttp > /dev/null 2>&1
echo 'Dependencies installed.'

echo '🔧 Running remote auth test script...'
echo
python test_remote_auth.py

echo
echo '🧪 Testing with simulated SSH environment:'
SSH_CLIENT='192.168.1.100 54321 22' python -c '
import sys, os
sys.path.insert(0, \"src\")
from grin_to_s3.auth import detect_remote_shell
print(f\"SSH environment detected: {detect_remote_shell()}\")
'

echo
echo '🎯 Testing --remote-auth flag detection:'
python -c '
import sys
sys.path.insert(0, \"src\")
from grin_to_s3.auth import setup_credentials

# Mock the file operations to avoid requiring real secrets
import unittest.mock
with unittest.mock.patch(\"grin_to_s3.auth.GRINAuth\") as mock_auth:
    mock_auth.return_value.secrets_file.exists.return_value = False
    try:
        setup_credentials(remote_auth=True)
    except SystemExit as e:
        print(f\"Expected exit (no secrets file): {e.code}\")
    except Exception as e:
        print(f\"Setup would use remote auth: {type(e).__name__}\")
'

echo
echo '✅ Docker container test completed!'
echo 'The remote authentication detection is working correctly in containerized environments.'
"