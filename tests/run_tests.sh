#!/bin/bash
# Run API tests

# Activate virtual environment
source venv/bin/activate

# Check if server is running
if ! curl -s http://localhost:8545/health > /dev/null 2>&1; then
    echo "❌ Error: ZFS API server is not running"
    echo "   Start it with: ./start.sh"
    exit 1
fi

# Run tests
echo "Running API tests..."
python test_api.py
