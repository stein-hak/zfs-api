#!/bin/bash
# Start ZFS API Server

# Activate virtual environment
source venv/bin/activate

# Start server
echo "Starting ZFS API Server..."
python zfs_api_server.py
