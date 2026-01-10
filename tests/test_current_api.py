#!/usr/bin/env python3
"""
Quick test to verify the API is working with async Redis
"""

import requests
import json

# API endpoint
url = "http://localhost:8545"

# Test creating a migration task
print("Testing migration creation...")

# Create JSON-RPC request
payload = {
    "jsonrpc": "2.0",
    "method": "migration_create",
    "params": {
        "source": "syspool/benchmark_test@snap_5120mb",
        "destination": "syspool/test_async",
        "limit": 10,
        "recursive": False
    },
    "id": 1
}

# Basic auth if needed
auth = ("admin", "admin")  # Update with your credentials

try:
    response = requests.post(url, json=payload, auth=auth)
    result = response.json()
    print(f"Response: {json.dumps(result, indent=2)}")
    
    if "result" in result:
        print(f"✅ Migration task created successfully!")
        task_id = result["result"].get("task_id")
        print(f"Task ID: {task_id}")
    else:
        print(f"❌ Error: {result.get('error')}")
        
except Exception as e:
    print(f"❌ Connection error: {e}")

print("\nTesting task list...")

# List tasks
payload = {
    "jsonrpc": "2.0",
    "method": "migration_list",
    "params": {"limit": 5},
    "id": 2
}

try:
    response = requests.post(url, json=payload, auth=auth)
    result = response.json()
    
    if "result" in result:
        tasks = result["result"].get("tasks", [])
        print(f"✅ Found {len(tasks)} tasks")
        for task in tasks[:3]:  # Show first 3
            print(f"  - {task['id']}: {task['status']}")
    else:
        print(f"❌ Error: {result.get('error')}")
        
except Exception as e:
    print(f"❌ Connection error: {e}")