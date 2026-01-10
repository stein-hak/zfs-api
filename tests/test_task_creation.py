#!/usr/bin/env python3
"""
Test task creation and retrieval
"""

import requests
import json
import time

API_BASE = "http://localhost:8545/api/v1/migrations"

# Create a simple task
print("Creating test task...")
response = requests.post(f"{API_BASE}/", json={
    "source": "syspool/test@snap1",
    "destination": "syspool/test_dest"
})

if response.status_code == 200:
    result = response.json()
    task_id = result['task_id']
    print(f"Task created: {task_id}")
    
    # Wait a bit
    time.sleep(1)
    
    # Try to get the task
    print(f"\nGetting task {task_id}...")
    response = requests.get(f"{API_BASE}/tasks/{task_id}")
    
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    
    # List all tasks
    print("\nListing all tasks...")
    response = requests.get(f"{API_BASE}/tasks")
    result = response.json()
    print(f"Total tasks: {result['total']}")
    for task in result['tasks']:
        print(f"  - {task['id']} ({task['status']})")
else:
    print(f"Error creating task: {response.status_code} - {response.text}")