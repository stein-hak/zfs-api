#!/usr/bin/env python3
"""
Test the migration API endpoints
"""

import requests
import json
import time
import sys

API_BASE = "http://localhost:8545/api/v1/migrations"

def test_create_migration():
    """Test creating a migration task"""
    print("=== Testing Migration API ===")
    
    # Create a migration task
    migration_data = {
        "source": "syspool/benchmark_test@snap_5120mb",
        "destination": "archive/api_test",
        "remote": "192.168.10.152",
        "limit": 10,  # 10MB/s for testing
        "recursive": False
    }
    
    print("\n1. Creating migration task...")
    print(f"   Source: {migration_data['source']}")
    print(f"   Remote: {migration_data['remote']}:{migration_data['destination']}")
    
    response = requests.post(f"{API_BASE}/", json=migration_data)
    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
        return None
    
    result = response.json()
    task_id = result['task_id']
    print(f"   Task ID: {task_id}")
    print(f"   Status: {result['status']}")
    
    return task_id

def monitor_task(task_id):
    """Monitor task progress"""
    print(f"\n2. Monitoring task {task_id}...")
    
    last_percentage = -1
    
    while True:
        # Get task details
        response = requests.get(f"{API_BASE}/tasks/{task_id}")
        if response.status_code != 200:
            print(f"Error getting task: {response.text}")
            break
            
        task = response.json()
        status = task['status']
        
        # Get progress
        response = requests.get(f"{API_BASE}/tasks/{task_id}/progress")
        if response.status_code == 200:
            progress = response.json()['progress']
            
            if progress and 'percentage' in progress:
                percentage = progress.get('percentage', 0)
                if percentage != last_percentage:
                    bytes_transferred = progress.get('bytes_transferred', 0)
                    bytes_total = progress.get('bytes_total', 0)
                    rate = progress.get('rate_mbps', 0)
                    eta = progress.get('eta_seconds', 0)
                    
                    # Format sizes
                    transferred_gb = bytes_transferred / (1024**3)
                    total_gb = bytes_total / (1024**3) if bytes_total else 0
                    
                    print(f"\r   Progress: {percentage:.1f}% ({transferred_gb:.2f}/{total_gb:.2f} GB) "
                          f"Rate: {rate:.1f} MB/s ETA: {eta}s", end='', flush=True)
                    
                    last_percentage = percentage
        
        # Check if completed
        if status in ['completed', 'failed', 'cancelled']:
            print(f"\n   Final status: {status}")
            if task.get('error'):
                print(f"   Error: {task['error']}")
            break
            
        time.sleep(0.5)
    
    return task

def test_list_tasks():
    """Test listing tasks"""
    print("\n3. Listing all tasks...")
    
    response = requests.get(f"{API_BASE}/tasks")
    if response.status_code != 200:
        print(f"Error: {response.text}")
        return
        
    result = response.json()
    print(f"   Total tasks: {result['total']}")
    
    for task in result['tasks'][:5]:  # Show max 5 tasks
        print(f"   - {task['id']} ({task['type']}) - {task['status']} - {task['created_at']}")

def test_cancel_task():
    """Test cancelling a task"""
    # Create a task to cancel
    migration_data = {
        "source": "syspool/benchmark_test@snap_5120mb",
        "destination": "archive/cancel_test",
        "limit": 1,  # Very slow to ensure we can cancel it
        "recursive": False
    }
    
    print("\n4. Testing task cancellation...")
    response = requests.post(f"{API_BASE}/", json=migration_data)
    if response.status_code != 200:
        print(f"Error creating task: {response.text}")
        return
        
    task_id = response.json()['task_id']
    print(f"   Created task: {task_id}")
    
    # Wait a bit for it to start
    time.sleep(2)
    
    # Cancel it
    response = requests.delete(f"{API_BASE}/tasks/{task_id}")
    if response.status_code == 200:
        print("   Task cancelled successfully")
    else:
        print(f"   Error cancelling: {response.text}")

def main():
    """Run all tests"""
    print("Testing ZFS Migration API")
    print("=" * 50)
    
    # Test 1: Create and monitor a migration
    task_id = test_create_migration()
    if task_id:
        monitor_task(task_id)
    
    # Test 2: List tasks
    test_list_tasks()
    
    # Test 3: Cancel a task
    # test_cancel_task()  # Commented out to avoid creating extra tasks
    
    print("\n" + "=" * 50)
    print("Tests completed")

if __name__ == "__main__":
    main()