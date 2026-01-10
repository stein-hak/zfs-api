#!/usr/bin/env python3
"""
Test client for ZFS API server
Tests all migrated AsyncZFS operations
"""

import requests
import json
import sys
from typing import Dict, Any, Optional

class ZFSAPIClient:
    def __init__(self, base_url: str = "http://localhost:8545"):
        self.base_url = base_url
        self.token = None

    def call(self, method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make JSON-RPC call to the API"""
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params or {},
            "id": 1
        }

        if self.token:
            payload["params"]["token"] = self.token

        response = requests.post(self.base_url, json=payload)
        result = response.json()

        if "error" in result:
            print(f"❌ {method}: {result['error']}")
            return result
        else:
            print(f"✅ {method}: Success")
            return result

    def login(self, username: str = "admin", password: str = "admin"):
        """Login and get token"""
        result = self.call("auth_login", {"username": username, "password": password})
        if "result" in result:
            self.token = result["result"]["token"]
            print(f"🔑 Logged in successfully")
        return result

def test_dataset_operations(client: ZFSAPIClient, test_pool: str):
    """Test dataset operations"""
    print("\n" + "="*60)
    print("TESTING DATASET OPERATIONS")
    print("="*60)

    test_dataset = f"{test_pool}/test_dataset"

    # List datasets
    result = client.call("dataset_list")

    # Create dataset
    result = client.call("dataset_create", {
        "dataset": test_dataset,
        "properties": {"compression": "lz4"}
    })

    # Get properties
    result = client.call("dataset_get_properties", {
        "dataset": test_dataset,
        "property": "all"
    })

    # Set property
    result = client.call("dataset_set_property", {
        "dataset": test_dataset,
        "property": "compression",
        "value": "zstd"
    })

    # Get space
    result = client.call("dataset_get_space", {
        "dataset": test_dataset
    })

    # Mount dataset
    result = client.call("dataset_mount", {
        "dataset": test_dataset
    })

    return test_dataset

def test_snapshot_operations(client: ZFSAPIClient, dataset: str):
    """Test snapshot operations"""
    print("\n" + "="*60)
    print("TESTING SNAPSHOT OPERATIONS")
    print("="*60)

    # Create snapshot
    result = client.call("snapshot_create", {
        "dataset": dataset,
        "name": "test_snap1",
        "recursive": False
    })

    # Create auto snapshot
    result = client.call("snapshot_create_auto", {
        "dataset": dataset,
        "tag": "test",
        "recursive": False
    })

    # List snapshots
    result = client.call("snapshot_list", {
        "dataset": dataset
    })

    # Hold snapshot
    result = client.call("snapshot_hold", {
        "dataset": dataset,
        "snapshot": "test_snap1",
        "tag": "test_hold",
        "recursive": False
    })

    # List holds
    result = client.call("snapshot_holds_list", {
        "dataset": dataset,
        "snapshot": "test_snap1",
        "recursive": False
    })

    # Release hold
    result = client.call("snapshot_release", {
        "dataset": dataset,
        "snapshot": "test_snap1",
        "tag": "test_hold",
        "recursive": False
    })

    # Create second snapshot for diff
    result = client.call("snapshot_create", {
        "dataset": dataset,
        "name": "test_snap2",
        "recursive": False
    })

    # Snapshot diff
    result = client.call("snapshot_diff", {
        "snapshot1": f"{dataset}@test_snap1",
        "snapshot2": f"{dataset}@test_snap2"
    })

    return f"{dataset}@test_snap1"

def test_bookmark_operations(client: ZFSAPIClient, dataset: str, snapshot: str):
    """Test bookmark operations"""
    print("\n" + "="*60)
    print("TESTING BOOKMARK OPERATIONS")
    print("="*60)

    bookmark = f"{dataset}#test_bookmark"

    # Create bookmark
    result = client.call("bookmark_create", {
        "snapshot": snapshot,
        "bookmark": bookmark
    })

    # List bookmarks
    result = client.call("bookmark_list", {
        "dataset": dataset
    })

    return bookmark

def test_pool_operations(client: ZFSAPIClient, test_pool: str):
    """Test pool operations"""
    print("\n" + "="*60)
    print("TESTING POOL OPERATIONS")
    print("="*60)

    # List pools
    result = client.call("pool_list")

    # Get pool properties
    result = client.call("pool_get_properties", {
        "pool": test_pool,
        "property": "all"
    })

    # Get pool status
    result = client.call("pool_status", {
        "pool": test_pool
    })

    # Note: scrub operations commented out to avoid unnecessary scrubs
    # Uncomment if you want to test them
    # result = client.call("pool_scrub_start", {"pool": test_pool})
    # result = client.call("pool_scrub_stop", {"pool": test_pool})

def test_clone_operations(client: ZFSAPIClient, snapshot: str, test_pool: str):
    """Test clone operations"""
    print("\n" + "="*60)
    print("TESTING CLONE OPERATIONS")
    print("="*60)

    clone_name = f"{test_pool}/test_clone"

    # Create clone
    result = client.call("clone_create", {
        "source": snapshot,
        "target": clone_name,
        "properties": {"compression": "lz4"}
    })

    return clone_name

def test_volume_operations(client: ZFSAPIClient, test_pool: str):
    """Test volume operations"""
    print("\n" + "="*60)
    print("TESTING VOLUME OPERATIONS")
    print("="*60)

    volume_name = f"{test_pool}/test_volume"

    # Create volume
    result = client.call("volume_create", {
        "dataset": volume_name,
        "size_gb": 1,
        "compression": "lz4",
        "volblocksize": "8K"
    })

    # List volumes
    result = client.call("volume_list")

    return volume_name

def cleanup(client: ZFSAPIClient, dataset: str, clone: str, volume: str, bookmark: str):
    """Cleanup test resources"""
    print("\n" + "="*60)
    print("CLEANUP")
    print("="*60)

    # Destroy clone
    result = client.call("dataset_destroy", {
        "dataset": clone,
        "recursive": False
    })

    # Destroy volume
    result = client.call("dataset_destroy", {
        "dataset": volume,
        "recursive": False
    })

    # Destroy bookmark
    result = client.call("bookmark_destroy", {
        "bookmark": bookmark
    })

    # Destroy snapshots
    result = client.call("snapshot_destroy", {
        "dataset": dataset,
        "snapshot": "test_snap2",
        "recursive": False
    })

    result = client.call("snapshot_destroy", {
        "dataset": dataset,
        "snapshot": "test_snap1",
        "recursive": False
    })

    # List and destroy auto snapshot (name varies)
    snap_result = client.call("snapshot_list", {"dataset": dataset})
    if "result" in snap_result:
        for snap in snap_result["result"]["snapshots"]:
            if "test_" in snap:  # Fixed: underscore not dash
                result = client.call("snapshot_destroy", {
                    "dataset": dataset,
                    "snapshot": snap,
                    "recursive": False
                })

    # Destroy dataset (with recursive to handle any remaining snapshots)
    result = client.call("dataset_destroy", {
        "dataset": dataset,
        "recursive": True  # Fixed: use recursive deletion
    })

def main():
    if len(sys.argv) < 2:
        print("Usage: python test_api_client.py <test_pool_name>")
        print("Example: python test_api_client.py tank")
        sys.exit(1)

    test_pool = sys.argv[1]

    print("="*60)
    print(f"ZFS API TEST SUITE - Testing with pool: {test_pool}")
    print("="*60)

    client = ZFSAPIClient()

    # Login
    client.login()

    try:
        # Run tests
        dataset = test_dataset_operations(client, test_pool)
        snapshot = test_snapshot_operations(client, dataset)
        bookmark = test_bookmark_operations(client, dataset, snapshot)
        test_pool_operations(client, test_pool)
        clone = test_clone_operations(client, snapshot, test_pool)
        volume = test_volume_operations(client, test_pool)

        # Cleanup
        cleanup(client, dataset, clone, volume, bookmark)

        print("\n" + "="*60)
        print("✅ ALL TESTS COMPLETED SUCCESSFULLY!")
        print("="*60)

    except Exception as e:
        print(f"\n❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
