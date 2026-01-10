#!/usr/bin/env python3
"""
Comprehensive test script for ZFS API Server
Tests all API endpoints with localhost passwordless access
"""

import asyncio
import json
import sys
import time
from datetime import datetime
from typing import Dict, Any, Optional

import aiohttp


class ZFSAPITest:
    def __init__(self, url: str = "http://localhost:8545"):
        self.url = url
        self.session = None
        self.test_pool = None
        self.test_dataset = "testpool/testds"
        self.test_snapshot = "testsnap"
        self.results = []
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()
        
    async def call_api(self, method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make JSON-RPC call to API"""
        # Build JSON-RPC request
        json_request = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params or {},
            "id": 1
        }
        
        async with self.session.post(self.url, json=json_request) as response:
            json_response = await response.json()
            
            if "error" in json_response:
                error = json_response["error"]
                raise Exception(f"API Error: {error.get('message', 'Unknown error')}")
            
            return json_response.get("result", {})
    
    def log_test(self, test_name: str, status: str, details: str = ""):
        """Log test result"""
        result = {
            "test": test_name,
            "status": status,
            "details": details,
            "timestamp": datetime.now().isoformat()
        }
        self.results.append(result)
        
        # Print colored output
        if status == "PASS":
            print(f"\033[92m✓ {test_name}\033[0m")
        elif status == "SKIP":
            print(f"\033[93m⚠ {test_name} (SKIPPED: {details})\033[0m")
        else:
            print(f"\033[91m✗ {test_name} (FAILED: {details})\033[0m")
    
    async def test_authentication(self):
        """Test authentication endpoints"""
        print("\n=== Testing Authentication ===")
        
        # Test localhost passwordless access
        try:
            result = await self.call_api("pool_list")
            self.log_test("Localhost passwordless access", "PASS")
        except Exception as e:
            self.log_test("Localhost passwordless access", "FAIL", str(e))
            
        # Test login (should work even from localhost)
        try:
            result = await self.call_api("auth_login", {
                "username": "admin",
                "password": "admin"
            })
            if "token" in result:
                self.log_test("Authentication login", "PASS")
            else:
                self.log_test("Authentication login", "FAIL", "No token returned")
        except Exception as e:
            self.log_test("Authentication login", "FAIL", str(e))
    
    async def test_pool_operations(self):
        """Test pool operations"""
        print("\n=== Testing Pool Operations ===")
        
        # List pools
        try:
            result = await self.call_api("pool_list")
            pools = result.get("pools", [])
            self.log_test("Pool list", "PASS", f"Found {len(pools)} pools")
            
            if pools:
                self.test_pool = pools[0]
            else:
                self.log_test("Pool operations", "SKIP", "No pools available for testing")
                return
                
        except Exception as e:
            self.log_test("Pool list", "FAIL", str(e))
            return
        
        # Get pool properties
        try:
            result = await self.call_api("pool_get_properties", {
                "pool": self.test_pool,
                "property": "size"
            })
            self.log_test("Pool get properties", "PASS")
        except Exception as e:
            self.log_test("Pool get properties", "FAIL", str(e))
        
        # Get all pool properties
        try:
            result = await self.call_api("pool_get_properties", {
                "pool": self.test_pool
            })
            self.log_test("Pool get all properties", "PASS")
        except Exception as e:
            self.log_test("Pool get all properties", "FAIL", str(e))
        
        # Get pool status
        try:
            result = await self.call_api("pool_status", {
                "pool": self.test_pool
            })
            self.log_test("Pool status", "PASS")
        except Exception as e:
            self.log_test("Pool status", "FAIL", str(e))
        
        # Start scrub (careful - only if you want to)
        # try:
        #     result = await self.call_api("pool_scrub_start", {
        #         "pool": self.test_pool
        #     })
        #     self.log_test("Pool scrub start", "PASS")
        # except Exception as e:
        #     self.log_test("Pool scrub start", "FAIL", str(e))
    
    async def test_dataset_operations(self):
        """Test dataset operations"""
        print("\n=== Testing Dataset Operations ===")
        
        if not self.test_pool:
            self.log_test("Dataset operations", "SKIP", "No test pool available")
            return
        
        # Update test dataset name
        self.test_dataset = f"{self.test_pool}/test_api_{int(time.time())}"
        
        # Create dataset
        try:
            result = await self.call_api("dataset_create", {
                "dataset": self.test_dataset,
                "properties": {
                    "compression": "lz4",
                    "atime": "off"
                }
            })
            self.log_test("Dataset create", "PASS", self.test_dataset)
        except Exception as e:
            self.log_test("Dataset create", "FAIL", str(e))
            return
        
        # List datasets
        try:
            result = await self.call_api("dataset_list", {
                "dataset": self.test_pool
            })
            datasets = result.get("datasets", [])
            if self.test_dataset in datasets:
                self.log_test("Dataset list", "PASS", "Test dataset found")
            else:
                self.log_test("Dataset list", "FAIL", "Test dataset not in list")
        except Exception as e:
            self.log_test("Dataset list", "FAIL", str(e))
        
        # Get dataset properties
        try:
            result = await self.call_api("dataset_get_properties", {
                "dataset": self.test_dataset,
                "property": "compression"
            })
            self.log_test("Dataset get property", "PASS")
        except Exception as e:
            self.log_test("Dataset get property", "FAIL", str(e))
        
        # Get all properties
        try:
            result = await self.call_api("dataset_get_properties", {
                "dataset": self.test_dataset
            })
            self.log_test("Dataset get all properties", "PASS")
        except Exception as e:
            self.log_test("Dataset get all properties", "FAIL", str(e))
        
        # Set property
        try:
            result = await self.call_api("dataset_set_property", {
                "dataset": self.test_dataset,
                "property": "compression",
                "value": "gzip"
            })
            self.log_test("Dataset set property", "PASS")
        except Exception as e:
            self.log_test("Dataset set property", "FAIL", str(e))
        
        # Get space usage
        try:
            result = await self.call_api("dataset_get_space", {
                "dataset": self.test_dataset
            })
            self.log_test("Dataset get space", "PASS")
        except Exception as e:
            self.log_test("Dataset get space", "FAIL", str(e))
        
        # Mount dataset
        try:
            result = await self.call_api("dataset_mount", {
                "dataset": self.test_dataset
            })
            self.log_test("Dataset mount", "PASS")
        except Exception as e:
            self.log_test("Dataset mount", "FAIL", str(e))
    
    async def test_snapshot_operations(self):
        """Test snapshot operations"""
        print("\n=== Testing Snapshot Operations ===")
        
        if not self.test_dataset:
            self.log_test("Snapshot operations", "SKIP", "No test dataset available")
            return
        
        # Create snapshot
        try:
            result = await self.call_api("snapshot_create", {
                "dataset": self.test_dataset,
                "name": self.test_snapshot
            })
            self.log_test("Snapshot create", "PASS")
        except Exception as e:
            self.log_test("Snapshot create", "FAIL", str(e))
        
        # Create auto snapshot
        try:
            result = await self.call_api("snapshot_create_auto", {
                "dataset": self.test_dataset,
                "tag": "auto_test",
                "tag1": "api"
            })
            auto_snap_name = result.get("snapshot", "")
            self.log_test("Snapshot create auto", "PASS", auto_snap_name)
        except Exception as e:
            self.log_test("Snapshot create auto", "FAIL", str(e))
        
        # List snapshots
        try:
            result = await self.call_api("snapshot_list", {
                "dataset": self.test_dataset
            })
            snapshots = result.get("snapshots", [])
            if self.test_snapshot in snapshots:
                self.log_test("Snapshot list", "PASS", f"Found {len(snapshots)} snapshots")
            else:
                self.log_test("Snapshot list", "FAIL", "Test snapshot not found")
        except Exception as e:
            self.log_test("Snapshot list", "FAIL", str(e))
        
        # Hold snapshot
        try:
            result = await self.call_api("snapshot_hold", {
                "dataset": self.test_dataset,
                "snapshot": self.test_snapshot,
                "tag": "test_hold"
            })
            self.log_test("Snapshot hold", "PASS")
        except Exception as e:
            self.log_test("Snapshot hold", "FAIL", str(e))
        
        # List holds
        try:
            result = await self.call_api("snapshot_holds_list", {
                "dataset": self.test_dataset,
                "snapshot": self.test_snapshot
            })
            holds = result.get("holds", [])
            self.log_test("Snapshot holds list", "PASS", f"Found {len(holds)} holds")
        except Exception as e:
            self.log_test("Snapshot holds list", "FAIL", str(e))
        
        # Release hold
        try:
            result = await self.call_api("snapshot_release", {
                "dataset": self.test_dataset,
                "snapshot": self.test_snapshot,
                "tag": "test_hold"
            })
            self.log_test("Snapshot release", "PASS")
        except Exception as e:
            self.log_test("Snapshot release", "FAIL", str(e))
        
        # Test rollback (careful!)
        # try:
        #     result = await self.call_api("snapshot_rollback", {
        #         "dataset": self.test_dataset,
        #         "snapshot": self.test_snapshot
        #     })
        #     self.log_test("Snapshot rollback", "PASS")
        # except Exception as e:
        #     self.log_test("Snapshot rollback", "FAIL", str(e))
    
    async def test_bookmark_operations(self):
        """Test bookmark operations"""
        print("\n=== Testing Bookmark Operations ===")
        
        if not self.test_dataset or not self.test_snapshot:
            self.log_test("Bookmark operations", "SKIP", "No test dataset/snapshot available")
            return
        
        bookmark_name = f"{self.test_dataset}#test_bookmark"
        
        # Create bookmark
        try:
            result = await self.call_api("bookmark_create", {
                "snapshot": f"{self.test_dataset}@{self.test_snapshot}",
                "bookmark": bookmark_name
            })
            self.log_test("Bookmark create", "PASS", bookmark_name)
        except Exception as e:
            self.log_test("Bookmark create", "FAIL", str(e))
        
        # List bookmarks
        try:
            result = await self.call_api("bookmark_list", {
                "dataset": self.test_dataset
            })
            bookmarks = result.get("bookmarks", [])
            if bookmark_name in bookmarks:
                self.log_test("Bookmark list", "PASS", f"Found {len(bookmarks)} bookmarks")
            else:
                self.log_test("Bookmark list", "FAIL", "Test bookmark not found")
        except Exception as e:
            self.log_test("Bookmark list", "FAIL", str(e))
        
        # Destroy bookmark
        try:
            result = await self.call_api("bookmark_destroy", {
                "bookmark": bookmark_name
            })
            self.log_test("Bookmark destroy", "PASS")
        except Exception as e:
            self.log_test("Bookmark destroy", "FAIL", str(e))
    
    async def test_clone_operations(self):
        """Test clone operations"""
        print("\n=== Testing Clone Operations ===")
        
        if not self.test_dataset or not self.test_snapshot:
            self.log_test("Clone operations", "SKIP", "No test dataset/snapshot available")
            return
        
        clone_name = f"{self.test_dataset}_clone"
        
        # Create clone
        try:
            result = await self.call_api("clone_create", {
                "source": f"{self.test_dataset}@{self.test_snapshot}",
                "target": clone_name,
                "properties": {
                    "compression": "lz4"
                }
            })
            self.log_test("Clone create", "PASS", clone_name)
            
            # Destroy clone after test
            await self.call_api("dataset_destroy", {"dataset": clone_name})
            
        except Exception as e:
            self.log_test("Clone create", "FAIL", str(e))
    
    async def test_volume_operations(self):
        """Test volume operations"""
        print("\n=== Testing Volume Operations ===")
        
        if not self.test_pool:
            self.log_test("Volume operations", "SKIP", "No test pool available")
            return
        
        volume_name = f"{self.test_pool}/testvol_{int(time.time())}"
        
        # Create volume
        try:
            result = await self.call_api("volume_create", {
                "dataset": volume_name,
                "size_gb": 1,
                "compression": "lz4"
            })
            self.log_test("Volume create", "PASS", volume_name)
        except Exception as e:
            self.log_test("Volume create", "FAIL", str(e))
            return
        
        # List volumes
        try:
            result = await self.call_api("volume_list")
            volumes = result.get("volumes", [])
            if volume_name in volumes:
                self.log_test("Volume list", "PASS", "Test volume found")
            else:
                self.log_test("Volume list", "FAIL", "Test volume not in list")
        except Exception as e:
            self.log_test("Volume list", "FAIL", str(e))
        
        # Cleanup - destroy volume
        try:
            await self.call_api("dataset_destroy", {"dataset": volume_name})
        except:
            pass
    
    async def test_send_receive_operations(self):
        """Test send/receive operations"""
        print("\n=== Testing Send/Receive Operations ===")
        
        if not self.test_dataset or not self.test_snapshot:
            self.log_test("Send/Receive operations", "SKIP", "No test dataset/snapshot available")
            return
        
        # Test send estimate
        try:
            result = await self.call_api("send_estimate", {
                "dataset": self.test_dataset,
                "snapshot": self.test_snapshot,
                "recursive": False
            })
            size_bytes = result.get("size_bytes", 0)
            size_human = result.get("size_human", "")
            self.log_test("Send estimate", "PASS", f"Size: {size_human}")
        except Exception as e:
            self.log_test("Send estimate", "FAIL", str(e))
        
        # Test send to file
        send_file = f"/tmp/zfs_test_send_{int(time.time())}.zfs"
        try:
            result = await self.call_api("send_to_file", {
                "dataset": self.test_dataset,
                "snapshot": self.test_snapshot,
                "output_file": send_file,
                "compressed": True,
                "recursive": False
            })
            self.log_test("Send to file", "PASS", f"File: {send_file}.gz")
            send_file = f"{send_file}.gz"  # Update to compressed filename
        except Exception as e:
            self.log_test("Send to file", "FAIL", str(e))
            return
        
        # Test receive from file (to a new dataset)
        receive_dataset = f"{self.test_dataset}_received"
        try:
            result = await self.call_api("receive_from_file", {
                "dataset": receive_dataset,
                "input_file": send_file,
                "force": True,
                "compressed": True
            })
            self.log_test("Receive from file", "PASS", receive_dataset)
            
            # Cleanup received dataset
            await self.call_api("dataset_destroy", {
                "dataset": receive_dataset,
                "recursive": True
            })
        except Exception as e:
            self.log_test("Receive from file", "FAIL", str(e))
        
        # Test streaming operations
        await self.test_streaming_operations()
        
        # Cleanup send file
        try:
            import os
            if os.path.exists(send_file):
                os.remove(send_file)
        except:
            pass
    
    async def test_streaming_operations(self):
        """Test streaming send/receive operations"""
        print("\n=== Testing Streaming Operations ===")
        
        if not self.test_dataset or not self.test_snapshot:
            self.log_test("Streaming operations", "SKIP", "No test dataset/snapshot available")
            return
        
        # Test stream init for send
        try:
            result = await self.call_api("stream_init", {
                "operation": "send",
                "dataset": self.test_dataset,
                "snapshot": self.test_snapshot
            })
            stream_id = result.get("stream_id")
            stream_url = result.get("url")
            self.log_test("Stream init (send)", "PASS", f"ID: {stream_id[:8]}...")
            
            # Test stream list
            result = await self.call_api("stream_list", {})
            sessions = result.get("sessions", [])
            if any(s["stream_id"] == stream_id for s in sessions):
                self.log_test("Stream list", "PASS", f"Found {len(sessions)} sessions")
            else:
                self.log_test("Stream list", "FAIL", "Stream not in list")
            
            # Cancel the stream
            result = await self.call_api("stream_cancel", {"stream_id": stream_id})
            self.log_test("Stream cancel", "PASS")
            
        except Exception as e:
            self.log_test("Stream init (send)", "FAIL", str(e))
        
        # Test stream init for receive
        receive_dataset = f"{self.test_dataset}_stream_recv"
        try:
            result = await self.call_api("stream_init", {
                "operation": "receive",
                "dataset": receive_dataset,
                "target_dataset": receive_dataset,
                "force": True
            })
            stream_id = result.get("stream_id")
            self.log_test("Stream init (receive)", "PASS", f"ID: {stream_id[:8]}...")
            
            # Cancel this stream too
            await self.call_api("stream_cancel", {"stream_id": stream_id})
            
        except Exception as e:
            self.log_test("Stream init (receive)", "FAIL", str(e))
    
    async def cleanup(self):
        """Clean up test resources"""
        print("\n=== Cleanup ===")
        
        # Destroy test dataset and snapshots
        if self.test_dataset:
            try:
                await self.call_api("dataset_destroy", {
                    "dataset": self.test_dataset,
                    "recursive": True
                })
                self.log_test("Cleanup dataset", "PASS")
            except Exception as e:
                self.log_test("Cleanup dataset", "FAIL", str(e))
    
    def print_summary(self):
        """Print test summary"""
        print("\n=== Test Summary ===")
        passed = sum(1 for r in self.results if r["status"] == "PASS")
        failed = sum(1 for r in self.results if r["status"] == "FAIL")
        skipped = sum(1 for r in self.results if r["status"] == "SKIP")
        total = len(self.results)
        
        print(f"Total tests: {total}")
        print(f"Passed: {passed} ({passed/total*100:.1f}%)")
        print(f"Failed: {failed} ({failed/total*100:.1f}%)")
        print(f"Skipped: {skipped} ({skipped/total*100:.1f}%)")
        
        if failed > 0:
            print("\nFailed tests:")
            for r in self.results:
                if r["status"] == "FAIL":
                    print(f"  - {r['test']}: {r['details']}")
        
        # Save results to file
        with open("test_results.json", "w") as f:
            json.dump(self.results, f, indent=2)
        print("\nDetailed results saved to test_results.json")
    
    async def run_all_tests(self):
        """Run all tests"""
        print("ZFS API Server Test Suite")
        print("========================")
        print(f"Testing API at: {self.url}")
        print(f"Started at: {datetime.now().isoformat()}")
        
        await self.test_authentication()
        await self.test_pool_operations()
        await self.test_dataset_operations()
        await self.test_snapshot_operations()
        await self.test_bookmark_operations()
        await self.test_clone_operations()
        await self.test_volume_operations()
        await self.test_send_receive_operations()
        await self.cleanup()
        
        self.print_summary()


async def main():
    """Main test runner"""
    # Check command line arguments
    url = "http://localhost:8545"
    if len(sys.argv) > 1:
        url = sys.argv[1]
    
    print(f"Testing ZFS API at: {url}")
    print("Note: This test requires:")
    print("  - ZFS API server running on localhost")
    print("  - At least one ZFS pool available")
    print("  - Permissions to create/destroy test datasets")
    print("")
    
    # Wait for user confirmation
    response = input("Continue with tests? (y/N): ")
    if response.lower() != 'y':
        print("Tests cancelled")
        return
    
    async with ZFSAPITest(url) as tester:
        await tester.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main())