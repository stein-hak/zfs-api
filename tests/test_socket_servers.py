#!/usr/bin/env python3
"""
Test TCP and Unix socket servers with async token authentication
"""

import socket
import json
import os
import time
import requests
from typing import Dict, Any, Tuple

# API configuration
API_URL = "http://localhost:8545"
API_AUTH = ("admin", "admin")  # Update with your credentials

def create_send_token(dataset: str, snapshot: str) -> Tuple[str, str]:
    """Create a send token via JSON-RPC"""
    payload = {
        "jsonrpc": "2.0",
        "method": "token_create_send",
        "params": {
            "dataset": dataset,
            "snapshot": snapshot,
            "ttl": 300
        },
        "id": 1
    }
    
    response = requests.post(API_URL, json=payload, auth=API_AUTH)
    result = response.json()
    
    if "result" in result:
        token_info = result["result"]
        # The token and token_id might be the same value
        token = token_info.get("token", token_info.get("token_id"))
        token_id = token_info.get("token_id", token_info.get("token"))
        if not token:
            raise Exception(f"No token in response: {token_info}")
        return token, token_id
    else:
        raise Exception(f"Failed to create token: {result.get('error')}")

def create_receive_token(dataset: str) -> Tuple[str, str]:
    """Create a receive token via JSON-RPC"""
    payload = {
        "jsonrpc": "2.0",
        "method": "token_create_receive",
        "params": {
            "dataset": dataset,
            "ttl": 300,
            "force": True
        },
        "id": 1
    }
    
    response = requests.post(API_URL, json=payload, auth=API_AUTH)
    result = response.json()
    
    if "result" in result:
        token_info = result["result"]
        # The token and token_id might be the same value
        token = token_info.get("token", token_info.get("token_id"))
        token_id = token_info.get("token_id", token_info.get("token"))
        if not token:
            raise Exception(f"No token in response: {token_info}")
        return token, token_id
    else:
        raise Exception(f"Failed to create token: {result.get('error')}")

def test_tcp_socket(host: str = "localhost", port: int = 9999):
    """Test TCP socket server with token authentication"""
    print(f"\n=== Testing TCP Socket Server on {host}:{port} ===")
    
    # Create a send token
    dataset = "syspool/benchmark_test"
    snapshot = "snap_5120mb"
    
    try:
        token, token_id = create_send_token(dataset, snapshot)
        print(f"✓ Created send token: {token[:20]}... (ID: {token_id})")
    except Exception as e:
        print(f"❌ Failed to create send token: {e}")
        return
    
    # Connect to TCP socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        print(f"✓ Connected to TCP socket server")
        
        # Send the token using binary protocol
        # First send the length (4 bytes, big-endian)
        import struct
        token_bytes = token.encode('utf-8')
        length_prefix = struct.pack("!I", len(token_bytes))
        
        sock.sendall(length_prefix)
        sock.sendall(token_bytes)
        print(f"✓ Sent authentication message (binary protocol)")
        
        # Receive response (binary protocol)
        # First read 4-byte length header
        length_data = sock.recv(4)
        if len(length_data) == 4:
            response_length = struct.unpack("!I", length_data)[0]
            # Read the actual response
            response_data = sock.recv(response_length)
            response = response_data.decode('utf-8')
            print(f"✓ Server response: {response.strip()}")
        else:
            print("❌ Failed to read response length")
        
        # Check if authenticated
        try:
            resp_data = json.loads(response)
            if resp_data.get("status") == "authenticated" or "authenticated" in str(resp_data).lower():
            print("✓ Authentication successful!")
            
            # For a send operation, we would normally pipe data here
            # Just close for this test
            sock.close()
            print("✓ Connection closed")
        else:
            print("❌ Authentication failed")
            sock.close()
            
    except Exception as e:
        print(f"❌ TCP socket error: {e}")

def test_unix_socket(socket_path: str = "./zfs_token_socket"):
    """Test Unix socket server with token authentication"""
    print(f"\n=== Testing Unix Socket Server at {socket_path} ===")
    
    # Create a receive token
    dataset = "syspool/test_receive"
    
    try:
        token, token_id = create_receive_token(dataset)
        print(f"✓ Created receive token: {token[:20]}... (ID: {token_id})")
    except Exception as e:
        print(f"❌ Failed to create receive token: {e}")
        return
    
    # Connect to Unix socket
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(socket_path)
        print(f"✓ Connected to Unix socket server")
        
        # Send the token using binary protocol
        # First send the length (4 bytes, big-endian)
        import struct
        token_bytes = token.encode('utf-8')
        length_prefix = struct.pack("!I", len(token_bytes))
        
        sock.sendall(length_prefix)
        sock.sendall(token_bytes)
        print(f"✓ Sent authentication message (binary protocol)")
        
        # Receive response (binary protocol)
        # First read 4-byte length header
        length_data = sock.recv(4)
        if len(length_data) == 4:
            response_length = struct.unpack("!I", length_data)[0]
            # Read the actual response
            response_data = sock.recv(response_length)
            response = response_data.decode('utf-8')
            print(f"✓ Server response: {response.strip()}")
        else:
            print("❌ Failed to read response length")
        
        # Check if authenticated
        try:
            resp_data = json.loads(response)
            if resp_data.get("status") == "authenticated" or "authenticated" in str(resp_data).lower():
            print("✓ Authentication successful!")
            
            # For a receive operation, we would normally send data here
            # Just close for this test
            sock.close()
            print("✓ Connection closed")
        else:
            print("❌ Authentication failed")
            sock.close()
            
    except Exception as e:
        print(f"❌ Unix socket error: {e}")

def test_token_listing():
    """Test listing tokens"""
    print("\n=== Testing Token Listing ===")
    
    payload = {
        "jsonrpc": "2.0",
        "method": "token_list",
        "params": {},
        "id": 1
    }
    
    response = requests.post(API_URL, json=payload, auth=API_AUTH)
    result = response.json()
    
    if "result" in result:
        tokens = result["result"].get("tokens", [])
        print(f"✓ Found {len(tokens)} active tokens")
        for token in tokens[:5]:  # Show first 5
            print(f"  - {token['id']}: {token['operation']} for {token['dataset']}")
    else:
        print(f"❌ Failed to list tokens: {result.get('error')}")

def test_send_receive_flow():
    """Test a complete send/receive flow with sockets"""
    print("\n=== Testing Send/Receive Flow ===")
    
    # Create a small test snapshot
    test_dataset = "syspool/test_socket"
    test_snapshot = "test_snap_socket"
    
    print(f"1. Creating test dataset and snapshot...")
    # Clean up any existing test dataset first
    os.system(f"zfs destroy -r {test_dataset} 2>/dev/null")
    os.system(f"zfs create -p {test_dataset} 2>/dev/null")
    os.system(f"zfs snapshot {test_dataset}@{test_snapshot}")
    print(f"✓ Created {test_dataset}@{test_snapshot}")
    
    # Create tokens
    send_token, send_id = create_send_token(test_dataset, test_snapshot)
    print(f"✓ Send token: {send_token[:20]}... (ID: {send_id})")
    
    receive_dataset = "syspool/test_socket_recv"
    receive_token, receive_id = create_receive_token(receive_dataset)
    print(f"✓ Receive token: {receive_token[:20]}... (ID: {receive_id})")
    
    # Test actual send/receive
    print("\n2. Testing actual send/receive operation...")
    
    # Use zfs send | nc for TCP test
    token_json = json.dumps({"token": send_token, "operation": "send"})
    send_cmd = f'zfs send {test_dataset}@{test_snapshot} | (echo \'{token_json}\'; cat) | nc localhost 9999'
    
    print(f"✓ Send command prepared: {send_cmd[:80]}...")
    print("  (not executing to avoid data transfer)")
    
    # Cleanup
    print("\n3. Cleanup...")
    os.system(f"zfs destroy -r {test_dataset} 2>/dev/null")
    print(f"✓ Cleaned up test dataset")

def main():
    """Run all tests"""
    print("Starting Socket Server Tests with Async Token Authentication")
    print("=" * 60)
    
    # Test token operations
    test_token_listing()
    
    # Test socket servers
    test_tcp_socket()
    test_unix_socket()
    
    # Test send/receive flow
    test_send_receive_flow()
    
    print("\n" + "=" * 60)
    print("✅ All tests completed!")

if __name__ == "__main__":
    main()