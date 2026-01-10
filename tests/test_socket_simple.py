#!/usr/bin/env python3
"""
Simple test for socket servers - just verify authentication works
"""

import socket
import json
import struct
import requests
import time
import subprocess
import os

# API configuration
API_URL = "http://localhost:8545"
API_AUTH = ("admin", "admin")

def create_send_token(dataset: str, snapshot: str):
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
        token = token_info.get("token", token_info.get("token_id"))
        return token
    else:
        raise Exception(f"Failed to create token: {result.get('error')}")

def create_receive_token(dataset: str):
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
        token = token_info.get("token", token_info.get("token_id"))
        return token
    else:
        raise Exception(f"Failed to create token: {result.get('error')}")

def test_tcp_auth():
    """Test TCP socket authentication only"""
    print("\n=== Testing TCP Socket Authentication ===")
    
    # Create send token for existing snapshot
    token = create_send_token("syspool/benchmark_test", "snap_5120mb")
    print(f"✓ Created send token: {token[:20]}...")
    
    # Connect and authenticate
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("localhost", 9999))
    print(f"✓ Connected to TCP socket")
    
    # Send token using binary protocol
    token_bytes = token.encode('utf-8')
    length_prefix = struct.pack("!I", len(token_bytes))
    sock.sendall(length_prefix)
    sock.sendall(token_bytes)
    print(f"✓ Sent authentication")
    
    # Read response
    length_data = sock.recv(4)
    if len(length_data) == 4:
        response_length = struct.unpack("!I", length_data)[0]
        response_data = sock.recv(response_length)
        response = response_data.decode('utf-8')
        resp_data = json.loads(response)
        
        if resp_data.get("status") == "started":
            print(f"✓ Authentication successful!")
            print(f"  Operation: {resp_data.get('operation')}")
            print(f"  Dataset: {resp_data.get('dataset')}")
            print(f"  Command: {resp_data.get('command')}")
            
            # Read just a small amount of data to verify stream started
            sock.settimeout(2.0)  # 2 second timeout
            try:
                # Read chunk size (8 bytes)
                size_data = sock.recv(8)
                if len(size_data) == 8:
                    chunk_size = struct.unpack("!Q", size_data)[0]
                    print(f"✓ Server started sending data (first chunk size: {chunk_size} bytes)")
                    
                    # Read just first 1KB of the chunk to verify
                    sample_size = min(chunk_size, 1024)
                    sample_data = sock.recv(sample_size)
                    print(f"✓ Received {len(sample_data)} bytes of ZFS stream data")
            except socket.timeout:
                print(f"⚠️  Timeout reading data (this may be normal)")
        else:
            print(f"❌ Unexpected response: {resp_data}")
    else:
        print(f"❌ Failed to read response")
    
    sock.close()
    print(f"✓ Connection closed")

def test_unix_auth():
    """Test Unix socket authentication only"""
    print("\n=== Testing Unix Socket Authentication ===")
    
    # Create receive token
    token = create_receive_token("syspool/test_receive")
    print(f"✓ Created receive token: {token[:20]}...")
    
    # Connect and authenticate
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect("./zfs_token_socket")
    print(f"✓ Connected to Unix socket")
    
    # Send token using binary protocol
    token_bytes = token.encode('utf-8')
    length_prefix = struct.pack("!I", len(token_bytes))
    sock.sendall(length_prefix)
    sock.sendall(token_bytes)
    print(f"✓ Sent authentication")
    
    # Read response
    length_data = sock.recv(4)
    if len(length_data) == 4:
        response_length = struct.unpack("!I", length_data)[0]
        response_data = sock.recv(response_length)
        response = response_data.decode('utf-8')
        resp_data = json.loads(response)
        
        if resp_data.get("status") == "started":
            print(f"✓ Authentication successful!")
            print(f"  Operation: {resp_data.get('operation')}")
            print(f"  Dataset: {resp_data.get('dataset')}")
            print(f"  Command: {resp_data.get('command')}")
            
            # For receive, we would send data, but let's just close
            print(f"  (Server is waiting for data - closing connection)")
        else:
            print(f"❌ Unexpected response: {resp_data}")
    else:
        print(f"❌ Failed to read response")
    
    sock.close()
    print(f"✓ Connection closed")

def test_piped_send():
    """Test using echo to send minimal data through socket"""
    print("\n=== Testing Minimal Data Transfer ===")
    
    # Create receive token
    token = create_receive_token("syspool/test_minimal")
    print(f"✓ Created receive token: {token[:20]}...")
    
    # Create a simple connect script
    connect_script = f'''
import socket
import struct
import sys

sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.connect("./zfs_token_socket")

# Send token
token = "{token}"
token_bytes = token.encode('utf-8')
sock.sendall(struct.pack("!I", len(token_bytes)))
sock.sendall(token_bytes)

# Read response
length_data = sock.recv(4)
if len(length_data) == 4:
    response_length = struct.unpack("!I", length_data)[0]
    response_data = sock.recv(response_length)
    print(f"Server response: {{response_data.decode('utf-8')}}")

# Send data from stdin
data = sys.stdin.buffer.read()
print(f"Sending {{len(data)}} bytes")
sock.sendall(data)
sock.shutdown(socket.SHUT_WR)

# Wait a bit
import time
time.sleep(1)

sock.close()
'''
    
    with open("/tmp/simple_connect.py", "w") as f:
        f.write(connect_script)
    
    # Send a small amount of test data
    print(f"✓ Sending test data through socket...")
    result = subprocess.run(
        "echo 'Test data' | python3 /tmp/simple_connect.py",
        shell=True,
        capture_output=True,
        text=True
    )
    
    print(f"  Exit code: {result.returncode}")
    if result.stdout:
        print(f"  Output: {result.stdout.strip()}")
    if result.stderr:
        print(f"  Error: {result.stderr.strip()}")
    
    # Cleanup
    os.remove("/tmp/simple_connect.py")

def main():
    """Run simple tests"""
    print("Starting Simple Socket Server Tests")
    print("=" * 60)
    
    # Test authentication
    test_tcp_auth()
    test_unix_auth()
    
    # Test minimal data transfer
    test_piped_send()
    
    print("\n" + "=" * 60)
    print("✅ All tests completed!")

if __name__ == "__main__":
    main()