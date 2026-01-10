#!/usr/bin/env python3
"""
Test actual ZFS send/receive through socket servers
"""

import socket
import json
import os
import time
import struct
import subprocess
import threading
import requests
from typing import Dict, Any, Tuple

# API configuration
API_URL = "http://localhost:8545"
API_AUTH = ("admin", "admin")

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
        token = token_info.get("token", token_info.get("token_id"))
        token_id = token_info.get("token_id", token_info.get("token"))
        if not token:
            raise Exception(f"No token in response: {token_info}")
        return token, token_id
    else:
        raise Exception(f"Failed to create token: {result.get('error')}")

def send_via_tcp(dataset: str, snapshot: str, host: str = "localhost", port: int = 9999):
    """Send a ZFS snapshot through TCP socket server"""
    print(f"\n=== Sending {dataset}@{snapshot} via TCP ===")
    
    # Create send token
    token, token_id = create_send_token(dataset, snapshot)
    print(f"✓ Created send token: {token[:20]}...")
    
    # Connect to TCP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    print(f"✓ Connected to TCP socket server")
    
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
            print(f"✓ Authenticated - server ready for send operation")
            
            # Now we need to receive the ZFS data chunks from the server
            # The server will be running zfs send and sending us the output
            received_data = []
            total_bytes = 0
            
            while True:
                # Read chunk size (8 bytes)
                size_data = sock.recv(8)
                if len(size_data) < 8:
                    break
                
                chunk_size = struct.unpack("!Q", size_data)[0]
                if chunk_size == 0:
                    # End marker
                    print(f"✓ Received end marker")
                    break
                
                # Read chunk data
                chunk = b""
                while len(chunk) < chunk_size:
                    data = sock.recv(min(chunk_size - len(chunk), 65536))
                    if not data:
                        break
                    chunk += data
                
                received_data.append(chunk)
                total_bytes += len(chunk)
                
                if total_bytes % (100 * 1024 * 1024) < len(chunk):
                    print(f"  Received {total_bytes / (1024*1024):.1f} MB")
            
            print(f"✓ Received total {total_bytes / (1024*1024):.1f} MB")
            
            # Check if there's an error message
            if total_bytes > 0 and len(received_data) == 1 and received_data[0].startswith(b"ZFS send failed"):
                print(f"❌ Server error: {received_data[0].decode('utf-8')}")
                return None
            
            return b"".join(received_data)
        else:
            print(f"❌ Authentication failed: {resp_data}")
            return None
    else:
        print(f"❌ Failed to read response")
        return None
    
    sock.close()

def receive_via_unix(zfs_data: bytes, dataset: str, socket_path: str = "./zfs_token_socket"):
    """Receive ZFS data through Unix socket server"""
    print(f"\n=== Receiving to {dataset} via Unix socket ===")
    
    # Create receive token
    token, token_id = create_receive_token(dataset)
    print(f"✓ Created receive token: {token[:20]}...")
    
    # Connect to Unix socket
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(socket_path)
    print(f"✓ Connected to Unix socket server")
    
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
            print(f"✓ Authenticated - server ready for receive operation")
            
            # Send the ZFS data
            total_sent = 0
            chunk_size = 64 * 1024  # 64KB chunks
            
            for i in range(0, len(zfs_data), chunk_size):
                chunk = zfs_data[i:i + chunk_size]
                sock.sendall(chunk)
                total_sent += len(chunk)
                
                if total_sent % (100 * 1024 * 1024) < len(chunk):
                    print(f"  Sent {total_sent / (1024*1024):.1f} MB")
            
            print(f"✓ Sent total {total_sent / (1024*1024):.1f} MB")
            
            # Close our end to signal EOF
            sock.shutdown(socket.SHUT_WR)
            
            # Wait a bit for the server to process
            time.sleep(2)
            
            # Server should close the connection when done
            sock.close()
            print(f"✓ Transfer completed")
            return True
        else:
            print(f"❌ Authentication failed: {resp_data}")
            return False
    else:
        print(f"❌ Failed to read response")
        return False

def test_send_receive():
    """Test complete send/receive flow"""
    print("\n=== Testing Complete Send/Receive Flow ===")
    
    # Use existing dataset and snapshot
    test_dataset = "syspool/benchmark_test"
    test_snapshot = "snap_5120mb"
    receive_dataset = "syspool/socket_test_recv"
    
    print(f"\n1. Using existing dataset...")
    # Clean up any existing receive dataset
    os.system(f"zfs destroy -r {receive_dataset} 2>/dev/null")
    
    print(f"✓ Using {test_dataset}@{test_snapshot}")
    
    # Get snapshot size
    size_output = subprocess.check_output([
        "zfs", "send", "--dryrun", f"{test_dataset}@{test_snapshot}"
    ], stderr=subprocess.STDOUT).decode('utf-8')
    print(f"  Snapshot info: {size_output.strip()}")
    
    try:
        # Send via TCP
        print(f"\n2. Sending snapshot via TCP socket...")
        zfs_data = send_via_tcp(test_dataset, test_snapshot)
        
        if zfs_data:
            print(f"✓ Successfully received ZFS stream data")
            
            # Receive via Unix socket
            print(f"\n3. Receiving snapshot via Unix socket...")
            success = receive_via_unix(zfs_data, receive_dataset)
            
            if success:
                # Verify the received dataset
                print(f"\n4. Verifying received dataset...")
                
                # Check if dataset exists
                check_cmd = ["zfs", "list", receive_dataset]
                result = subprocess.run(check_cmd, capture_output=True, text=True)
                
                if result.returncode == 0:
                    print(f"✓ Dataset {receive_dataset} created successfully")
                    
                    # Compare files
                    diff_cmd = ["diff", "-r", f"/{test_dataset}", f"/{receive_dataset}"]
                    diff_result = subprocess.run(diff_cmd, capture_output=True, text=True)
                    
                    if diff_result.returncode == 0:
                        print(f"✓ Data verified - contents match!")
                    else:
                        print(f"❌ Data mismatch: {diff_result.stdout}")
                else:
                    print(f"❌ Dataset {receive_dataset} not found")
        else:
            print(f"❌ Failed to send snapshot")
            
    finally:
        # Cleanup
        print(f"\n5. Cleanup...")
        os.system(f"zfs destroy -r {receive_dataset} 2>/dev/null")
        print(f"✓ Cleaned up test datasets")

def test_receive_direct():
    """Test sending directly to receive socket using zfs send"""
    print("\n=== Testing Direct ZFS Send to Socket ===")
    
    test_dataset = "syspool/direct_test"
    test_snapshot = "snap1"
    receive_dataset = "syspool/direct_test_recv"
    
    # Prepare test dataset
    os.system(f"zfs destroy -r {test_dataset} 2>/dev/null")
    os.system(f"zfs destroy -r {receive_dataset} 2>/dev/null")
    os.system(f"zfs create -p {test_dataset}")
    os.system(f"echo 'Direct test' > /{test_dataset}/test.txt")
    os.system(f"zfs snapshot {test_dataset}@{test_snapshot}")
    print(f"✓ Created {test_dataset}@{test_snapshot}")
    
    # Create receive token
    token, token_id = create_receive_token(receive_dataset)
    print(f"✓ Created receive token: {token[:20]}...")
    
    # Use a small Python script to connect and send token, then pipe data
    connect_script = f"""
import socket
import struct
import sys

# Connect to Unix socket
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
    
# Now pipe stdin to socket
while True:
    data = sys.stdin.buffer.read(65536)
    if not data:
        break
    sock.sendall(data)

sock.shutdown(socket.SHUT_WR)
sock.close()
"""
    
    # Write connect script
    with open("/tmp/socket_connect.py", "w") as f:
        f.write(connect_script)
    
    # Run zfs send piped to our connect script
    print(f"✓ Sending via: zfs send {test_dataset}@{test_snapshot} | python3 /tmp/socket_connect.py")
    result = os.system(f"zfs send {test_dataset}@{test_snapshot} | python3 /tmp/socket_connect.py")
    
    if result == 0:
        print(f"✓ Send completed successfully")
        
        # Verify
        check_result = subprocess.run(["zfs", "list", receive_dataset], capture_output=True)
        if check_result.returncode == 0:
            print(f"✓ Dataset {receive_dataset} created successfully")
        else:
            print(f"❌ Dataset {receive_dataset} not found")
    else:
        print(f"❌ Send failed with code {result}")
    
    # Cleanup
    os.system(f"zfs destroy -r {test_dataset} 2>/dev/null")
    os.system(f"zfs destroy -r {receive_dataset} 2>/dev/null")
    os.remove("/tmp/socket_connect.py")
    print(f"✓ Cleanup completed")

def main():
    """Run all tests"""
    print("Starting Socket Server Transfer Tests")
    print("=" * 60)
    
    # Test complete flow
    test_send_receive()
    
    # Test direct send
    test_receive_direct()
    
    print("\n" + "=" * 60)
    print("✅ All tests completed!")

if __name__ == "__main__":
    main()