#!/usr/bin/env python3
"""
Test actual ZFS send/receive through socket servers with small datasets
"""

import socket
import json
import struct
import requests
import time
import subprocess
import os
import sys

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
        return token_info.get("token", token_info.get("token_id"))
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
        return token_info.get("token", token_info.get("token_id"))
    else:
        raise Exception(f"Failed to create token: {result.get('error')}")

def test_small_transfer():
    """Test with a very small dataset"""
    print("\n=== Testing Small Dataset Transfer ===")
    
    # Create a small test dataset
    test_dataset = "syspool/socket_small_test"
    test_snapshot = "test_snap"
    receive_dataset = "syspool/socket_small_recv"
    
    print(f"\n1. Creating small test dataset...")
    # Cleanup first
    subprocess.run(f"zfs destroy -r {test_dataset} 2>/dev/null", shell=True)
    subprocess.run(f"zfs destroy -r {receive_dataset} 2>/dev/null", shell=True)
    
    # Create dataset with minimal data
    subprocess.run(f"zfs create {test_dataset}", shell=True, check=True)
    subprocess.run(f"echo 'Small test data' > /{test_dataset}/test.txt", shell=True, check=True)
    subprocess.run(f"zfs snapshot {test_dataset}@{test_snapshot}", shell=True, check=True)
    print(f"✓ Created {test_dataset}@{test_snapshot}")
    
    # Get snapshot size
    size_result = subprocess.run(
        f"zfs send -nv {test_dataset}@{test_snapshot}",
        shell=True,
        capture_output=True,
        text=True
    )
    print(f"  Snapshot size: {size_result.stderr.strip()}")
    
    # Create tokens
    print(f"\n2. Creating tokens...")
    send_token = create_send_token(test_dataset, test_snapshot)
    receive_token = create_receive_token(receive_dataset)
    print(f"✓ Send token: {send_token[:20]}...")
    print(f"✓ Receive token: {receive_token[:20]}...")
    
    # Create the pipe command
    print(f"\n3. Running transfer...")
    
    # Create scripts for both ends
    send_script = f'''
import socket
import struct
import sys

# Connect to TCP socket for send
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(("localhost", 9999))

# Send token
token = "{send_token}"
token_bytes = token.encode('utf-8')
sock.sendall(struct.pack("!I", len(token_bytes)))
sock.sendall(token_bytes)

# Read authentication response
length_data = sock.recv(4)
if len(length_data) == 4:
    response_length = struct.unpack("!I", length_data)[0]
    response_data = sock.recv(response_length)
    resp = response_data.decode('utf-8')
    print(f"Send auth response: {{resp}}", file=sys.stderr)

# Now receive ZFS stream chunks and write to stdout
total = 0
while True:
    # Read chunk size (8 bytes)
    size_data = sock.recv(8)
    if len(size_data) < 8:
        break
    
    chunk_size = struct.unpack("!Q", size_data)[0]
    if chunk_size == 0:
        # End marker
        break
    
    # Read chunk data
    chunk = b""
    while len(chunk) < chunk_size:
        data = sock.recv(min(chunk_size - len(chunk), 65536))
        if not data:
            break
        chunk += data
    
    # Write to stdout for piping
    sys.stdout.buffer.write(chunk)
    sys.stdout.buffer.flush()
    total += len(chunk)

sock.close()
print(f"Received total {{total}} bytes", file=sys.stderr)
'''

    receive_script = f'''
import socket
import struct
import sys

# Connect to Unix socket for receive
sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.connect("./zfs_token_socket")

# Send token
token = "{receive_token}"
token_bytes = token.encode('utf-8')
sock.sendall(struct.pack("!I", len(token_bytes)))
sock.sendall(token_bytes)

# Read authentication response
length_data = sock.recv(4)
if len(length_data) == 4:
    response_length = struct.unpack("!I", length_data)[0]
    response_data = sock.recv(response_length)
    resp = response_data.decode('utf-8')
    print(f"Receive auth response: {{resp}}", file=sys.stderr)

# Now read from stdin and send to socket
total = 0
while True:
    data = sys.stdin.buffer.read(65536)
    if not data:
        break
    sock.sendall(data)
    total += len(data)

sock.shutdown(socket.SHUT_WR)
print(f"Sent total {{total}} bytes", file=sys.stderr)

# Wait a bit for server to finish
import time
time.sleep(2)

sock.close()
'''

    # Write scripts
    with open("/tmp/send_script.py", "w") as f:
        f.write(send_script)
    with open("/tmp/receive_script.py", "w") as f:
        f.write(receive_script)
    
    # Run the transfer
    cmd = "python3 /tmp/send_script.py | python3 /tmp/receive_script.py"
    print(f"✓ Running: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    print(f"\nTransfer result:")
    print(f"  Exit code: {result.returncode}")
    if result.stderr:
        for line in result.stderr.strip().split('\n'):
            print(f"  {line}")
    
    # Verify the transfer
    print(f"\n4. Verifying transfer...")
    verify_result = subprocess.run(f"zfs list {receive_dataset}", shell=True, capture_output=True, text=True)
    
    if verify_result.returncode == 0:
        print(f"✓ Dataset {receive_dataset} created successfully!")
        
        # Mount the received dataset
        mount_result = subprocess.run(f"zfs mount {receive_dataset} 2>/dev/null", shell=True)
        
        # List snapshots
        snap_result = subprocess.run(
            f"zfs list -t snapshot -r {receive_dataset}",
            shell=True,
            capture_output=True,
            text=True
        )
        if snap_result.stdout:
            print(f"✓ Snapshots:")
            for line in snap_result.stdout.strip().split('\n'):
                print(f"    {line}")
        
        # Check mount status
        mount_check = subprocess.run(
            f"zfs get mounted {receive_dataset}",
            shell=True,
            capture_output=True,
            text=True
        )
        print(f"  Mount status: {mount_check.stdout.strip()}")
        
        # Compare contents if mounted
        if "mounted" in mount_check.stdout and "yes" in mount_check.stdout:
            diff_result = subprocess.run(
                f"diff -r /{test_dataset} /{receive_dataset}",
                shell=True,
                capture_output=True,
                text=True
            )
            
            if diff_result.returncode == 0:
                print(f"✓ Data verified - contents match!")
            else:
                print(f"  Note: {diff_result.stdout}")
        else:
            print(f"  Note: Dataset not mounted, skipping content verification")
    else:
        print(f"❌ Dataset {receive_dataset} not found")
        print(f"  Error: {verify_result.stderr}")
    
    # Cleanup
    print(f"\n5. Cleanup...")
    subprocess.run(f"zfs destroy -r {test_dataset} 2>/dev/null", shell=True)
    subprocess.run(f"zfs destroy -r {receive_dataset} 2>/dev/null", shell=True)
    os.remove("/tmp/send_script.py")
    os.remove("/tmp/receive_script.py")
    print(f"✓ Cleanup completed")

def test_direct_pipe():
    """Test direct piping with zfs commands"""
    print("\n=== Testing Direct ZFS Pipe Transfer ===")
    
    # Create test dataset
    test_dataset = "syspool/pipe_test"
    test_snapshot = "snap1"
    receive_dataset = "syspool/pipe_recv"
    
    print(f"\n1. Creating test dataset...")
    subprocess.run(f"zfs destroy -r {test_dataset} 2>/dev/null", shell=True)
    subprocess.run(f"zfs destroy -r {receive_dataset} 2>/dev/null", shell=True)
    
    subprocess.run(f"zfs create {test_dataset}", shell=True, check=True)
    subprocess.run(f"echo 'Pipe test data' > /{test_dataset}/data.txt", shell=True, check=True)
    subprocess.run(f"zfs snapshot {test_dataset}@{test_snapshot}", shell=True, check=True)
    print(f"✓ Created {test_dataset}@{test_snapshot}")
    
    # Create receive token
    receive_token = create_receive_token(receive_dataset)
    print(f"✓ Receive token: {receive_token[:20]}...")
    
    # Create a simple wrapper for receive
    wrapper_script = f'''#!/bin/sh
echo "{receive_token}" | socat - UNIX-CONNECT:./zfs_token_socket
'''
    
    with open("/tmp/zfs_receive_wrapper.sh", "w") as f:
        f.write(wrapper_script)
    os.chmod("/tmp/zfs_receive_wrapper.sh", 0o755)
    
    # Try the transfer
    print(f"\n2. Running transfer...")
    cmd = f"zfs send {test_dataset}@{test_snapshot} | /tmp/zfs_receive_wrapper.sh"
    print(f"✓ Running: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    print(f"  Exit code: {result.returncode}")
    if result.stderr:
        print(f"  Error: {result.stderr}")
    
    # Cleanup
    subprocess.run(f"zfs destroy -r {test_dataset} 2>/dev/null", shell=True)
    subprocess.run(f"zfs destroy -r {receive_dataset} 2>/dev/null", shell=True)
    os.remove("/tmp/zfs_receive_wrapper.sh")

def main():
    """Run transfer tests"""
    print("Starting ZFS Socket Transfer Tests")
    print("=" * 60)
    
    # Test with small dataset
    test_small_transfer()
    
    # Test direct piping (commented out for now as it needs socat)
    # test_direct_pipe()
    
    print("\n" + "=" * 60)
    print("✅ All tests completed!")

if __name__ == "__main__":
    main()