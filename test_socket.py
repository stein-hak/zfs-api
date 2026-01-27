#!/usr/bin/env python3
"""Test Unix socket connection"""
import socket
import struct
import json
import sys

socket_path = '/home/stein/python/zfs-api/zfs_token_socket'

# Connect to Unix socket
sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

try:
    print(f"Connecting to {socket_path}...")
    sock.connect(socket_path)
    print(f"✓ Connected!")

    # Send operation data as JSON (for Unix socket passwordless auth)
    operation_data = {
        "operation": "send",
        "dataset": "syspool/test",
        "snapshot": "test_snap"
    }
    test_token = json.dumps(operation_data)
    token_bytes = test_token.encode('utf-8')

    print(f"Sending token (length: {len(token_bytes)})...")
    sock.send(struct.pack("!I", len(token_bytes)))
    sock.send(token_bytes)
    print(f"✓ Token sent")

    # Read response
    sock.settimeout(3.0)
    print("Waiting for response...")

    response_len_data = sock.recv(4)
    if not response_len_data:
        print("✗ No response received (connection closed)")
        sys.exit(1)

    response_len = struct.unpack("!I", response_len_data)[0]
    print(f"✓ Response length: {response_len} bytes")

    response_data = b''
    while len(response_data) < response_len:
        chunk = sock.recv(response_len - len(response_data))
        if not chunk:
            break
        response_data += chunk

    response = json.loads(response_data.decode('utf-8'))
    print(f"\n✓ Response received:")
    print(json.dumps(response, indent=2))

except socket.timeout:
    print("✗ Timeout waiting for response")
    sys.exit(1)
except ConnectionRefusedError:
    print(f"✗ Connection refused - socket server not running")
    sys.exit(1)
except PermissionError as e:
    print(f"✗ Permission denied: {e}")
    print("Try running with: sudo python3 test_socket.py")
    sys.exit(1)
except Exception as e:
    print(f"✗ Error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    sock.close()
    print("\n✓ Socket closed")
