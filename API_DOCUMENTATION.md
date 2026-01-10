# ZFS API Documentation

## Overview

The ZFS API provides a JSON-RPC interface for managing ZFS filesystems, snapshots, and performing send/receive operations. It includes both HTTP API endpoints and high-performance socket servers for streaming operations.

## Base URL

```
http://localhost:8545
```

## Authentication

Optional bearer token authentication:
```
Authorization: Bearer <token>
```

## API Methods

### Dataset Operations

#### dataset_list
List all ZFS datasets on the system.

**Parameters:**
- `dataset` (string, optional): Parent dataset to list children of (if omitted, lists all datasets)

**Returns:** List of dataset names

**Example:**
```json
{
  "jsonrpc": "2.0",
  "method": "dataset_list",
  "params": {},
  "id": 1
}
```

---

#### dataset_create
Create a new ZFS dataset.

**Parameters:**
- `dataset` (string, required): Dataset name (e.g., "pool/dataset")
- `properties` (object, optional): ZFS properties to set

**Example:**
```json
{
  "jsonrpc": "2.0",
  "method": "dataset_create",
  "params": {
    "dataset": "pool/mydataset",
    "properties": {
      "compression": "lz4",
      "quota": "10G"
    }
  },
  "id": 1
}
```

---

#### dataset_destroy
Destroy a ZFS dataset.

**Parameters:**
- `dataset` (string, required): Dataset name
- `recursive` (boolean, optional): Recursively destroy children (default: false)

---

#### dataset_get_properties
Get properties of a dataset.

**Parameters:**
- `dataset` (string, required): Dataset name
- `property` (string, optional): Specific property to get (if omitted, returns all properties)

**Returns:** Property value or object with all dataset properties

---

#### dataset_set_property
Set a property on a dataset.

**Parameters:**
- `dataset` (string, required): Dataset name
- `property` (string, required): Property name
- `value` (string, required): Property value

---

### Snapshot Operations

#### snapshot_list
List snapshots for a dataset.

**Parameters:**
- `dataset` (string, required): Dataset name

**Returns:** List of snapshot objects with name and creation time

---

#### snapshot_create
Create a snapshot.

**Parameters:**
- `dataset` (string, required): Dataset name
- `name` (string, required): Snapshot name
- `recursive` (boolean, optional): Recursively snapshot children

**Example:**
```json
{
  "jsonrpc": "2.0",
  "method": "snapshot_create",
  "params": {
    "dataset": "pool/dataset",
    "name": "backup_2024"
  },
  "id": 1
}
```

---

#### snapshot_destroy
Destroy a snapshot.

**Parameters:**
- `dataset` (string, required): Dataset name
- `snapshot` (string, required): Snapshot name
- `recursive` (boolean, optional): Recursively destroy dependent snapshots (default: false)

---

#### snapshot_rollback
Rollback dataset to a snapshot.

**Parameters:**
- `dataset` (string, required): Dataset name
- `snapshot` (string, required): Snapshot name

---

#### snapshot_diff
Compare differences between snapshots or between a snapshot and current filesystem.

**Parameters:**
- `snapshot1` (string, required): First snapshot (format: dataset@snapshot)
- `snapshot2` (string, optional): Second snapshot. If not provided, compares against current filesystem

**Returns:**
```json
{
  "snapshot1": "pool/dataset@snap1",
  "snapshot2": "pool/dataset@snap2",
  "changes": {
    "new": [{"path": "/file1", "type": "F"}],
    "modified": [{"path": "/file2", "type": "F"}],
    "deleted": [{"path": "/file3", "type": "F"}],
    "renamed": [{"old_path": "/old", "new_path": "/new", "type": "F"}]
  },
  "summary": {
    "new_count": 1,
    "modified_count": 1,
    "deleted_count": 1,
    "renamed_count": 1,
    "total_changes": 4
  }
}
```

**File Types:**
- `F` - Regular file
- `D` - Directory
- `L` - Symbolic link

---

### Clone Operations

#### clone_create
Create a clone from a snapshot.

**Parameters:**
- `source` (string, required): Source snapshot (format: dataset@snapshot)
- `target` (string, required): Target dataset name
- `properties` (object, optional): Properties to set on the clone

---

### Hold Operations

#### snapshot_hold
Place a hold on a snapshot to prevent deletion.

**Parameters:**
- `dataset` (string, required): Dataset name
- `snapshot` (string, required): Snapshot name
- `tag` (string, required): Hold tag name
- `recursive` (boolean, optional): Apply hold recursively

---

#### snapshot_release
Release a hold on a snapshot.

**Parameters:**
- `dataset` (string, required): Dataset name
- `snapshot` (string, required): Snapshot name
- `tag` (string, required): Hold tag name
- `recursive` (boolean, optional): Release hold recursively

---

#### snapshot_holds_list
List all holds on a snapshot.

**Parameters:**
- `dataset` (string, required): Dataset name
- `snapshot` (string, required): Snapshot name
- `recursive` (boolean, optional): List holds recursively

---

### Send/Receive Operations

#### send_estimate
Estimate the size of a ZFS send operation.

**Parameters:**
- `dataset` (string, required): Dataset name
- `snapshot` (string, required): Snapshot name
- `from_snapshot` (string, optional): Base snapshot for incremental send
- `recursive` (boolean, optional): Include child datasets (default: true)
- `raw` (boolean, optional): Use raw send for encrypted datasets (default: auto-detect)
- `native_compressed` (boolean, optional): Send compressed blocks (default: auto-detect)
- `resumable` (boolean, optional): Make send resumable (default: true)

**Returns:**
```json
{
  "dataset": "pool/dataset",
  "snapshot": "snap1",
  "estimated_size": 1073741824,
  "human_size": "1.0G",
  "type": "full",
  "flags": {
    "raw": false,
    "compressed": true
  }
}
```

---

#### send_to_file
Send a ZFS snapshot to a file.

**Parameters:**
- `dataset` (string, required): Dataset name
- `snapshot` (string, required): Snapshot name
- `output_file` (string, required): Output file path
- `from_snapshot` (string, optional): Base snapshot for incremental
- `raw` (boolean, optional): Use raw send (default: auto-detect)
- `compressed` (boolean, optional): Send compressed blocks (default: auto-detect)
- `resumable` (boolean, optional): Make send resumable (default: true)

---

#### receive_from_file
Receive a ZFS snapshot from a file.

**Parameters:**
- `input_file` (string, required): Input file path
- `target_dataset` (string, required): Target dataset name
- `force` (boolean, optional): Force receive (default: true)

---

#### dataset_get_space
Get detailed space usage information for a dataset.

**Parameters:**
- `dataset` (string, required): Dataset name

**Returns:**
```json
{
  "dataset": "pool/dataset",
  "space": {
    "name": "pool/dataset",
    "avail": 189979131904,
    "used": 8595955712,
    "usedsnap": 114688,
    "useddss": 8595841024,
    "usedrefreserv": 0,
    "usedchild": 0
  }
}
```

**Space Fields:**
- `avail`: Available space in bytes
- `used`: Total used space in bytes
- `usedsnap`: Space used by snapshots in bytes
- `useddss`: Space used by dataset itself in bytes
- `usedrefreserv`: Space used by refreservation in bytes
- `usedchild`: Space used by child datasets in bytes

---

### Pool Operations

#### pool_get_properties
Get properties of a ZFS pool, including space information.

**Parameters:**
- `pool` (string, required): Pool name
- `property` (string, optional): Specific property to get (default: "all")

**Common Space Properties:**
- `size`: Total pool size in bytes
- `free`: Free space available in bytes
- `allocated`: Space allocated/used in bytes
- `capacity`: Percentage of pool used
- `fragmentation`: Pool fragmentation percentage
- `health`: Pool health status

**Example:**
```json
{
  "jsonrpc": "2.0",
  "method": "pool_get_properties",
  "params": {
    "pool": "syspool",
    "property": "size"
  },
  "id": 1
}
```

---

### Migration Operations

The API provides background task management for long-running migration operations between hosts using Redis-based task queues.

#### migration_create
Create a new migration task to transfer datasets between hosts.

**Parameters:**
- `source` (string, required): Source dataset with snapshot (e.g., "pool/dataset@snapshot")
- `destination` (string, required): Destination dataset path
- `remote` (string, optional): Remote host for SSH-based migration (e.g., "192.168.1.100")
- `limit` (integer, optional): Bandwidth limit in MB/s
- `compression` (string, optional): Compression type ("gzip", "lz4", "zstd", etc.)
- `recursive` (boolean, optional): Include child datasets (default: false)
- `sync` (boolean, optional): Keep snapshots with holds for replication (default: true)

**Returns:**
```json
{
  "task_id": "uuid-string",
  "status": "pending",
  "created_at": "2025-10-04T14:30:00Z"
}
```

**Example:**
```json
{
  "jsonrpc": "2.0",
  "method": "migration_create",
  "params": {
    "source": "syspool/data@snapshot1",
    "destination": "backup/syspool/data",
    "remote": "192.168.10.152",
    "limit": 10,
    "compression": "lz4",
    "sync": true
  },
  "id": 1
}
```

---

#### migration_list
List all migration tasks with their current status.

**Parameters:**
- `status` (string, optional): Filter by status ("pending", "running", "completed", "failed", "cancelled")
- `limit` (integer, optional): Maximum number of tasks to return (default: 100)

**Returns:**
```json
{
  "tasks": [
    {
      "id": "uuid-string",
      "status": "completed",
      "created_at": "2025-10-04T14:25:32Z",
      "started_at": "2025-10-04T14:25:34Z",
      "completed_at": "2025-10-04T14:25:48Z",
      "error": null,
      "progress": {
        "timestamp": "2025-10-04T14:25:47Z",
        "task_id": "uuid-string",
        "status": "progress",
        "bytes_transferred": 8611409428,
        "bytes_total": 8600672010,
        "percentage": 100.1,
        "rate_mbps": 686.0,
        "eta_seconds": 0,
        "elapsed_seconds": 12
      },
      "params": {
        "source": "syspool/benchmark_test@snap_5120mb",
        "destination": "syspool/test_async",
        "recursive": false,
        "limit": 10
      }
    }
  ],
  "total": 32
}
```

---

#### migration_get
Get detailed status of a specific migration task.

**Parameters:**
- `task_id` (string, required): Migration task UUID

**Returns:**
```json
{
  "id": "uuid-string",
  "status": "running",
  "created_at": "2025-10-04T14:25:32Z",
  "started_at": "2025-10-04T14:25:34Z",
  "completed_at": null,
  "error": null,
  "progress": {
    "timestamp": "2025-10-04T14:25:47Z",
    "task_id": "uuid-string",
    "status": "progress",
    "bytes_transferred": 2147483648,
    "bytes_total": 8600672010,
    "percentage": 25.0,
    "rate_mbps": 92.1,
    "eta_seconds": 180,
    "elapsed_seconds": 30
  },
  "params": {
    "source": "syspool/data@snapshot1",
    "destination": "backup/data",
    "remote": "192.168.10.152",
    "limit": 10,
    "compression": "lz4"
  }
}
```

---

#### migration_cancel
Cancel a running migration task.

**Parameters:**
- `task_id` (string, required): Migration task UUID

**Returns:**
```json
{
  "cancelled": true,
  "task_id": "uuid-string"
}
```

---

#### migration_progress
Get real-time progress of a specific migration task.

**Parameters:**
- `task_id` (string, required): Migration task UUID

**Returns:**
```json
{
  "timestamp": "2025-10-04T14:25:47Z",
  "task_id": "uuid-string",
  "status": "progress",
  "bytes_transferred": 2147483648,
  "bytes_total": 8600672010,
  "percentage": 25.0,
  "rate_mbps": 92.1,
  "eta_seconds": 180,
  "elapsed_seconds": 30
}
```

---

### Background Task System

The migration system uses a Redis-based background task queue for managing long-running operations.

**Task States:**
- `pending`: Task created but not yet started
- `running`: Task currently executing  
- `completed`: Task finished successfully
- `failed`: Task failed with error
- `cancelled`: Task was cancelled by user

**Progress Tracking:**
- Real-time progress updates with bytes transferred
- Transfer speed calculation (rate_mbps)
- Estimated time to completion (eta_seconds)
- Elapsed time tracking

**Sync Functionality:**
When `sync: true` (default), the migration system:
- Creates ZFS holds on snapshots to prevent deletion
- Maintains sync points between source and destination hosts
- Enables incremental replication by preserving reference points
- Uses hold tags with format: `sync_YYYY-MM-DD-HH-MM-SS_hostname`
- Automatically cleans up old sync points while keeping the latest
- Critical for production backup systems to maintain replication chains

---

### Bookmark Operations

#### bookmark_create
Create a bookmark from a snapshot.

**Parameters:**
- `snapshot` (string, required): Source snapshot (format: dataset@snapshot)
- `bookmark` (string, required): Bookmark name (format: dataset#bookmark)

---

## Token-Based Socket Authentication

For secure socket operations, the API provides token-based authentication. Tokens are created via the API and validated through Redis.

### Token Methods

#### token_create_send
Create a token for authenticated socket send operation.

**Parameters:**
- `dataset` (string, required): The dataset to send
- `snapshot` (string, required): The snapshot name
- `from_snapshot` (string, optional): Base snapshot for incremental send
- `raw` (boolean, optional): Send raw encrypted stream (null=autodetect)
- `compressed` (boolean, optional): Send compressed blocks (null=autodetect)
- `resumable` (boolean, optional): Create resumable send stream (default: true)
- `recursive` (boolean, optional): Send recursively (default: false)
- `ttl` (integer, optional): Token time-to-live in seconds (default: 300)

**Returns:**
- `token`: Authentication token to use with socket
- `expires_in`: Seconds until token expires
- `operation`: "send"
- `dataset`: Dataset name
- `snapshot`: Snapshot name
- `socket_tcp`: TCP socket address
- `socket_unix`: Unix socket path

**Example:**
```json
{
  "jsonrpc": "2.0",
  "method": "token_create_send",
  "params": {
    "dataset": "pool/dataset",
    "snapshot": "snap1",
    "raw": null,
    "compressed": null,
    "resumable": true
  },
  "id": 1
}
```

---

#### token_create_receive
Create a token for authenticated socket receive operation.

**Parameters:**
- `dataset` (string, required): The dataset to receive into
- `force` (boolean, optional): Force receive, overwriting existing (default: true)
- `resumable` (boolean, optional): Support resumable receives (default: false)
- `ttl` (integer, optional): Token time-to-live in seconds (default: 300)

**Returns:** Same format as token_create_send but with operation="receive"

---

#### token_list
List all active tokens for the current user.

**Parameters:** None

**Returns:**
- `tokens`: Array of token objects with:
  - `token_id`: First 8 characters of token
  - `operation`: "send" or "receive"
  - `dataset`: Dataset name
  - `created_at`: Creation timestamp
  - `expires_at`: Expiration timestamp
  - `used`: Whether token has been used
  - `use_count`: Number of times used

---

#### token_revoke
Revoke a token immediately.

**Parameters:**
- `token_id` (string, required): Full token ID to revoke

**Returns:**
- `revoked`: Boolean indicating success

---

#### token_stats
Get token system statistics.

**Parameters:** None

**Returns:**
- `created`: Token creation counts by operation
- `used`: Token usage counts by operation
- `validation`: Validation statistics
- `revoked`: Revocation statistics
- `active_tokens`: Number of active tokens
- `redis_connected`: Redis connection status
- `redis_stats`: Redis connection statistics

---

### Using Tokens with Socket Servers

When connecting to a socket server with token authentication:

1. Create a token using `token_create_send` or `token_create_receive`
2. Connect to the socket server (TCP or Unix)
3. Send token as the first message:
   - 4 bytes: Token length (big-endian)
   - N bytes: Token string
4. Receive response:
   - 4 bytes: Response length (big-endian)
   - N bytes: JSON response
5. If successful, proceed with normal socket protocol

**Example Client:**
```python
import socket
import struct
import json

# Get token from API
token = "your-token-here"

# Connect to socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('localhost', 9999))

# Send token
token_bytes = token.encode('utf-8')
sock.sendall(struct.pack('!I', len(token_bytes)))
sock.sendall(token_bytes)

# Read response
resp_len = struct.unpack('!I', sock.recv(4))[0]
response = json.loads(sock.recv(resp_len))

if response.get('status') == 'started':
    # Token valid, proceed with operation
    # For send: read chunks
    # For receive: send data
    pass
```

---

## Socket Servers

For high-performance streaming operations, dedicated socket servers are available:

### Unix Domain Socket
- **Path:** `./zfs_unix_socket`
- **Best for:** Local operations, highest performance

### TCP Socket
- **Port:** 9999
- **Best for:** Network operations, remote access

### Socket Protocol

1. **Connection:** Connect to socket
2. **Header:** Send 8-byte header
   - 4 bytes: Operation type (1=send, 2=receive)
   - 4 bytes: Metadata size
3. **Metadata:** Send JSON metadata
4. **Data Transfer:** Stream data directly

### Operation Types
- `OP_SEND` (1): Send ZFS snapshot
- `OP_RECEIVE` (2): Receive ZFS snapshot

### Send Operation Metadata
```json
{
  "dataset": "pool/dataset",
  "snapshot": "snap1",
  "from_snapshot": "snap0",  // optional
  "compressed": false,       // optional, auto-detected if not specified
  "raw": false,             // optional, auto-detected if not specified
  "recursive": false
}
```

### Receive Operation Metadata
```json
{
  "dataset": "pool/target",
  "force": true
}
```

## Performance

- **HTTP API:** Good for management operations
- **Unix Socket:** ~600 MB/s send, ~330 MB/s receive
- **TCP Socket:** ~605 MB/s send, ~310 MB/s receive

## Auto-detection Features

The API automatically detects optimal flags for send operations:

1. **Raw Send (-w):** Auto-enabled for encrypted datasets
2. **Compressed Send (-c):** Auto-enabled for datasets with compression
3. **Resumable Send (-s):** Enabled by default

Manual overrides are available for all auto-detected flags.

## Error Codes

- `-32001`: Invalid parameters
- `-32002`: Dataset not found
- `-32003`: Snapshot not found
- `-32004`: Permission denied
- `-32005`: Operation failed
- `-32010`: Send operation failed
- `-32020`: Receive operation failed
- `-32021`: Snapshot creation failed
- `-32030`: Hold operation failed
- `-32035`: Diff operation failed

## Example Client Code

### Python Example
```python
import aiohttp
import asyncio

async def create_snapshot():
    async with aiohttp.ClientSession() as session:
        payload = {
            "jsonrpc": "2.0",
            "method": "snapshot_create",
            "params": {
                "dataset": "pool/dataset",
                "name": "backup_2024"
            },
            "id": 1
        }
        
        async with session.post("http://localhost:8545", json=payload) as resp:
            result = await resp.json()
            print(result)

asyncio.run(create_snapshot())
```

### Socket Client Example
```python
import socket
import struct
import json

# Connect to Unix socket
sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.connect('./zfs_unix_socket')

# Send operation
metadata = {
    "dataset": "pool/dataset",
    "snapshot": "snap1"
}

# Send header
op_type = 1  # OP_SEND
metadata_bytes = json.dumps(metadata).encode('utf-8')
header = struct.pack("!II", op_type, len(metadata_bytes))
sock.sendall(header)

# Send metadata
sock.sendall(metadata_bytes)

# Read response
response_data = sock.recv(8192)
response = json.loads(response_data.decode('utf-8'))
print(response)
```

## Testing

Test scripts are available in the repository:
- `test_api.py` - Basic API functionality
- `test_autodetect.py` - Auto-detection features
- `test_diff.py` - Snapshot diff functionality
- `benchmark_new_servers.py` - Performance testing

## Security Considerations

1. **Authentication:** Optional bearer token support
2. **Path Validation:** All paths are validated to prevent traversal
3. **Command Injection:** Parameterized commands prevent injection
4. **Rate Limiting:** Configure reverse proxy for rate limiting
5. **TLS:** Use reverse proxy for HTTPS support

## Monitoring

The API exposes Prometheus metrics at `/metrics`:
- Request counts by method and status
- Request duration histograms
- Active connection gauge

## Configuration

Environment variables:
- `ZFS_API_HOST`: Listen host (default: 0.0.0.0)
- `ZFS_API_PORT`: Listen port (default: 8545)
- `ZFS_API_TOKEN`: Bearer token for authentication
- `ZFS_API_LOG_LEVEL`: Logging level (default: INFO)