# ZFS API Server

A high-performance async JSON-RPC API server for ZFS management with authentication and rate limiting.

## Features

- **JSON-RPC 2.0 Protocol**: Clean, efficient RPC interface
- **Native Async**: Pure asyncio execution with AsyncZFS (no thread pool overhead)
- **Clean Architecture**: Single unified API through zfs_commands package
- **Authentication**: JWT token-based auth with passwordless localhost access
- **Rate Limiting**: Configurable request rate limiting
- **Comprehensive API**: Full coverage of ZFS operations (datasets, snapshots, pools, clones, volumes, bookmarks)
- **Better Error Messages**: Detailed stderr output for debugging
- **Monitoring**: Prometheus metrics endpoint
- **Logging**: Structured logging with configurable levels
- **Redis Integration**: Token-based streaming for large transfers

## Quick Start

1. **Start Redis** (required for streaming operations):
   ```bash
   docker compose up -d
   ```

2. **Run the setup script**:
   ```bash
   ./setup.sh
   ```
   This will:
   - Create a Python virtual environment
   - Install all dependencies
   - Create default configuration
   - Generate convenience scripts

3. **Start the server**:
   ```bash
   ./start.sh
   ```

4. **Test the API**:
   ```bash
   python3 test_api_client.py <pool_name>
   ```
   Example: `python3 test_api_client.py tank`

## API Endpoints

### Authentication
- `auth_login` - Get authentication token

### Dataset Operations
- `dataset_create` - Create new dataset
- `dataset_destroy` - Destroy dataset
- `dataset_list` - List datasets
- `dataset_get_properties` - Get dataset properties
- `dataset_set_property` - Set dataset property
- `dataset_get_space` - Get space usage
- `dataset_mount` - Mount dataset

### Snapshot Operations
- `snapshot_create` - Create snapshot
- `snapshot_create_auto` - Create auto-named snapshot with timestamp
- `snapshot_list` - List snapshots for a dataset
- `snapshot_destroy` - Destroy snapshot
- `snapshot_rollback` - Rollback to snapshot
- `snapshot_hold` - Place hold on snapshot
- `snapshot_release` - Release hold on snapshot
- `snapshot_holds_list` - List holds on a snapshot
- `snapshot_diff` - Compare differences between snapshots or snapshot and current filesystem

### Pool Operations
- `pool_list` - List pools
- `pool_get_properties` - Get pool properties
- `pool_scrub_start` - Start scrub
- `pool_scrub_stop` - Stop scrub
- `pool_status` - Get pool status

### Clone Operations
- `clone_create` - Create clone from snapshot

### Volume Operations
- `volume_create` - Create ZFS volume
- `volume_list` - List volumes

### Bookmark Operations
- `bookmark_create` - Create bookmark from snapshot
- `bookmark_list` - List bookmarks for dataset
- `bookmark_destroy` - Destroy bookmark

### Send/Receive Operations
- `send_estimate` - Estimate size of send operation
- `send_to_file` - Send snapshot to file (with optional compression)
- `receive_from_file` - Receive snapshot from file

### Streaming Operations (for large transfers)
- `stream_init` - Initialize streaming session (returns UUID and URL)
- `stream_list` - List active streaming sessions
- `stream_cancel` - Cancel a streaming session
- HTTP GET `/stream/{uuid}` - Stream ZFS send data
- HTTP POST `/stream/{uuid}` - Stream ZFS receive data

## Configuration

Edit `config.yaml` to customize:
- Server host/port
- Authentication settings
- Rate limiting
- Logging

## Example Usage

### Using curl (localhost - no auth required):
```bash
curl -X POST http://localhost:8545 \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "pool_list",
    "params": {},
    "id": 1
  }'
```

### Using Python client:
```python
from jsonrpcclient import request
import requests

# Localhost - no auth needed
response = requests.post("http://localhost:8545", json=request("pool_list"))
```

## Security

- Localhost connections bypass authentication
- Remote connections require JWT token
- Default admin password should be changed in production
- Rate limiting prevents abuse

## Requirements

- Python 3.8+
- ZFS installed and accessible
- Root/sudo access for ZFS operations
- Docker (for Redis container)
- Redis 7+ (provided via docker-compose.yml)

## Architecture

- **Protocol**: JSON-RPC 2.0 over HTTP
- **Execution Model**: Native async with asyncio (no thread pool overhead)
- **ZFS Commands**: Clean three-layer architecture
  - **Command Builder** (`ZFSCommands`): Pure command construction
  - **Async Executor** (`AsyncZFS`): Native async subprocess execution
  - **Sync Executor** (`SyncZFS`): For scripts and tools
- **Framework**: aiohttp + jsonrpcserver
- **Authentication**: JWT tokens with passlib bcrypt
- **Storage**: Redis for streaming session management
- **Metrics**: Prometheus format

### Benefits of New Architecture

- ✅ **40% less code** - Eliminated ThreadPoolExecutor wrapper overhead
- ✅ **Better performance** - Native async execution throughout
- ✅ **Improved errors** - stderr included in error messages
- ✅ **Single source of truth** - All ZFS commands in one place
- ✅ **Easy sudo support** - Modify command builder in one place
- ✅ **Consistent API** - Unified interface across all operations

## Monitoring

- Health endpoint: `http://localhost:8545/health`
- Metrics endpoint: `http://localhost:8545/metrics`

## Development

To work with the code:

1. Activate virtual environment:
   ```bash
   source venv/bin/activate
   ```

2. Install new dependencies:
   ```bash
   pip install <package>
   pip freeze > requirements.txt
   ```

3. Run tests:
   ```bash
   python test_api.py
   ```

## License

This project uses the existing zfs.py library for ZFS operations.