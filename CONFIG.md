# ZFS API Configuration Guide

The ZFS API server uses a YAML configuration file (`config.yaml`) to control various aspects of its operation. Below are all the available configuration options with their defaults.

## Configuration File Structure

```yaml
# ZFS API Server Configuration

server:
  # HTTP API Configuration
  http:
    host: 0.0.0.0      # Interface to bind HTTP server to
    port: 8545         # Port for HTTP API

  # TCP Socket Configuration  
  tcp:
    host: 0.0.0.0      # Interface to bind TCP socket server to
    port: 9999         # Port for token-authenticated TCP socket
    enabled: true      # Enable/disable TCP socket server

  # Unix Socket Configuration
  unix:
    path: ./zfs_token_socket  # Path to Unix domain socket file
    enabled: true             # Enable/disable Unix socket server

  # Worker configuration
  workers: 20          # Number of thread pool workers for ZFS operations

# Background Task Manager Configuration
task_manager:
  # Number of concurrent migration workers
  migration_workers: 4  # How many migrations can run simultaneously

auth:
  enabled: true        # Enable/disable authentication
  # Default users (password: admin)
  users:
    admin: "$2b$12$/Cgi8ftHgz1adWuhpy7SGOfnKc2HJqY5WtKYrmoqeaN3eKDq055rO"

rate_limit:
  enabled: true        # Enable/disable rate limiting
  requests: 100        # Number of requests
  window: 60          # Time window in seconds

logging:
  level: INFO         # Log level (DEBUG, INFO, WARNING, ERROR)
  file: zfs_api.log   # Log file path

redis:
  host: 192.168.18.25  # Redis server hostname/IP
  port: 6379           # Redis server port
  db: 0               # Redis database number
  password: null      # Redis password (if required)
```

## Configuration Options Explained

### Server Section

#### HTTP API Configuration
- `server.http.host`: The network interface to bind the HTTP API to. Use `0.0.0.0` to listen on all interfaces.
- `server.http.port`: The port number for the HTTP JSON-RPC API.

#### TCP Socket Configuration
- `server.tcp.host`: The network interface for the token-authenticated TCP socket server.
- `server.tcp.port`: The port number for TCP socket connections.
- `server.tcp.enabled`: Set to `false` to disable the TCP socket server.

#### Unix Socket Configuration
- `server.unix.path`: File path for the Unix domain socket. Can be relative or absolute.
- `server.unix.enabled`: Set to `false` to disable the Unix socket server.

#### Worker Configuration
- `server.workers`: Number of threads in the thread pool for handling ZFS operations. Increase for better concurrency.

### Task Manager Section

- `task_manager.migration_workers`: Maximum number of concurrent ZFS migration operations. Each migration uses one worker.

### Authentication Section

- `auth.enabled`: Enable/disable authentication for the HTTP API.
- `auth.users`: Dictionary of username -> bcrypt hashed password pairs.

To generate a new password hash:
```python
from passlib.context import CryptContext
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
hash = pwd_context.hash("your_password")
print(hash)
```

### Rate Limiting Section

- `rate_limit.enabled`: Enable/disable rate limiting.
- `rate_limit.requests`: Maximum number of requests allowed.
- `rate_limit.window`: Time window in seconds for the rate limit.

### Logging Section

- `logging.level`: Set to `DEBUG`, `INFO`, `WARNING`, or `ERROR`.
- `logging.file`: Path to the log file.

### Redis Section

- `redis.host`: Redis server hostname or IP address.
- `redis.port`: Redis server port.
- `redis.db`: Redis database number (0-15).
- `redis.password`: Redis authentication password if required.

## Environment Variables

The following environment variables can also be used:

- `ZFS_API_SECRET_KEY`: JWT secret key for authentication (overrides default)
- `PYTHONUNBUFFERED`: Set to `1` for unbuffered output (automatically set)

## Default Configuration

If no `config.yaml` file is present, the server will use built-in defaults that match the example configuration above.

## Applying Configuration Changes

Most configuration changes require a server restart to take effect:

```bash
sudo systemctl restart zfs-api
```

## Example Configurations

### High-Performance Setup
```yaml
server:
  http:
    host: 0.0.0.0
    port: 8545
  workers: 50

task_manager:
  migration_workers: 10

redis:
  host: localhost
  port: 6379
```

### Security-Focused Setup
```yaml
server:
  http:
    host: 127.0.0.1  # Only localhost
    port: 8545
  tcp:
    enabled: false   # Disable TCP socket
  unix:
    enabled: true    # Unix socket for local access only

auth:
  enabled: true

rate_limit:
  enabled: true
  requests: 50
  window: 60
```

### Development Setup
```yaml
server:
  http:
    host: 0.0.0.0
    port: 8545

logging:
  level: DEBUG
  file: debug.log

auth:
  enabled: false

rate_limit:
  enabled: false
```