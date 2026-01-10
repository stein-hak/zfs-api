#!/usr/bin/env python3
"""
ZFS JSON-RPC API Server
Multi-threaded server for ZFS operations with authentication and rate limiting
"""

import asyncio
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple

import yaml
from aiohttp import web
from jsonrpcserver import Success, Error, method, Result, async_dispatch
from passlib.context import CryptContext
from jose import JWTError, jwt
from prometheus_client import Counter, Histogram, generate_latest

from zfs_commands import AsyncZFS
# Legacy socket servers - replaced by token-authenticated versions
# from zfs_tcp_socketserver import ZFSTCPSocketServer
# from zfs_socketserver import run_socketserver
# from zfs_unix_socketserver import ZFSUnixSocketServer
# Import async versions
from async_redis_reliable import AsyncReliableRedis
from async_redis_token_manager import AsyncTokenManager
from async_background_task_manager import AsyncBackgroundTaskManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('zfs_api')
logging.getLogger('background_task_manager').setLevel(logging.INFO)

# Metrics
api_requests = Counter('zfs_api_requests_total', 'Total API requests', ['method', 'status'])
api_duration = Histogram('zfs_api_request_duration_seconds', 'API request duration', ['method'])
active_operations = Counter('zfs_api_active_operations', 'Currently active operations')

# Global instances
zfs = AsyncZFS()  # Unified async ZFS executor
unix_socket_server = None  # Will be initialized during startup
tcp_socket_server = None  # Will be initialized during startup
token_manager = None  # Will be initialized during startup
redis_client = None  # Will be initialized during startup
task_manager = None  # Will be initialized during startup

# Authentication
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
SECRET_KEY = os.environ.get("ZFS_API_SECRET_KEY", "your-secret-key-change-this")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Rate limiting
rate_limit_store = {}
RATE_LIMIT_REQUESTS = 100
RATE_LIMIT_WINDOW = 60  # seconds

class Config:
    """Configuration handler"""
    def __init__(self, config_file="config.yaml"):
        self.config_file = config_file
        self.config = self.load_config()
    
    def load_config(self):
        """Load configuration from YAML file"""
        if os.path.exists(self.config_file):
            with open(self.config_file, 'r') as f:
                return yaml.safe_load(f)
        else:
            # Default configuration
            return {
                "server": {
                    "http": {
                        "host": "0.0.0.0",
                        "port": 8545
                    },
                    "tcp": {
                        "host": "0.0.0.0", 
                        "port": 9999,
                        "enabled": True
                    },
                    "unix": {
                        "path": "./zfs_token_socket",
                        "enabled": True
                    },
                    "workers": 20
                },
                "task_manager": {
                    "migration_workers": 4
                },
                "auth": {
                    "enabled": True,
                    "users": {
                        "admin": "$2b$12$xFSRKpLYk0e.YXdMsVSWiOFjBqYn7ktPqJB3FpDmcVvUeCqQRxXKu"  # password: admin
                    }
                },
                "rate_limit": {
                    "enabled": True,
                    "requests": 100,
                    "window": 60
                },
                "logging": {
                    "level": "INFO",
                    "file": "zfs_api.log"
                },
                "redis": {
                    "host": "localhost",
                    "port": 6379,
                    "db": 0,
                    "password": None
                }
            }
    
    def save_config(self):
        """Save configuration to YAML file"""
        with open(self.config_file, 'w') as f:
            yaml.dump(self.config, f, default_flow_style=False)

config = Config()

def create_token(username: str) -> str:
    """Create JWT token"""
    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode = {"sub": username, "exp": expire}
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def verify_token(token: str) -> Optional[str]:
    """Verify JWT token and return username"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        return username
    except JWTError:
        return None

def check_rate_limit(client_id: str) -> bool:
    """Check if client has exceeded rate limit"""
    if not config.config["rate_limit"]["enabled"]:
        return True
    
    now = time.time()
    window = config.config["rate_limit"]["window"]
    limit = config.config["rate_limit"]["requests"]
    
    # Clean old entries
    cutoff = now - window
    if client_id in rate_limit_store:
        rate_limit_store[client_id] = [t for t in rate_limit_store[client_id] if t > cutoff]
    
    # Check limit
    if client_id not in rate_limit_store:
        rate_limit_store[client_id] = []
    
    if len(rate_limit_store[client_id]) >= limit:
        return False
    
    rate_limit_store[client_id].append(now)
    return True

def require_auth(f):
    """Decorator to require authentication"""
    @wraps(f)
    async def wrapper(context, *args, **kwargs):
        # Allow passwordless access from localhost
        request = context.get("request")
        if request:
            client_ip = request.remote
            if client_ip in ["127.0.0.1", "::1", "localhost"]:
                context["username"] = "localhost"
                return await f(context, *args, **kwargs)
        
        if not config.config["auth"]["enabled"]:
            return await f(context, *args, **kwargs)
        
        token = context.get("token")
        if not token:
            return Error(-32001, "Authentication required")
        
        username = verify_token(token)
        if not username:
            return Error(-32002, "Invalid token")
        
        context["username"] = username
        return await f(context, *args, **kwargs)
    
    return wrapper

# Authentication methods
@method
async def auth_login(context: Dict[str, Any], username: str, password: str) -> Result:
    """Authenticate user and return token"""
    users = config.config["auth"]["users"]
    
    if username not in users:
        return Error(-32003, "Invalid credentials")
    
    if not pwd_context.verify(password, users[username]):
        return Error(-32003, "Invalid credentials")
    
    token = create_token(username)
    return Success({"token": token, "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60})

# Dataset operations
@method
@require_auth
async def dataset_create(context: Dict[str, Any], dataset: str, properties: Optional[Dict[str, str]] = None) -> Result:
    """Create a new ZFS dataset"""
    try:
        api_requests.labels(method="dataset_create", status="started").inc()
        with api_duration.labels(method="dataset_create").time():
            result = await zfs.dataset_create(dataset, properties)

        if result.success:
            api_requests.labels(method="dataset_create", status="success").inc()
            return Success({"dataset": dataset, "created": True})
        else:
            api_requests.labels(method="dataset_create", status="failed").inc()
            return Error(-32010, f"Failed to create dataset: {result.stderr}")
    except Exception as e:
        api_requests.labels(method="dataset_create", status="error").inc()
        logger.exception(f"Error creating dataset {dataset}")
        return Error(-32011, str(e))

@method
@require_auth
async def dataset_destroy(context: Dict[str, Any], dataset: str, recursive: bool = False) -> Result:
    """Destroy a ZFS dataset"""
    try:
        api_requests.labels(method="dataset_destroy", status="started").inc()
        with api_duration.labels(method="dataset_destroy").time():
            result = await zfs.dataset_destroy(dataset, recursive)

        if result.success:
            api_requests.labels(method="dataset_destroy", status="success").inc()
            return Success({"dataset": dataset, "destroyed": True})
        else:
            api_requests.labels(method="dataset_destroy", status="failed").inc()
            return Error(-32012, f"Failed to destroy dataset: {result.stderr}")
    except Exception as e:
        api_requests.labels(method="dataset_destroy", status="error").inc()
        logger.exception(f"Error destroying dataset {dataset}")
        return Error(-32013, str(e))

@method
@require_auth
async def dataset_list(context: Dict[str, Any], dataset: Optional[str] = None) -> Result:
    """List ZFS datasets"""
    try:
        api_requests.labels(method="dataset_list", status="started").inc()
        with api_duration.labels(method="dataset_list").time():
            datasets = await zfs.dataset_list(dataset)

        api_requests.labels(method="dataset_list", status="success").inc()
        return Success({"datasets": datasets})
    except Exception as e:
        api_requests.labels(method="dataset_list", status="error").inc()
        logger.exception("Error listing datasets")
        return Error(-32014, str(e))

@method
@require_auth
async def dataset_get_properties(context: Dict[str, Any], dataset: str, property: str = "all") -> Result:
    """Get ZFS dataset properties"""
    try:
        api_requests.labels(method="dataset_get_properties", status="started").inc()
        with api_duration.labels(method="dataset_get_properties").time():
            props = await zfs.dataset_get_properties(dataset, property)

        api_requests.labels(method="dataset_get_properties", status="success").inc()
        return Success({"dataset": dataset, "properties": props})
    except Exception as e:
        api_requests.labels(method="dataset_get_properties", status="error").inc()
        logger.exception(f"Error getting properties for dataset {dataset}")
        return Error(-32015, str(e))

@method
@require_auth
async def dataset_set_property(context: Dict[str, Any], dataset: str, property: str, value: str) -> Result:
    """Set ZFS dataset property"""
    try:
        api_requests.labels(method="dataset_set_property", status="started").inc()
        with api_duration.labels(method="dataset_set_property").time():
            result = await zfs.dataset_set_property(dataset, property, value)

        if result.success:
            api_requests.labels(method="dataset_set_property", status="success").inc()
            return Success({"dataset": dataset, "property": property, "value": value})
        else:
            api_requests.labels(method="dataset_set_property", status="failed").inc()
            return Error(-32016, f"Failed to set property: {result.stderr}")
    except Exception as e:
        api_requests.labels(method="dataset_set_property", status="error").inc()
        logger.exception(f"Error setting property {property} on dataset {dataset}")
        return Error(-32017, str(e))

@method
@require_auth
async def dataset_get_space(context: Dict[str, Any], dataset: str) -> Result:
    """Get space usage for dataset"""
    try:
        api_requests.labels(method="dataset_get_space", status="started").inc()
        with api_duration.labels(method="dataset_get_space").time():
            space = await zfs.dataset_get_space(dataset)

        api_requests.labels(method="dataset_get_space", status="success").inc()
        return Success({"dataset": dataset, "space": space})
    except Exception as e:
        api_requests.labels(method="dataset_get_space", status="error").inc()
        logger.exception(f"Error getting space for dataset {dataset}")
        return Error(-32018, str(e))

@method
@require_auth
async def dataset_mount(context: Dict[str, Any], dataset: str) -> Result:
    """Mount a ZFS dataset"""
    try:
        api_requests.labels(method="dataset_mount", status="started").inc()
        result = await zfs.dataset_mount(dataset)

        if result.success:
            api_requests.labels(method="dataset_mount", status="success").inc()
            return Success({"dataset": dataset, "mounted": True})
        else:
            stderr_text = result.stderr.lower()
            # Check if error is because it's already mounted
            if 'already mounted' in stderr_text or 'filesystem already mounted' in stderr_text:
                api_requests.labels(method="dataset_mount", status="success").inc()
                return Success({"dataset": dataset, "mounted": True, "already_mounted": True})
            else:
                api_requests.labels(method="dataset_mount", status="failed").inc()
                return Error(-32019, f"Failed to mount dataset: {result.stderr.strip()}")
    except Exception as e:
        api_requests.labels(method="dataset_mount", status="error").inc()
        logger.exception(f"Error mounting dataset {dataset}")
        return Error(-32020, str(e))

# Snapshot operations
@method
@require_auth
async def snapshot_create(context: Dict[str, Any], dataset: str, name: str, recursive: bool = False) -> Result:
    """Create a snapshot"""
    try:
        api_requests.labels(method="snapshot_create", status="started").inc()
        with api_duration.labels(method="snapshot_create").time():
            result = await zfs.snapshot_create(dataset, name, recursive)

        if result.success:
            api_requests.labels(method="snapshot_create", status="success").inc()
            return Success({"dataset": dataset, "snapshot": name, "created": True})
        else:
            api_requests.labels(method="snapshot_create", status="failed").inc()
            return Error(-32021, f"Failed to create snapshot: {result.stderr}")
    except Exception as e:
        api_requests.labels(method="snapshot_create", status="error").inc()
        logger.exception(f"Error creating snapshot {name} for dataset {dataset}")
        return Error(-32022, str(e))

@method
@require_auth
async def snapshot_create_auto(context: Dict[str, Any], dataset: str, tag: str, tag1: Optional[str] = None,
                              recursive: bool = False) -> Result:
    """Create an auto-named snapshot"""
    try:
        api_requests.labels(method="snapshot_create_auto", status="started").inc()
        with api_duration.labels(method="snapshot_create_auto").time():
            result, name = await zfs.snapshot_create_auto(dataset, tag, tag1, recursive)

        if result.success:
            api_requests.labels(method="snapshot_create_auto", status="success").inc()
            return Success({"dataset": dataset, "snapshot": name, "created": True})
        else:
            api_requests.labels(method="snapshot_create_auto", status="failed").inc()
            return Error(-32023, f"Failed to create auto snapshot: {result.stderr}")
    except Exception as e:
        api_requests.labels(method="snapshot_create_auto", status="error").inc()
        logger.exception(f"Error creating auto snapshot for dataset {dataset}")
        return Error(-32024, str(e))

@method
@require_auth
async def snapshot_list(context: Dict[str, Any], dataset: str) -> Result:
    """List snapshots for a dataset"""
    try:
        api_requests.labels(method="snapshot_list", status="started").inc()
        with api_duration.labels(method="snapshot_list").time():
            snapshots = await zfs.snapshot_list(dataset)

        api_requests.labels(method="snapshot_list", status="success").inc()
        return Success({"dataset": dataset, "snapshots": snapshots})
    except Exception as e:
        api_requests.labels(method="snapshot_list", status="error").inc()
        logger.exception(f"Error listing snapshots for dataset {dataset}")
        return Error(-32025, str(e))

@method
@require_auth
async def snapshot_destroy(context: Dict[str, Any], dataset: str, snapshot: str, recursive: bool = False) -> Result:
    """Destroy a snapshot"""
    try:
        api_requests.labels(method="snapshot_destroy", status="started").inc()
        full_snapshot = f"{dataset}@{snapshot}"
        with api_duration.labels(method="snapshot_destroy").time():
            result = await zfs.snapshot_destroy(dataset, snapshot, recursive)

        if result.success:
            api_requests.labels(method="snapshot_destroy", status="success").inc()
            return Success({"snapshot": full_snapshot, "destroyed": True})
        else:
            api_requests.labels(method="snapshot_destroy", status="failed").inc()
            return Error(-32026, f"Failed to destroy snapshot: {result.stderr}")
    except Exception as e:
        api_requests.labels(method="snapshot_destroy", status="error").inc()
        logger.exception(f"Error destroying snapshot {snapshot} for dataset {dataset}")
        return Error(-32027, str(e))

@method
@require_auth
async def snapshot_rollback(context: Dict[str, Any], dataset: str, snapshot: str) -> Result:
    """Rollback to a snapshot"""
    try:
        api_requests.labels(method="snapshot_rollback", status="started").inc()
        with api_duration.labels(method="snapshot_rollback").time():
            result = await zfs.snapshot_rollback(dataset, snapshot)

        if result.success:
            api_requests.labels(method="snapshot_rollback", status="success").inc()
            return Success({"dataset": dataset, "snapshot": snapshot, "rolled_back": True})
        else:
            api_requests.labels(method="snapshot_rollback", status="failed").inc()
            return Error(-32028, f"Failed to rollback: {result.stderr}")
    except Exception as e:
        api_requests.labels(method="snapshot_rollback", status="error").inc()
        logger.exception(f"Error rolling back to snapshot {snapshot} for dataset {dataset}")
        return Error(-32029, str(e))

@method
@require_auth
async def snapshot_hold(context: Dict[str, Any], dataset: str, snapshot: str, tag: str,
                       recursive: bool = False) -> Result:
    """Place a hold on a snapshot"""
    try:
        api_requests.labels(method="snapshot_hold", status="started").inc()
        with api_duration.labels(method="snapshot_hold").time():
            result = await zfs.snapshot_hold(dataset, snapshot, tag, recursive)

        if result.success:
            api_requests.labels(method="snapshot_hold", status="success").inc()
            return Success({"dataset": dataset, "snapshot": snapshot, "hold": tag})
        else:
            api_requests.labels(method="snapshot_hold", status="failed").inc()
            return Error(-32030, f"Failed to hold snapshot: {result.stderr}")
    except Exception as e:
        api_requests.labels(method="snapshot_hold", status="error").inc()
        logger.exception(f"Error holding snapshot {snapshot} for dataset {dataset}")
        return Error(-32031, str(e))

@method
@require_auth
async def snapshot_release(context: Dict[str, Any], dataset: str, snapshot: str, tag: str,
                          recursive: bool = False) -> Result:
    """Release a hold on a snapshot"""
    try:
        api_requests.labels(method="snapshot_release", status="started").inc()
        with api_duration.labels(method="snapshot_release").time():
            result = await zfs.snapshot_release(dataset, snapshot, tag, recursive)

        if result.success:
            api_requests.labels(method="snapshot_release", status="success").inc()
            return Success({"dataset": dataset, "snapshot": snapshot, "released": tag})
        else:
            api_requests.labels(method="snapshot_release", status="failed").inc()
            return Error(-32032, f"Failed to release snapshot: {result.stderr}")
    except Exception as e:
        api_requests.labels(method="snapshot_release", status="error").inc()
        logger.exception(f"Error releasing snapshot {snapshot} for dataset {dataset}")
        return Error(-32033, str(e))

@method
@require_auth
async def snapshot_holds_list(context: Dict[str, Any], dataset: str, snapshot: str,
                             recursive: bool = False) -> Result:
    """List holds on a snapshot"""
    try:
        api_requests.labels(method="snapshot_holds_list", status="started").inc()
        with api_duration.labels(method="snapshot_holds_list").time():
            holds = await zfs.snapshot_list_holds(dataset, snapshot, recursive)

        api_requests.labels(method="snapshot_holds_list", status="success").inc()
        return Success({"dataset": dataset, "snapshot": snapshot, "holds": holds})
    except Exception as e:
        api_requests.labels(method="snapshot_holds_list", status="error").inc()
        logger.exception(f"Error listing holds for snapshot {snapshot}")
        return Error(-32034, str(e))

@method
@require_auth
async def snapshot_diff(context: Dict[str, Any], snapshot1: str, snapshot2: Optional[str] = None) -> Result:
    """Compare differences between snapshots or between a snapshot and current filesystem

    Args:
        snapshot1: First snapshot (format: dataset@snapshot)
        snapshot2: Second snapshot (optional, format: dataset@snapshot)
                  If not provided, compares snapshot1 with current filesystem

    Returns:
        Dictionary with:
        - new: List of new files/directories [(path, type), ...]
        - modified: List of modified files [(path, type), ...]
        - deleted: List of deleted files/directories [(path, type), ...]
        - renamed: List of renamed files [(path, new_path, type), ...]
    """
    try:
        api_requests.labels(method="snapshot_diff", status="started").inc()

        with api_duration.labels(method="snapshot_diff").time():
            # Call the diff method from zfs module
            new, modified, deleted, renamed = await zfs.snapshot_diff(snapshot1, snapshot2)

            # Format the response
            result = {
                "snapshot1": snapshot1,
                "snapshot2": snapshot2 if snapshot2 else "current",
                "changes": {
                    "new": [{"path": path, "type": ftype} for path, ftype in new],
                    "modified": [{"path": path, "type": ftype} for path, ftype in modified],
                    "deleted": [{"path": path, "type": ftype} for path, ftype in deleted],
                    "renamed": [{"old_path": old_path, "new_path": new_path, "type": ftype}
                               for old_path, new_path, ftype in renamed]
                },
                "summary": {
                    "new_count": len(new),
                    "modified_count": len(modified),
                    "deleted_count": len(deleted),
                    "renamed_count": len(renamed),
                    "total_changes": len(new) + len(modified) + len(deleted) + len(renamed)
                }
            }

        api_requests.labels(method="snapshot_diff", status="success").inc()
        return Success(result)

    except Exception as e:
        api_requests.labels(method="snapshot_diff", status="error").inc()
        logger.exception(f"Error comparing snapshots {snapshot1} and {snapshot2}")
        return Error(-32035, str(e))

# Bookmark operations
@method
@require_auth
async def bookmark_create(context: Dict[str, Any], snapshot: str, bookmark: str) -> Result:
    """Create a bookmark from a snapshot"""
    try:
        api_requests.labels(method="bookmark_create", status="started").inc()
        result = await zfs.bookmark_create(snapshot, bookmark)

        if result.success:
            api_requests.labels(method="bookmark_create", status="success").inc()
            return Success({"snapshot": snapshot, "bookmark": bookmark, "created": True})
        else:
            api_requests.labels(method="bookmark_create", status="failed").inc()
            return Error(-32035, f"Failed to create bookmark: {result.stderr}")

    except Exception as e:
        api_requests.labels(method="bookmark_create", status="error").inc()
        logger.exception(f"Error creating bookmark from {snapshot}")
        return Error(-32036, str(e))

@method
@require_auth
async def bookmark_list(context: Dict[str, Any], dataset: str) -> Result:
    """List bookmarks for a dataset"""
    try:
        api_requests.labels(method="bookmark_list", status="started").inc()
        bookmarks = await zfs.bookmark_list(dataset)

        api_requests.labels(method="bookmark_list", status="success").inc()
        return Success({"dataset": dataset, "bookmarks": bookmarks})

    except Exception as e:
        api_requests.labels(method="bookmark_list", status="error").inc()
        logger.exception(f"Error listing bookmarks for {dataset}")
        return Error(-32038, str(e))

@method
@require_auth
async def bookmark_destroy(context: Dict[str, Any], bookmark: str) -> Result:
    """Destroy a bookmark"""
    try:
        api_requests.labels(method="bookmark_destroy", status="started").inc()
        result = await zfs.bookmark_destroy(bookmark)

        if result.success:
            api_requests.labels(method="bookmark_destroy", status="success").inc()
            return Success({"bookmark": bookmark, "destroyed": True})
        else:
            api_requests.labels(method="bookmark_destroy", status="failed").inc()
            return Error(-32039, f"Failed to destroy bookmark: {result.stderr}")

    except Exception as e:
        api_requests.labels(method="bookmark_destroy", status="error").inc()
        logger.exception(f"Error destroying bookmark {bookmark}")
        return Error(-32040, str(e))

# Pool operations
@method
@require_auth
async def pool_list(context: Dict[str, Any]) -> Result:
    """List ZFS pools"""
    try:
        api_requests.labels(method="pool_list", status="started").inc()
        with api_duration.labels(method="pool_list").time():
            pools = await zfs.pool_list()

        api_requests.labels(method="pool_list", status="success").inc()
        return Success({"pools": pools})
    except Exception as e:
        api_requests.labels(method="pool_list", status="error").inc()
        logger.exception("Error listing pools")
        return Error(-32040, str(e))

@method
@require_auth
async def pool_get_properties(context: Dict[str, Any], pool: str, property: str = "all") -> Result:
    """Get pool properties"""
    try:
        api_requests.labels(method="pool_get_properties", status="started").inc()
        with api_duration.labels(method="pool_get_properties").time():
            props = await zfs.pool_get_properties(pool, property)

        api_requests.labels(method="pool_get_properties", status="success").inc()
        return Success({"pool": pool, "properties": props})
    except Exception as e:
        api_requests.labels(method="pool_get_properties", status="error").inc()
        logger.exception(f"Error getting properties for pool {pool}")
        return Error(-32041, str(e))

@method
@require_auth
async def pool_scrub_start(context: Dict[str, Any], pool: str) -> Result:
    """Start a scrub on a pool"""
    try:
        api_requests.labels(method="pool_scrub_start", status="started").inc()
        with api_duration.labels(method="pool_scrub_start").time():
            result = await zfs.pool_scrub_start(pool)

        if result.success:
            api_requests.labels(method="pool_scrub_start", status="success").inc()
            return Success({"pool": pool, "scrub": "started"})
        else:
            api_requests.labels(method="pool_scrub_start", status="failed").inc()
            return Error(-32042, f"Failed to start scrub: {result.stderr}")
    except Exception as e:
        api_requests.labels(method="pool_scrub_start", status="error").inc()
        logger.exception(f"Error starting scrub on pool {pool}")
        return Error(-32043, str(e))

@method
@require_auth
async def pool_scrub_stop(context: Dict[str, Any], pool: str) -> Result:
    """Stop a scrub on a pool"""
    try:
        api_requests.labels(method="pool_scrub_stop", status="started").inc()
        with api_duration.labels(method="pool_scrub_stop").time():
            result = await zfs.pool_scrub_stop(pool)

        if result.success:
            api_requests.labels(method="pool_scrub_stop", status="success").inc()
            return Success({"pool": pool, "scrub": "stopped"})
        else:
            api_requests.labels(method="pool_scrub_stop", status="failed").inc()
            return Error(-32044, f"Failed to stop scrub: {result.stderr}")
    except Exception as e:
        api_requests.labels(method="pool_scrub_stop", status="error").inc()
        logger.exception(f"Error stopping scrub on pool {pool}")
        return Error(-32045, str(e))

@method
@require_auth
async def pool_status(context: Dict[str, Any], pool: str) -> Result:
    """Get detailed pool status"""
    try:
        api_requests.labels(method="pool_status", status="started").inc()
        with api_duration.labels(method="pool_status").time():
            status_result = await zfs.pool_status(pool)

        result = {
            "pool": pool,
            "status": status_result.stdout if status_result.success else status_result.stderr
        }

        api_requests.labels(method="pool_status", status="success").inc()
        return Success(result)
    except Exception as e:
        api_requests.labels(method="pool_status", status="error").inc()
        logger.exception(f"Error getting status for pool {pool}")
        return Error(-32046, str(e))

# Clone operations
@method
@require_auth
async def clone_create(context: Dict[str, Any], source: str, target: str,
                      properties: Optional[Dict[str, str]] = None) -> Result:
    """Create a clone from a snapshot"""
    try:
        api_requests.labels(method="clone_create", status="started").inc()
        with api_duration.labels(method="clone_create").time():
            result = await zfs.clone_create(source, target, properties)

        if result.success:
            api_requests.labels(method="clone_create", status="success").inc()
            return Success({"source": source, "clone": target, "created": True})
        else:
            api_requests.labels(method="clone_create", status="failed").inc()
            return Error(-32050, f"Failed to create clone: {result.stderr}")
    except Exception as e:
        api_requests.labels(method="clone_create", status="error").inc()
        logger.exception(f"Error creating clone from {source} to {target}")
        return Error(-32051, str(e))

# Volume operations
@method
@require_auth
async def volume_create(context: Dict[str, Any], dataset: str, size_gb: Optional[int] = None,
                       size_bytes: Optional[int] = None, compression: str = "lz4",
                       volblocksize: str = "8K") -> Result:
    """Create a ZFS volume"""
    try:
        api_requests.labels(method="volume_create", status="started").inc()
        if not size_gb and not size_bytes:
            return Error(-32052, "Either size_gb or size_bytes must be specified")

        with api_duration.labels(method="volume_create").time():
            result = await zfs.volume_create(dataset, size_gb, size_bytes, compression, volblocksize)

        if result.success:
            api_requests.labels(method="volume_create", status="success").inc()
            return Success({"volume": dataset, "created": True})
        else:
            api_requests.labels(method="volume_create", status="failed").inc()
            return Error(-32053, f"Failed to create volume: {result.stderr}")
    except Exception as e:
        api_requests.labels(method="volume_create", status="error").inc()
        logger.exception(f"Error creating volume {dataset}")
        return Error(-32054, str(e))

@method
@require_auth
async def volume_list(context: Dict[str, Any]) -> Result:
    """List ZFS volumes"""
    try:
        api_requests.labels(method="volume_list", status="started").inc()
        with api_duration.labels(method="volume_list").time():
            volumes = await zfs.volume_list()

        api_requests.labels(method="volume_list", status="success").inc()
        return Success({"volumes": volumes})
    except Exception as e:
        api_requests.labels(method="volume_list", status="error").inc()
        logger.exception("Error listing volumes")
        return Error(-32055, str(e))

# Send/Receive operations
@method
@require_auth
async def send_estimate(context: Dict[str, Any], dataset: str, snapshot: str, 
                       from_snapshot: Optional[str] = None, recursive: bool = True,
                       raw: Optional[bool] = None, native_compressed: Optional[bool] = None,
                       resumable: bool = True) -> Result:
    """Estimate the size of a ZFS send operation with automatic flag detection"""
    try:
        api_requests.labels(method="send_estimate", status="started").inc()
        
        # Use enhanced get_send_size for full sends without from_snapshot
        if not from_snapshot:
            size_bytes = await async_wrapper(zfs_instance.get_send_size)(
                dataset, snapshot, recurse=recursive,
                raw=raw, compressed=native_compressed, resumable=resumable
            )
            if size_bytes is not None:
                # Format size for human display
                if size_bytes >= 1024 * 1024 * 1024 * 1024:
                    size_human = f"{size_bytes / (1024 * 1024 * 1024 * 1024):.2f}T"
                elif size_bytes >= 1024 * 1024 * 1024:
                    size_human = f"{size_bytes / (1024 * 1024 * 1024):.2f}G"
                elif size_bytes >= 1024 * 1024:
                    size_human = f"{size_bytes / (1024 * 1024):.2f}M"
                elif size_bytes >= 1024:
                    size_human = f"{size_bytes / 1024:.2f}K"
                else:
                    size_human = str(size_bytes)
                
                api_requests.labels(method="send_estimate", status="success").inc()
                return Success({
                    "dataset": dataset,
                    "snapshot": snapshot,
                    "from_snapshot": from_snapshot,
                    "size_bytes": size_bytes,
                    "size_human": size_human
                })
        
        # For incremental sends, build command manually with autodetection
        cmd = ["zfs", "send", "-nv"]
        
        # Autodetect or use explicit raw flag
        use_raw = raw
        if use_raw is None:
            try:
                encryption = await async_wrapper(zfs_instance.get)(dataset, 'encryption')
                use_raw = encryption is not None and encryption != 'off'
            except:
                use_raw = False
        
        if use_raw:
            cmd.append("-w")
        
        # Autodetect or use explicit compressed flag
        use_compressed = native_compressed
        if use_compressed is None:
            try:
                compression = await async_wrapper(zfs_instance.get)(dataset, 'compression')
                use_compressed = compression is not None and compression != 'off'
                if not use_compressed:
                    compressratio = await async_wrapper(zfs_instance.get)(dataset, 'compressratio')
                    if compressratio and compressratio != '1.00x':
                        use_compressed = True
            except:
                use_compressed = False
        
        if use_compressed:
            cmd.append("-c")
        
        if resumable:
            cmd.append("-s")
        
        if recursive:
            cmd.append("-R")
        
        if from_snapshot:
            # Incremental send
            cmd.extend(["-I", f"{dataset}@{from_snapshot}", f"{dataset}@{snapshot}"])
        else:
            # Full send
            cmd.append(f"{dataset}@{snapshot}")
        
        # Execute estimation
        loop = asyncio.get_event_loop()
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await proc.communicate()
        
        if proc.returncode == 0:
            # Parse size from output (last line contains size)
            output = stderr.decode('utf-8').strip()
            lines = output.split('\n')
            size_str = "0"
            
            # Find the line with size info
            if lines:
                last_line = lines[-1].strip()
                if last_line and ' ' in last_line:
                    parts = last_line.split()
                    if parts:
                        size_str = parts[-1]
            
            # Convert size to bytes
            size_bytes = 0
            if size_str.endswith('K'):
                size_bytes = int(float(size_str[:-1]) * 1024)
            elif size_str.endswith('M'):
                size_bytes = int(float(size_str[:-1]) * 1024 * 1024)
            elif size_str.endswith('G'):
                size_bytes = int(float(size_str[:-1]) * 1024 * 1024 * 1024)
            elif size_str.endswith('T'):
                size_bytes = int(float(size_str[:-1]) * 1024 * 1024 * 1024 * 1024)
            else:
                size_bytes = int(size_str)
            
            api_requests.labels(method="send_estimate", status="success").inc()
            return Success({
                "dataset": dataset,
                "snapshot": snapshot,
                "from_snapshot": from_snapshot,
                "size_bytes": size_bytes,
                "size_human": size_str
            })
        else:
            api_requests.labels(method="send_estimate", status="failed").inc()
            return Error(-32060, f"Failed to estimate send size: {stderr.decode('utf-8')}")
            
    except Exception as e:
        api_requests.labels(method="send_estimate", status="error").inc()
        logger.exception(f"Error estimating send size for {dataset}@{snapshot}")
        return Error(-32061, str(e))

@method
@require_auth
async def send_to_file(context: Dict[str, Any], dataset: str, snapshot: str, 
                      output_file: str, from_snapshot: Optional[str] = None, 
                      recursive: bool = True, compressed: bool = False,
                      raw: Optional[bool] = None, native_compressed: Optional[bool] = None,
                      resumable: bool = False) -> Result:
    """Send a ZFS snapshot to a file with automatic flag detection
    
    Args:
        dataset: The dataset to send
        snapshot: The snapshot name
        output_file: Output file path
        from_snapshot: Optional base snapshot for incremental send
        recursive: Whether to send recursively (default: True)
        compressed: Whether to gzip the output (default: False)
        raw: Send raw encrypted stream. None=autodetect, True/False=force
        native_compressed: Send compressed blocks (-c flag). None=autodetect, True/False=force
        resumable: Create resumable send stream -s flag (default: True)
    """
    try:
        api_requests.labels(method="send_to_file", status="started").inc()
        
        # Use the enhanced send method from zfs module
        if from_snapshot:
            # For incremental sends, we need to build command manually
            # since zfs.send() doesn't directly support our incremental format
            send_cmd = ["zfs", "send"]
            
            # Autodetect or use explicit raw flag
            use_raw = raw
            if use_raw is None:
                try:
                    encryption = await async_wrapper(zfs_instance.get)(dataset, 'encryption')
                    use_raw = encryption is not None and encryption != 'off'
                except:
                    use_raw = False
            
            if use_raw:
                send_cmd.append("-w")
            
            # Autodetect or use explicit compressed flag
            use_compressed = native_compressed
            if use_compressed is None:
                try:
                    compression = await async_wrapper(zfs_instance.get)(dataset, 'compression')
                    use_compressed = compression is not None and compression != 'off'
                except:
                    use_compressed = False
            
            if use_compressed:
                send_cmd.append("-c")
            
            if resumable:
                send_cmd.append("-s")
            
            if recursive:
                send_cmd.append("-R")
            
            send_cmd.extend(["-I", f"{dataset}@{from_snapshot}", f"{dataset}@{snapshot}"])
        else:
            # For full sends, we can use the enhanced send method
            send_proc = await async_wrapper(zfs_instance.send)(
                dataset, snapshot, recurse=recursive,
                raw=raw, compressed=native_compressed, resumable=resumable
            )
            if not send_proc:
                raise Exception("Failed to create send process")
            send_cmd = send_proc.args
        
        # Build compression command if requested
        if compressed:
            # Use gzip for compression
            cmd = f"{' '.join(send_cmd)} | gzip > {output_file}.gz"
            output_file = f"{output_file}.gz"
        else:
            cmd = f"{' '.join(send_cmd)} > {output_file}"
        
        # Execute send operation
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # Start monitoring task
        start_time = time.time()
        
        # Wait for completion
        stdout, stderr = await proc.communicate()
        
        duration = time.time() - start_time
        
        if proc.returncode == 0:
            # Get file size
            import os
            file_size = os.path.getsize(output_file) if os.path.exists(output_file) else 0
            
            api_requests.labels(method="send_to_file", status="success").inc()
            return Success({
                "dataset": dataset,
                "snapshot": snapshot,
                "from_snapshot": from_snapshot,
                "output_file": output_file,
                "compressed": compressed,
                "size_bytes": file_size,
                "duration_seconds": duration
            })
        else:
            api_requests.labels(method="send_to_file", status="failed").inc()
            return Error(-32062, f"Failed to send snapshot: {stderr.decode('utf-8')}")
            
    except Exception as e:
        api_requests.labels(method="send_to_file", status="error").inc()
        logger.exception(f"Error sending {dataset}@{snapshot} to file")
        return Error(-32063, str(e))

@method
@require_auth
async def receive_from_file(context: Dict[str, Any], dataset: str, input_file: str, 
                           force: bool = True, compressed: bool = False) -> Result:
    """Receive a ZFS snapshot from a file"""
    try:
        api_requests.labels(method="receive_from_file", status="started").inc()
        
        # Check if file exists
        import os
        if not os.path.exists(input_file):
            return Error(-32064, f"Input file not found: {input_file}")
        
        # Build receive command
        recv_cmd = ["zfs", "receive"]
        if force:
            recv_cmd.append("-F")
        recv_cmd.append(dataset)
        
        # Build decompression command if needed
        if compressed or input_file.endswith('.gz'):
            cmd = f"gzip -dc {input_file} | {' '.join(recv_cmd)}"
        else:
            cmd = f"{' '.join(recv_cmd)} < {input_file}"
        
        # Execute receive operation
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        start_time = time.time()
        
        # Wait for completion
        stdout, stderr = await proc.communicate()
        
        duration = time.time() - start_time
        
        if proc.returncode == 0:
            api_requests.labels(method="receive_from_file", status="success").inc()
            return Success({
                "dataset": dataset,
                "input_file": input_file,
                "force": force,
                "compressed": compressed,
                "duration_seconds": duration
            })
        else:
            api_requests.labels(method="receive_from_file", status="failed").inc()
            return Error(-32065, f"Failed to receive snapshot: {stderr.decode('utf-8')}")
            
    except Exception as e:
        api_requests.labels(method="receive_from_file", status="error").inc()
        logger.exception(f"Error receiving snapshot from {input_file}")
        return Error(-32066, str(e))


# Streaming operations storage - DEPRECATED, MARKED FOR DELETION
# TODO: Remove this variable after confirming no dependencies
streaming_sessions = {}

# HTTP streaming session methods - DEPRECATED AND MARKED FOR DELETION
# These methods have poor performance compared to alternatives:
# - Use /upload/receive and /download/send endpoints for HTTP streaming
# - Use token-authenticated socket streaming (port 9999 or Unix socket) for maximum performance
# TODO: Remove all code below until "Token-based socket authentication methods" comment

# @method
# @require_auth
# async def stream_init(context: Dict[str, Any], operation: str, dataset: str, 
#                      snapshot: Optional[str] = None, target_dataset: Optional[str] = None,
#                      from_snapshot: Optional[str] = None, force: bool = True,
#                      raw: Optional[bool] = None, native_compressed: Optional[bool] = None,
#                      resumable: bool = True, recursive: bool = True) -> Result:
#     """Initialize a streaming send or receive operation with automatic flag detection
#     
#     Args:
#         operation: 'send' or 'receive'
#         dataset: The dataset to send/receive
#         snapshot: The snapshot name (for send operations)
#         target_dataset: Target dataset (for receive operations)
#         from_snapshot: Base snapshot for incremental sends
#         force: Force receive operations
#         raw: Send raw encrypted stream. None=autodetect, True/False=force
#         native_compressed: Send compressed blocks (-c flag). None=autodetect, True/False=force
#         resumable: Create resumable send stream -s flag (default: True)
#         recursive: Whether to send recursively (default: True)
#     """
#     try:
#         stream_id = str(uuid.uuid4())
#         
#         # Store stream info
#         stream_info = {
#             "operation": operation,
#             "dataset": dataset,
#             "snapshot": snapshot,
#             "target_dataset": target_dataset or dataset,
#             "from_snapshot": from_snapshot,
#             "force": force,
#             "raw": raw,
#             "native_compressed": native_compressed,
#             "resumable": resumable,
#             "recursive": recursive,
#             "created": datetime.now(timezone.utc),
#             "username": context.get("username", "unknown")
#         }
#         
#         streaming_sessions[stream_id] = stream_info
#         
#         return Success({
#             "stream_id": stream_id,
#             "url": f"http://{context['request'].host}/stream/{stream_id}",
#             "method": "GET" if operation == "send" else "POST"
#         })
#         
#     except Exception as e:
#         logger.exception(f"Error initializing stream")
#         return Error(-32070, str(e))
# 
# @method
# @require_auth
# async def stream_list(context: Dict[str, Any]) -> Result:
#     """List active streaming sessions"""
#     sessions = []
#     for stream_id, info in streaming_sessions.items():
#         sessions.append({
#             "stream_id": stream_id,
#             "operation": info["operation"],
#             "dataset": info["dataset"],
#             "created": info["created"].isoformat(),
#             "username": info["username"]
#         })
#     return Success({"sessions": sessions})
# 
# @method
# @require_auth
# async def stream_cancel(context: Dict[str, Any], stream_id: str) -> Result:
#     """Cancel a streaming session"""
#     if stream_id in streaming_sessions:
#         del streaming_sessions[stream_id]
#         return Success({"cancelled": True})
#     else:
#         return Error(-32071, "Stream session not found")

# Token-based socket authentication methods
@method
@require_auth
async def token_create_send(context: Dict[str, Any], dataset: str, snapshot: str,
                           from_snapshot: Optional[str] = None,
                           raw: Optional[bool] = None,
                           compressed: Optional[bool] = None,
                           resumable: bool = True,
                           recursive: bool = False,
                           ttl: int = 300) -> Result:
    """Create a token for authenticated socket send operation
    
    Args:
        dataset: The dataset to send
        snapshot: The snapshot name
        from_snapshot: Base snapshot for incremental send
        raw: Send raw encrypted stream. None=autodetect
        compressed: Send compressed blocks. None=autodetect
        resumable: Create resumable send stream
        recursive: Send recursively
        ttl: Token time-to-live in seconds (default: 5 minutes)
    """
    try:
        if not token_manager:
            return Error(-32080, "Token authentication not configured")
        
        api_requests.labels(method="token_create_send", status="started").inc()
        
        # Get client IP from request
        client_ip = context.get("request", {}).get("remote", "unknown")
        username = context.get("username", "unknown")
        
        # Create token with parameters
        token_info = await token_manager.create_token(
            operation="send",
            dataset=dataset,
            user_id=username,
            client_ip=client_ip,
            snapshot=snapshot,
            parameters={
                "from_snapshot": from_snapshot,
                "raw": raw,
                "compressed": compressed,
                "resumable": resumable,
                "recursive": recursive
            },
            ttl=ttl
        )
        
        if not token_info:
            api_requests.labels(method="token_create_send", status="failed").inc()
            return Error(-32081, "Failed to create token")
        
        api_requests.labels(method="token_create_send", status="success").inc()
        return Success({
            "token": token_info["token"],
            "expires_in": token_info["expires_in"],
            "operation": "send",
            "dataset": dataset,
            "snapshot": snapshot,
            "socket_tcp": f"{config.config['server'].get('host', '0.0.0.0')}:9999",
            "socket_unix": "./zfs_token_socket"
        })
        
    except Exception as e:
        api_requests.labels(method="token_create_send", status="error").inc()
        logger.exception("Error creating send token")
        return Error(-32082, str(e))

@method
@require_auth
async def token_create_receive(context: Dict[str, Any], dataset: str,
                             force: bool = True,
                             resumable: bool = False,
                             ttl: int = 300) -> Result:
    """Create a token for authenticated socket receive operation
    
    Args:
        dataset: The dataset to receive into
        force: Force receive (overwrite existing)
        resumable: Support resumable receives
        ttl: Token time-to-live in seconds (default: 5 minutes)
    """
    try:
        if not token_manager:
            return Error(-32083, "Token authentication not configured")
        
        api_requests.labels(method="token_create_receive", status="started").inc()
        
        # Get client IP from request
        client_ip = context.get("request", {}).get("remote", "unknown")
        username = context.get("username", "unknown")
        
        # Create token with parameters
        token_info = await token_manager.create_token(
            operation="receive",
            dataset=dataset,
            user_id=username,
            client_ip=client_ip,
            parameters={
                "force": force,
                "resumable": resumable
            },
            ttl=ttl
        )
        
        if not token_info:
            api_requests.labels(method="token_create_receive", status="failed").inc()
            return Error(-32084, "Failed to create token")
        
        api_requests.labels(method="token_create_receive", status="success").inc()
        return Success({
            "token": token_info["token"],
            "expires_in": token_info["expires_in"],
            "operation": "receive",
            "dataset": dataset,
            "socket_tcp": f"{config.config['server'].get('host', '0.0.0.0')}:9999",
            "socket_unix": "./zfs_token_socket"
        })
        
    except Exception as e:
        api_requests.labels(method="token_create_receive", status="error").inc()
        logger.exception("Error creating receive token")
        return Error(-32085, str(e))

@method
@require_auth
async def token_list(context: Dict[str, Any]) -> Result:
    """List all active tokens for the current user"""
    try:
        if not token_manager:
            return Error(-32086, "Token authentication not configured")
        
        api_requests.labels(method="token_list", status="started").inc()
        
        username = context.get("username", "unknown")
        
        # Get user's tokens
        tokens = await token_manager.list_user_tokens(username)
        
        # Format token info
        token_list = []
        for token in tokens:
            token_list.append({
                "token_id": token.get("token_id", "")[:8] + "...",  # Show only first 8 chars
                "operation": token.get("operation"),
                "dataset": token.get("dataset"),
                "created_at": token.get("created_at"),
                "expires_at": token.get("expires_at"),
                "used": token.get("used", False),
                "use_count": token.get("use_count", 0)
            })
        
        api_requests.labels(method="token_list", status="success").inc()
        return Success({"tokens": token_list})
        
    except Exception as e:
        api_requests.labels(method="token_list", status="error").inc()
        logger.exception("Error listing tokens")
        return Error(-32087, str(e))

@method
@require_auth
async def token_revoke(context: Dict[str, Any], token_id: str) -> Result:
    """Revoke a token immediately"""
    try:
        if not token_manager:
            return Error(-32088, "Token authentication not configured")
        
        api_requests.labels(method="token_revoke", status="started").inc()
        
        # Revoke the token
        success = await token_manager.revoke_token(token_id)
        
        if success:
            api_requests.labels(method="token_revoke", status="success").inc()
            return Success({"revoked": True})
        else:
            api_requests.labels(method="token_revoke", status="failed").inc()
            return Error(-32089, "Token not found or already revoked")
        
    except Exception as e:
        api_requests.labels(method="token_revoke", status="error").inc()
        logger.exception("Error revoking token")
        return Error(-32090, str(e))

@method
@require_auth
async def token_stats(context: Dict[str, Any]) -> Result:
    """Get token system statistics"""
    try:
        if not token_manager:
            return Error(-32091, "Token authentication not configured")
        
        api_requests.labels(method="token_stats", status="started").inc()
        
        # Get statistics
        stats = await token_manager.get_stats()
        
        api_requests.labels(method="token_stats", status="success").inc()
        return Success(stats)
        
    except Exception as e:
        api_requests.labels(method="token_stats", status="error").inc()
        logger.exception("Error getting token stats")
        return Error(-32092, str(e))

# Migration management via JSON-RPC
@method
@require_auth
async def migration_create(context: Dict[str, Any], source: str, destination: str,
                          remote: Optional[str] = None, limit: Optional[int] = None,
                          compression: Optional[str] = None, recursive: bool = False,
                          sync: bool = True) -> Result:
    """Create a new migration task
    
    Args:
        source: Source dataset (e.g., 'pool/dataset@snapshot')
        destination: Destination dataset path
        remote: Remote host for SSH-based migration (optional)
        limit: Bandwidth limit in MB/s (optional)
        compression: Compression type ('gzip', 'lz4', 'zstd', etc.)
        recursive: Include child datasets
        sync: Keep snapshots with holds for replication (default: True)
    """
    try:
        if not task_manager:
            return Error(-32100, "Migration service not available")
            
        api_requests.labels(method="migration_create", status="started").inc()
        
        # Build task parameters
        params = {
            'source': source,
            'destination': destination,
            'recursive': recursive,
            'sync': sync
        }
        
        if remote:
            params['remote'] = remote
        if limit:
            params['limit'] = limit
        if compression:
            params['compression'] = compression
            
        # Create background task
        task_id = await task_manager.create_task('migration', params)
        
        api_requests.labels(method="migration_create", status="success").inc()
        return Success({
            'task_id': task_id,
            'status': 'created',
            'params': params
        })
        
    except Exception as e:
        api_requests.labels(method="migration_create", status="error").inc()
        logger.exception("Error creating migration")
        return Error(-32101, str(e))

@method
@require_auth
async def migration_list(context: Dict[str, Any], status: Optional[str] = None,
                        limit: int = 100) -> Result:
    """List migration tasks
    
    Args:
        status: Filter by status (pending, running, completed, failed, cancelled)
        limit: Maximum number of tasks to return
    """
    try:
        if not task_manager:
            return Error(-32102, "Migration service not available")
            
        api_requests.labels(method="migration_list", status="started").inc()
        
        # Get all task keys from Redis
        keys = await task_manager.redis.keys('task:*') or []
        tasks = []
        
        for key in keys:
            task_id = key.decode().split(":", 1)[1] if isinstance(key, bytes) else key.split(":", 1)[1]
            task = await task_manager.get_task(task_id)
            if task and task.type == 'migration':
                # Filter by status if specified
                if status and task.status.value != status:
                    continue
                    
                tasks.append({
                    'id': task.id,
                    'status': task.status.value,
                    'created_at': task.created_at.isoformat(),
                    'started_at': task.started_at.isoformat() if task.started_at else None,
                    'completed_at': task.completed_at.isoformat() if task.completed_at else None,
                    'error': task.error,
                    'progress': task.progress,
                    'params': task.params
                })
                
                if len(tasks) >= limit:
                    break
        
        # Sort by created_at descending
        tasks.sort(key=lambda t: t['created_at'], reverse=True)
        
        api_requests.labels(method="migration_list", status="success").inc()
        return Success({
            'tasks': tasks,
            'total': len(tasks)
        })
        
    except Exception as e:
        api_requests.labels(method="migration_list", status="error").inc()
        logger.exception("Error listing migrations")
        return Error(-32103, str(e))

@method
@require_auth
async def migration_get(context: Dict[str, Any], task_id: str) -> Result:
    """Get migration task details
    
    Args:
        task_id: Task UUID
    """
    try:
        if not task_manager:
            return Error(-32104, "Migration service not available")
            
        api_requests.labels(method="migration_get", status="started").inc()
        
        task = await task_manager.get_task(task_id)
        if not task:
            api_requests.labels(method="migration_get", status="failed").inc()
            return Error(-32105, "Task not found")
            
        if task.type != 'migration':
            api_requests.labels(method="migration_get", status="failed").inc()
            return Error(-32106, "Not a migration task")
            
        api_requests.labels(method="migration_get", status="success").inc()
        return Success({
            'id': task.id,
            'status': task.status.value,
            'created_at': task.created_at.isoformat(),
            'started_at': task.started_at.isoformat() if task.started_at else None,
            'completed_at': task.completed_at.isoformat() if task.completed_at else None,
            'error': task.error,
            'result': task.result,
            'progress': task.progress,
            'params': task.params
        })
        
    except Exception as e:
        api_requests.labels(method="migration_get", status="error").inc()
        logger.exception(f"Error getting migration {task_id}")
        return Error(-32107, str(e))

@method
@require_auth
async def migration_cancel(context: Dict[str, Any], task_id: str) -> Result:
    """Cancel a running migration
    
    Args:
        task_id: Task UUID
    """
    try:
        if not task_manager:
            return Error(-32108, "Migration service not available")
            
        api_requests.labels(method="migration_cancel", status="started").inc()
        
        # Verify it's a migration task
        task = await task_manager.get_task(task_id)
        if not task:
            api_requests.labels(method="migration_cancel", status="failed").inc()
            return Error(-32109, "Task not found")
            
        if task.type != 'migration':
            api_requests.labels(method="migration_cancel", status="failed").inc()
            return Error(-32110, "Not a migration task")
            
        # Cancel the task
        success = await task_manager.cancel_task(task_id)
        
        if success:
            api_requests.labels(method="migration_cancel", status="success").inc()
            return Success({
                'task_id': task_id,
                'cancelled': True
            })
        else:
            api_requests.labels(method="migration_cancel", status="failed").inc()
            return Error(-32111, "Failed to cancel task - may have already completed")
            
    except Exception as e:
        api_requests.labels(method="migration_cancel", status="error").inc()
        logger.exception(f"Error cancelling migration {task_id}")
        return Error(-32112, str(e))

@method
@require_auth
async def migration_progress(context: Dict[str, Any], task_id: str) -> Result:
    """Get migration progress
    
    Args:
        task_id: Task UUID
    """
    try:
        if not task_manager:
            return Error(-32113, "Migration service not available")
            
        api_requests.labels(method="migration_progress", status="started").inc()
        
        task = await task_manager.get_task(task_id)
        if not task:
            api_requests.labels(method="migration_progress", status="failed").inc()
            return Error(-32114, "Task not found")
            
        if task.type != 'migration':
            api_requests.labels(method="migration_progress", status="failed").inc()
            return Error(-32115, "Not a migration task")
            
        api_requests.labels(method="migration_progress", status="success").inc()
        return Success({
            'task_id': task_id,
            'status': task.status.value,
            'progress': task.progress or {},
            'error': task.error
        })
        
    except Exception as e:
        api_requests.labels(method="migration_progress", status="error").inc()
        logger.exception(f"Error getting migration progress {task_id}")
        return Error(-32116, str(e))

# HTTP streaming handlers - DEPRECATED AND MARKED FOR DELETION
# TODO: Remove handle_stream_send() and handle_stream_receive() functions
# Use handle_upload_receive() and handle_download_send() for HTTP streaming instead
async def handle_stream_send(request):
    """Handle streaming ZFS send (GET request)"""
    stream_id = request.match_info['stream_id']
    stream_info = streaming_sessions.get(stream_id)
    
    if not stream_info:
        return web.Response(status=404, text="Stream session not found")
    
    if stream_info["operation"] != "send":
        return web.Response(status=400, text="Invalid operation for GET")
    
    try:
        # Build ZFS send command with autodetection
        cmd = ["zfs", "send"]
        
        # Autodetect or use explicit raw flag
        use_raw = stream_info.get("raw")
        if use_raw is None:
            # Autodetect: check if dataset is encrypted
            try:
                encryption = zfs_instance.get(stream_info['dataset'], 'encryption')
                use_raw = encryption is not None and encryption != 'off'
            except:
                use_raw = False
        
        if use_raw:
            cmd.append("-w")
        
        # Autodetect or use explicit compressed flag
        use_compressed = stream_info.get("native_compressed")
        if use_compressed is None:
            # Autodetect: check if dataset uses compression
            try:
                compression = zfs_instance.get(stream_info['dataset'], 'compression')
                use_compressed = compression is not None and compression != 'off'
            except:
                use_compressed = False
        
        if use_compressed:
            cmd.append("-c")
        
        # Note: resumable flag (-s) is for receive operations, not send
        # For send, -s means "skip missing snapshots" which requires -R
        
        # Add recursive flag if requested
        if stream_info.get("recursive", True):
            cmd.append("-R")
        
        if stream_info["from_snapshot"]:
            # Incremental send
            cmd.extend(["-i", f"{stream_info['dataset']}@{stream_info['from_snapshot']}"])
            
        snapshot_name = f"{stream_info['dataset']}@{stream_info['snapshot']}"
        cmd.append(snapshot_name)
        
        # Log the operation
        logger.info(f"Starting stream send: {' '.join(cmd)}")
        
        # Create response
        response = web.StreamResponse(
            status=200,
            headers={
                'Content-Type': 'application/octet-stream',
                'Content-Disposition': f'attachment; filename="{stream_info["snapshot"]}.zfs"',
                'X-ZFS-Dataset': stream_info["dataset"],
                'X-ZFS-Snapshot': stream_info["snapshot"]
            }
        )
        await response.prepare(request)
        
        # Start ZFS send process
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # Stream data
        bytes_sent = 0
        chunk_size = 16777216  # 16MB chunks for better performance
        start_time = time.time()
        
        try:
            while True:
                chunk = await proc.stdout.read(chunk_size)
                if not chunk:
                    break
                    
                await response.write(chunk)
                bytes_sent += len(chunk)
                
                # Log progress every 1GB
                if bytes_sent % (1024 * 1048576) == 0:
                    elapsed = time.time() - start_time
                    rate = bytes_sent / elapsed / 1048576  # MB/s
                    logger.info(f"Stream {stream_id}: {bytes_sent / 1073741824:.2f}GB sent @ {rate:.2f}MB/s")
        
        finally:
            await proc.wait()
            
            if proc.returncode != 0:
                stderr = await proc.stderr.read()
                logger.error(f"ZFS send failed: {stderr.decode('utf-8')}")
        
        elapsed = time.time() - start_time
        logger.info(f"Stream send completed: {bytes_sent / 1073741824:.2f}GB in {elapsed:.2f}s")
        
        # Clean up session
        del streaming_sessions[stream_id]
        
        await response.write_eof()
        return response
        
    except Exception as e:
        logger.exception(f"Error in stream send")
        if stream_id in streaming_sessions:
            del streaming_sessions[stream_id]
        return web.Response(status=500, text=str(e))

async def handle_stream_receive(request):
    """Handle streaming ZFS receive (POST request)"""
    stream_id = request.match_info['stream_id']
    stream_info = streaming_sessions.get(stream_id)
    
    if not stream_info:
        return web.Response(status=404, text="Stream session not found")
    
    if stream_info["operation"] != "receive":
        return web.Response(status=400, text="Invalid operation for POST")
    
    try:
        # Build ZFS receive command
        cmd = ["zfs", "receive"]
        if stream_info["force"]:
            cmd.append("-F")
        cmd.append(stream_info["target_dataset"])
        
        # Log the operation
        logger.info(f"Starting stream receive: {' '.join(cmd)}")
        
        # Start ZFS receive process
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # Stream data from request to ZFS
        bytes_received = 0
        start_time = time.time()
        
        try:
            async for chunk in request.content.iter_chunked(16777216):  # 16MB chunks
                if proc.stdin.is_closing():
                    break
                    
                proc.stdin.write(chunk)
                await proc.stdin.drain()
                bytes_received += len(chunk)
                
                # Log progress every 1GB
                if bytes_received % (1024 * 1048576) == 0:
                    elapsed = time.time() - start_time
                    rate = bytes_received / elapsed / 1048576  # MB/s
                    logger.info(f"Stream {stream_id}: {bytes_received / 1073741824:.2f}GB received @ {rate:.2f}MB/s")
                    
        except Exception as e:
            logger.error(f"Error writing to ZFS receive: {e}")
            proc.stdin.close()
            await proc.wait()
            raise
            
        proc.stdin.close()
        stdout, stderr = await proc.communicate()
        
        if proc.returncode != 0:
            logger.error(f"ZFS receive failed: {stderr.decode('utf-8')}")
            return web.Response(
                status=400, 
                text=f"ZFS receive failed: {stderr.decode('utf-8')}"
            )
        
        elapsed = time.time() - start_time
        logger.info(f"Stream receive completed: {bytes_received / 1073741824:.2f}GB in {elapsed:.2f}s")
        
        # Clean up session
        del streaming_sessions[stream_id]
        
        return web.Response(text=json.dumps({
            "success": True,
            "bytes_received": bytes_received,
            "dataset": stream_info["target_dataset"],
            "duration": elapsed,
            "rate_mbps": bytes_received / elapsed / 1048576
        }), content_type="application/json")
        
    except Exception as e:
        logger.exception(f"Error in stream receive")
        if stream_id in streaming_sessions:
            del streaming_sessions[stream_id]
        return web.Response(status=500, text=str(e))

# HTTP request handlers
async def handle_jsonrpc(request):
    """Handle JSON-RPC requests"""
    try:
        # Get client ID for rate limiting
        client_id = request.remote
        
        # Check rate limit
        if not check_rate_limit(client_id):
            return web.json_response({
                "jsonrpc": "2.0",
                "error": {
                    "code": -32000,
                    "message": "Rate limit exceeded"
                },
                "id": None
            }, status=429)
        
        # Get request data
        data = await request.text()
        
        # Extract auth token from headers
        auth_header = request.headers.get("Authorization", "")
        token = None
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
        
        # Create context
        context = {
            "request": request,
            "token": token,
            "client_id": client_id
        }
        
        # Dispatch request
        response = await async_dispatch(data, context=context)
        
        return web.Response(text=response, content_type="application/json")
        
    except Exception as e:
        logger.exception("Error handling JSON-RPC request")
        return web.json_response({
            "jsonrpc": "2.0",
            "error": {
                "code": -32603,
                "message": "Internal error",
                "data": str(e)
            },
            "id": None
        }, status=500)

async def handle_metrics(request):
    """Prometheus metrics endpoint"""
    metrics = generate_latest()
    return web.Response(body=metrics, content_type="text/plain")

async def handle_health(request):
    """Health check endpoint"""
    try:
        # Simple health check - verify we can list pools
        pools = zpool_instance.list()
        return web.json_response({
            "status": "healthy",
            "pools": len(pools),
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    except Exception as e:
        return web.json_response({
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }, status=503)

async def handle_upload_receive(request):
    """Handle ZFS receive via streaming file upload - no buffering!"""
    try:
        # Check authentication
        auth_header = request.headers.get('Authorization')
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header.split(' ')[1]
            username = verify_token(token)
            if not username:
                return web.Response(status=401, text="Invalid token")
        elif config.config['auth']['enabled']:
            return web.Response(status=401, text="Authentication required")
        else:
            username = "anonymous"
        
        # Parse multipart form data
        reader = await request.multipart()
        
        dataset = None
        force = True
        resumable = False
        filename = None
        zfs_file = None
        
        # Process form fields first
        while True:
            part = await reader.next()
            if part is None:
                break
            
            if part.name == 'dataset':
                dataset = (await part.read()).decode('utf-8')
            elif part.name == 'force':
                force = (await part.read()).decode('utf-8').lower() == 'true'
            elif part.name == 'resumable':
                resumable = (await part.read()).decode('utf-8').lower() == 'true'
            elif part.name == 'file':
                # This is the uploaded file - don't read it yet!
                zfs_file = part
                filename = part.filename or 'stream.zfs'
                break
        
        if not dataset:
            return web.Response(status=400, text="Dataset name required")
        
        if not zfs_file:
            return web.Response(status=400, text="No file uploaded")
        
        # Build ZFS receive command
        zfs_cmd = ['zfs', 'receive']
        if force:
            zfs_cmd.append('-F')
        if resumable:
            zfs_cmd.append('-s')
        zfs_cmd.append(dataset)
        
        # Determine decompression based on filename
        if filename.endswith('.gz'):
            decompress_cmd = ['gzip', '-dc']
        elif filename.endswith('.zst') or filename.endswith('.zstd'):
            decompress_cmd = ['zstd', '-dc']
        else:
            decompress_cmd = None
            
        # Log the operation
        logger.info(f"Starting upload receive for {dataset} from {filename}")
        
        # Create the pipeline
        start_time = time.time()
        bytes_uploaded = 0
        
        if decompress_cmd:
            # Create pipe for decompressor -> zfs pipeline
            import os
            read_fd, write_fd = os.pipe()
            
            # Create decompressor process
            decompress_proc = await asyncio.create_subprocess_exec(
                *decompress_cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=write_fd,
                stderr=asyncio.subprocess.PIPE
            )
            
            # Close write end in parent
            os.close(write_fd)
            
            # Create zfs receive process
            zfs_proc = await asyncio.create_subprocess_exec(
                *zfs_cmd,
                stdin=read_fd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            # Close read end in parent
            os.close(read_fd)
            
            target_stdin = decompress_proc.stdin
        else:
            # Direct to zfs receive
            zfs_proc = await asyncio.create_subprocess_exec(
                *zfs_cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            decompress_proc = None
            target_stdin = zfs_proc.stdin
        
        try:
            # Stream the file data
            async for chunk in zfs_file:
                if chunk:
                    target_stdin.write(chunk)
                    await target_stdin.drain()
                    bytes_uploaded += len(chunk)
                    
                    # Log progress every 100MB
                    if bytes_uploaded % (100 * 1024 * 1024) == 0:
                        logger.info(f"Upload progress: {bytes_uploaded / (1024*1024):.1f} MB")
                        
            # Close stdin to signal EOF
            target_stdin.close()
            
            # Wait for processes to complete
            if decompress_proc:
                await decompress_proc.wait()
                
            await zfs_proc.wait()
            duration = time.time() - start_time
            
            if zfs_proc.returncode == 0:
                rate_mbps = bytes_uploaded / duration / (1024 * 1024) if duration > 0 else 0
                return web.json_response({
                    'status': 'success',
                    'dataset': dataset,
                    'bytes_uploaded': bytes_uploaded,
                    'filename': filename,
                    'duration_seconds': duration,
                    'rate_mbps': rate_mbps,
                    'compression': 'gzip' if filename.endswith('.gz') else 'zstd' if filename.endswith(('.zst', '.zstd')) else 'none'
                })
            else:
                # Get error output
                zfs_stderr = await zfs_proc.stderr.read()
                error_msg = zfs_stderr.decode('utf-8', errors='ignore')
                
                if decompress_proc and decompress_proc.returncode != 0:
                    decompress_stderr = await decompress_proc.stderr.read()
                    error_msg = f"Decompression error: {decompress_stderr.decode('utf-8', errors='ignore')}\n{error_msg}"
                    
                return web.json_response({
                    'status': 'error',
                    'error': error_msg,
                    'bytes_uploaded': bytes_uploaded
                }, status=500)
                
        except Exception as e:
            # Clean up processes on error
            if target_stdin and not target_stdin.is_closing():
                target_stdin.close()
            if decompress_proc:
                try:
                    decompress_proc.terminate()
                    await decompress_proc.wait()
                except:
                    pass
            try:
                zfs_proc.terminate()
                await zfs_proc.wait()
            except:
                pass
                
            logger.exception("Stream upload error")
            return web.json_response({
                'status': 'error', 
                'error': str(e),
                'bytes_uploaded': bytes_uploaded
            }, status=500)
                
    except Exception as e:
        logger.exception("Upload handler error")
        return web.Response(status=500, text=str(e))

async def handle_download_send(request):
    """Handle ZFS send via streaming download - supports full and incremental sends"""
    try:
        # Check authentication
        auth_header = request.headers.get('Authorization')
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header.split(' ')[1]
            username = verify_token(token)
            if not username:
                return web.Response(status=401, text="Invalid token")
        elif config.config['auth']['enabled']:
            return web.Response(status=401, text="Authentication required")
        else:
            username = "anonymous"
        
        # Get parameters from query string
        dataset = request.query.get('dataset')
        snapshot = request.query.get('snapshot')
        from_snapshot = request.query.get('from_snapshot')  # For incremental
        compression = request.query.get('compression', 'none')  # none, gzip, zstd
        raw = request.query.get('raw', 'auto')  # true, false, auto
        compressed = request.query.get('compressed', 'auto')  # true, false, auto
        
        if not dataset or not snapshot:
            return web.Response(status=400, text="dataset and snapshot parameters required")
        
        full_snapshot = f"{dataset}@{snapshot}"
        
        # Verify snapshot exists
        check_cmd = ["zfs", "list", "-t", "snapshot", full_snapshot]
        check_result = await asyncio.create_subprocess_exec(
            *check_cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
        await check_result.wait()
        
        if check_result.returncode != 0:
            return web.Response(status=404, text=f"Snapshot {full_snapshot} not found")
        
        # If incremental, verify from_snapshot exists
        if from_snapshot:
            full_from_snapshot = f"{dataset}@{from_snapshot}"
            check_cmd = ["zfs", "list", "-t", "snapshot", full_from_snapshot]
            check_result = await asyncio.create_subprocess_exec(
                *check_cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL
            )
            await check_result.wait()
            
            if check_result.returncode != 0:
                return web.Response(status=404, text=f"From snapshot {full_from_snapshot} not found")
        
        # Build ZFS send command
        zfs_cmd = ["zfs", "send"]
        
        # Auto-detect raw flag if needed
        if raw == 'auto':
            keystatus_result = await asyncio.create_subprocess_exec(
                "zfs", "get", "-H", "-o", "value", "keystatus", dataset,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL
            )
            keystatus_output, _ = await keystatus_result.communicate()
            keystatus = keystatus_output.decode('utf-8').strip()
            use_raw = keystatus in ["available", "unavailable"]
        else:
            use_raw = raw == 'true'
        
        if use_raw:
            zfs_cmd.append("-w")  # Send raw encrypted streams
            
        # Auto-detect compressed flag if needed  
        if compressed == 'auto':
            compression_result = await asyncio.create_subprocess_exec(
                "zfs", "get", "-H", "-o", "value", "compression", dataset,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL
            )
            compression_output, _ = await compression_result.communicate()
            compression_prop = compression_output.decode('utf-8').strip()
            use_compressed = compression_prop not in ["off", ""]
        else:
            use_compressed = compressed == 'true'
            
        if use_compressed:
            zfs_cmd.append("-c")  # Send compressed blocks
        
        # Add incremental flag if needed
        if from_snapshot:
            zfs_cmd.extend(["-i", full_from_snapshot])
            
        zfs_cmd.append(full_snapshot)
        
        # Determine output filename
        base_name = f"{dataset.replace('/', '_')}_{snapshot}"
        if from_snapshot:
            base_name += f"_from_{from_snapshot}"
            
        if compression == "gzip":
            filename = f"{base_name}.zfs.gz"
            content_type = "application/gzip"
        elif compression == "zstd":
            filename = f"{base_name}.zfs.zst"
            content_type = "application/zstd"
        else:
            filename = f"{base_name}.zfs"
            content_type = "application/octet-stream"
        
        # Set response headers
        headers = {
            'Content-Disposition': f'attachment; filename="{filename}"',
            'Content-Type': content_type,
        }
        
        # Create streaming response
        response = web.StreamResponse(
            status=200,
            headers=headers
        )
        await response.prepare(request)
        
        logger.info(f"Starting download of {full_snapshot} with {compression} compression")
        if from_snapshot:
            logger.info(f"  Incremental from {from_snapshot}")
        
        # Create the pipeline
        if compression != "none":
            # Create pipe for zfs -> compressor pipeline
            import os
            read_fd, write_fd = os.pipe()
            
            # Start ZFS send process
            zfs_proc = await asyncio.create_subprocess_exec(
                *zfs_cmd,
                stdout=write_fd,
                stderr=asyncio.subprocess.PIPE
            )
            
            # Close write end in parent
            os.close(write_fd)
            
            # Start compressor process
            if compression == "gzip":
                compress_cmd = ["gzip", "-c", "-6"]  # Level 6 for good balance
            else:  # zstd
                compress_cmd = ["zstd", "-c", "-3"]  # Level 3 for good balance
                
            compress_proc = await asyncio.create_subprocess_exec(
                *compress_cmd,
                stdin=read_fd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            # Close read end in parent
            os.close(read_fd)
            
            # Stream compressed data to client
            bytes_sent = 0
            start_time = time.time()
            
            while True:
                chunk = await compress_proc.stdout.read(1024 * 1024)  # 1MB chunks
                if not chunk:
                    break
                await response.write(chunk)
                bytes_sent += len(chunk)
                
                # Log progress every 100MB
                if bytes_sent % (100 * 1024 * 1024) == 0:
                    elapsed = time.time() - start_time
                    rate = bytes_sent / elapsed / (1024 * 1024)
                    logger.info(f"Download progress: {bytes_sent / (1024*1024):.1f} MB @ {rate:.1f} MB/s")
            
            # Wait for processes to complete
            await compress_proc.wait()
            await zfs_proc.wait()
            
            if zfs_proc.returncode != 0:
                zfs_stderr = await zfs_proc.stderr.read()
                logger.error(f"ZFS send error: {zfs_stderr.decode('utf-8')}")
                
        else:
            # Direct streaming without compression
            zfs_proc = await asyncio.create_subprocess_exec(
                *zfs_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            # Stream data directly to client
            bytes_sent = 0
            start_time = time.time()
            
            while True:
                chunk = await zfs_proc.stdout.read(1024 * 1024)  # 1MB chunks
                if not chunk:
                    break
                await response.write(chunk)
                bytes_sent += len(chunk)
                
                # Log progress every 100MB
                if bytes_sent % (100 * 1024 * 1024) == 0:
                    elapsed = time.time() - start_time
                    rate = bytes_sent / elapsed / (1024 * 1024)
                    logger.info(f"Download progress: {bytes_sent / (1024*1024):.1f} MB @ {rate:.1f} MB/s")
            
            await zfs_proc.wait()
            
            if zfs_proc.returncode != 0:
                zfs_stderr = await zfs_proc.stderr.read()
                logger.error(f"ZFS send error: {zfs_stderr.decode('utf-8')}")
        
        await response.write_eof()
        
        duration = time.time() - start_time
        rate_mbps = bytes_sent / duration / (1024 * 1024) if duration > 0 else 0
        logger.info(f"Download completed: {bytes_sent / (1024*1024):.1f} MB sent @ {rate_mbps:.1f} MB/s")
        
        return response
        
    except Exception as e:
        logger.exception("Download handler error")
        return web.Response(status=500, text=str(e))


def create_app():
    """Create the aiohttp application"""
    app = web.Application()
    
    # Add routes
    app.router.add_post("/", handle_jsonrpc)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/metrics", handle_metrics)
    
    # Streaming routes - DEPRECATED AND MARKED FOR DELETION
    # TODO: Remove these commented routes after migration period
    # Use /upload/receive and /download/send instead for better performance
    # app.router.add_get("/stream/{stream_id}", handle_stream_send)
    # app.router.add_post("/stream/{stream_id}", handle_stream_receive)
    
    # File upload/download routes for web interface
    app.router.add_post("/upload/receive", handle_upload_receive)
    app.router.add_get("/download/send", handle_download_send)
    
    # Add migration API if task manager is available
    if task_manager:
        try:
            import migration_api_aiohttp
            migration_api_aiohttp.task_manager = task_manager
            migration_api_aiohttp.setup_migration_routes(app)
            logger.info("Migration API routes added")
        except Exception as e:
            logger.error(f"Failed to add migration API routes: {e}", exc_info=True)
    
    return app

# async def start_tcp_server(host='0.0.0.0', port=8546):
#     """Start the TCP server for direct ZFS operations"""
#     tcp_handler = ZFSTCPHandler()
#     server = await asyncio.start_server(
#         tcp_handler.handle_client,
#         host,
#         port
#     )
#     
#     addr = server.sockets[0].getsockname()
#     logger.info(f"ZFS TCP server listening on {addr[0]}:{addr[1]}")
#     
#     async with server:
#         await server.serve_forever()

# Legacy socket server functions - replaced by token-authenticated versions
# def start_socket_server_thread(host='0.0.0.0', port=8547):
#     """Start socketserver in a separate thread"""
#     import threading
#     thread = threading.Thread(
#         target=run_socketserver,
#         args=(host, port),
#         daemon=True
#     )
#     thread.start()
#     logger.info(f"ZFS socketserver started in thread on {host}:{port}")
#
# def start_tcp_socket_server(host, port):
#     """Start TCP socket server using socketserver module"""
#     global tcp_socket_server
#     try:
#         # Use ForkingMixIn for better performance with multiple cores
#         tcp_socket_server = ZFSTCPSocketServer(host, port, use_forking=True)
#         tcp_socket_server.start()
#         logger.info(f"TCP socket server started on {host}:{port} (using ForkingMixIn)")
#     except Exception as e:
#         logger.error(f"Failed to start TCP socket server: {e}", exc_info=True)
#
# def start_unix_socket_server():
#     """Start Unix socket server using socketserver module"""
#     global unix_socket_server
#     try:
#         # Using local directory for testing
#         socket_path = "./zfs_unix_socket"
#         unix_socket_server = ZFSUnixSocketServer(socket_path)
#         unix_socket_server.start()
#         
#         # Verify socket exists
#         time.sleep(0.1)
#         if os.path.exists(socket_path):
#             logger.info(f"Unix socket server verified at {socket_path}")
#         else:
#             logger.error(f"Unix socket file not found after startup at {socket_path}")
#     except Exception as e:
#         logger.error(f"Failed to start Unix socket server: {e}", exc_info=True)

def start_token_tcp_socket_server(host, port, token_manager):
    """Start token-authenticated TCP socket server"""
    from zfs_token_socketserver import TokenAuthenticatedTCPServer, TokenAuthenticatedHandler
    import threading
    
    def run_server():
        try:
            server = TokenAuthenticatedTCPServer(
                (host, port),
                TokenAuthenticatedHandler,
                token_manager
            )
            logger.info(f"Token-authenticated TCP socket server started on {host}:{port}")
            server.serve_forever()
        except Exception as e:
            logger.error(f"Token TCP server error: {e}")
    
    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()

def start_token_unix_socket_server(socket_path, token_manager):
    """Start token-authenticated Unix socket server"""
    from zfs_token_socketserver import TokenAuthenticatedUnixServer, TokenAuthenticatedHandler
    import threading
    
    def run_server():
        try:
            server = TokenAuthenticatedUnixServer(
                socket_path,
                TokenAuthenticatedHandler,
                token_manager
            )
            logger.info(f"Token-authenticated Unix socket server started at {socket_path}")
            server.serve_forever()
        except Exception as e:
            logger.error(f"Token Unix server error: {e}")
    
    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()

async def main_async():
    """Async main to run both servers"""
    # Set up logging
    if "logging" in config.config:
        log_config = config.config["logging"]
        logging.getLogger().setLevel(getattr(logging, log_config.get("level", "INFO")))

        if "file" in log_config:
            handler = logging.FileHandler(log_config["file"])
            handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
            logging.getLogger().addHandler(handler)

    # Get server configuration
    server_config = config.config.get("server", {})
    http_config = server_config.get("http", {})
    tcp_config = server_config.get("tcp", {})
    unix_config = server_config.get("unix", {})
    
    # Initialize async Redis client
    global token_manager, redis_client
    redis_client = None
    try:
        redis_config = config.config.get("redis", {"host": "192.168.18.25", "port": 6379})
        redis_client = AsyncReliableRedis(
            host=redis_config.get("host", "192.168.18.25"),
            port=redis_config.get("port", 6379),
            password=redis_config.get("password"),
            db=redis_config.get("db", 0),
            max_retries=5,
            raise_on_failure=False,  # Don't crash the server if Redis is unavailable
            decode_responses=False
        )
        await redis_client.connect()
        
        # Create async token manager using the Redis client
        token_manager = AsyncTokenManager(redis_client)
        logger.info(f"Token authentication initialized with Redis at {redis_config.get('host')}:{redis_config.get('port')}")
        
        # Create a separate sync token manager for socket servers
        # Socket servers run in threads and need their own Redis connection
        from redis_token_manager import TokenManager as SyncTokenManager
        sync_token_manager = SyncTokenManager(
            redis_host=redis_config.get("host", "192.168.18.25"),
            redis_port=redis_config.get("port", 6379),
            redis_db=redis_config.get("db", 0),
            password=redis_config.get("password"),
            max_retries=5,
            raise_on_failure=False
        )
        
        if tcp_config.get("enabled", True):
            tcp_host = tcp_config.get("host", "0.0.0.0")
            tcp_port = tcp_config.get("port", 9999)
            start_token_tcp_socket_server(tcp_host, tcp_port, sync_token_manager)
            logger.info(f"Token-authenticated TCP socket server started on {tcp_host}:{tcp_port}")
        
        if unix_config.get("enabled", True):
            unix_path = unix_config.get("path", "./zfs_token_socket")
            start_token_unix_socket_server(unix_path, sync_token_manager)
            logger.info(f"Token-authenticated Unix socket server started at {unix_path}")
        
    except Exception as e:
        logger.error(f"Failed to initialize token manager: {e}")
        logger.info("Token authentication will be disabled")
    
    # Initialize task manager for background operations
    global task_manager
    if token_manager:
        try:
            task_config = config.config.get("task_manager", {})
            migration_workers = task_config.get("migration_workers", 4)
            task_manager = AsyncBackgroundTaskManager(redis_client, max_workers=migration_workers)
            # Register migration handler
            from async_background_task_manager import handle_migration_task
            task_manager.register_handler('migration', handle_migration_task)
            await task_manager.start()
            logger.info(f"Background task manager started with {migration_workers} migration workers")
        except Exception as e:
            logger.error(f"Failed to initialize task manager: {e}")
            logger.info("Background tasks will be disabled")
    
    # Get HTTP server config
    http_host = http_config.get("host", "0.0.0.0")
    http_port = http_config.get("port", 8545)
    
    logger.info(f"Starting ZFS API server on {http_host}:{http_port}")
    logger.info(f"Authentication: {'enabled' if config.config['auth']['enabled'] else 'disabled'}")
    logger.info(f"Rate limiting: {'enabled' if config.config['rate_limit']['enabled'] else 'disabled'}")
    logger.info(f"Token authentication: {'enabled' if token_manager else 'disabled'}")
    
    # Create and run HTTP app
    app = create_app()
    logger.info("Created aiohttp application")
    
    # Create runner manually to handle both servers
    runner = web.AppRunner(app)
    await runner.setup()
    logger.info("AppRunner setup completed")
    
    try:
        site = web.TCPSite(runner, http_host, http_port)
        await site.start()
        logger.info(f"HTTP server started on {http_host}:{http_port}")
    except Exception as e:
        logger.error(f"Failed to start HTTP server: {e}", exc_info=True)
        raise
    
    try:
        # DISABLED for debugging - just keep the event loop running
        # await asyncio.gather(tcp_task)
        await asyncio.Event().wait()  # Wait forever
    except KeyboardInterrupt:
        pass
    finally:
        # Cleanup task manager if it exists
        if task_manager:
            await task_manager.stop()
            logger.info("Background task manager stopped")
        # Close Redis connection
        if redis_client:
            await redis_client.close()
            logger.info("Redis connection closed")
        await runner.cleanup()

def main():
    """Main entry point"""
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Servers shutdown requested")
    except Exception as e:
        logger.exception("Server error")

if __name__ == "__main__":
    # Set socket buffer sizes via environment if needed
    import os
    os.environ.setdefault('PYTHONUNBUFFERED', '1')
    main()