"""
ZFS Commands Package - Clean architecture for ZFS operations

This package provides a unified interface for ZFS command execution with:
- Single source of truth for command construction (builder.py)
- Async executor for asyncio applications (AsyncZFS)
- Sync executor for scripts and CLI tools (SyncZFS)
- Consistent error handling (CommandResult)

Usage:
    # For async applications (zfs_api_server.py)
    from zfs_commands import AsyncZFS
    zfs = AsyncZFS()
    result = await zfs.dataset_create('tank/data')

    # For sync applications (standalone_migrate.py, scripts)
    from zfs_commands import SyncZFS
    zfs = SyncZFS()
    result = zfs.dataset_create('tank/data')

    # Both have identical APIs, just sync vs async execution
"""

from .async_executor import AsyncZFS
from .sync_executor import SyncZFS
from .builder import ZFSCommands
from .types import CommandResult, ZFSCommandError

__all__ = [
    'AsyncZFS',
    'SyncZFS',
    'ZFSCommands',
    'CommandResult',
    'ZFSCommandError',
]

__version__ = '1.0.0'
