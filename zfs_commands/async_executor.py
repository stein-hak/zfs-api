"""
Async ZFS Executor - For use with asyncio event loops
Use this in: zfs_api_server.py, async handlers
"""
import asyncio
from typing import List, Optional, Dict, Tuple
from .builder import ZFSCommands
from .types import CommandResult


class AsyncZFS:
    """
    Async ZFS executor for asyncio-based applications.
    All methods use native async subprocess execution.
    """

    def __init__(self):
        self.commands = ZFSCommands()

    async def _execute(self, cmd: List[str]) -> CommandResult:
        """Execute command and return standardized result"""
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()

        return CommandResult(
            returncode=proc.returncode,
            stdout=stdout.decode('utf-8', errors='ignore'),
            stderr=stderr.decode('utf-8', errors='ignore')
        )

    # ==================== DATASET OPERATIONS ====================

    async def dataset_create(self, dataset: str,
                           properties: Optional[Dict[str, str]] = None) -> CommandResult:
        """Create a ZFS dataset"""
        cmd = self.commands.dataset_create(dataset, properties)
        return await self._execute(cmd)

    async def dataset_destroy(self, dataset: str, recursive: bool = False) -> CommandResult:
        """Destroy a ZFS dataset"""
        cmd = self.commands.dataset_destroy(dataset, recursive)
        return await self._execute(cmd)

    async def dataset_list(self, dataset: Optional[str] = None) -> List[str]:
        """List ZFS datasets, returns list of dataset names"""
        cmd = self.commands.dataset_list(dataset)
        result = await self._execute(cmd)
        if result.success:
            return [line.split('\t')[0] for line in result.stdout.splitlines() if line.strip()]
        return []

    async def dataset_get_properties(self, dataset: str,
                                    property: str = "all") -> Dict[str, str]:
        """Get dataset properties, returns dict of property: value"""
        cmd = self.commands.dataset_get_properties(dataset, property)
        result = await self._execute(cmd)
        if result.success:
            props = {}
            for line in result.stdout.splitlines():
                parts = line.split('\t')
                if len(parts) >= 3:
                    props[parts[1]] = parts[2]
            return props
        return {}

    async def dataset_set_property(self, dataset: str,
                                  property: str, value: str) -> CommandResult:
        """Set dataset property"""
        cmd = self.commands.dataset_set_property(dataset, property, value)
        return await self._execute(cmd)

    async def dataset_get_space(self, dataset: str) -> Dict[str, int]:
        """Get space usage information"""
        cmd = self.commands.dataset_get_space(dataset)
        result = await self._execute(cmd)
        space = {'name': dataset, 'avail': 0, 'used': 0, 'usedsnap': 0,
                'useddss': 0, 'usedrefreserv': 0, 'usedchild': 0}

        if result.success:
            parts = result.stdout.strip().split('\t')
            if len(parts) >= 7:
                space['name'] = parts[0]
                space['avail'] = int(parts[1])
                space['used'] = int(parts[2])
                space['usedsnap'] = int(parts[3])
                space['useddss'] = int(parts[4])
                space['usedrefreserv'] = int(parts[5])
                space['usedchild'] = int(parts[6].strip())

        return space

    async def dataset_mount(self, dataset: str) -> CommandResult:
        """Mount a ZFS dataset"""
        cmd = self.commands.dataset_mount(dataset)
        return await self._execute(cmd)

    async def dataset_rename(self, old_name: str, new_name: str) -> CommandResult:
        """Rename a ZFS dataset"""
        cmd = self.commands.dataset_rename(old_name, new_name)
        return await self._execute(cmd)

    async def dataset_promote(self, dataset: str) -> CommandResult:
        """Promote a clone dataset"""
        cmd = self.commands.dataset_promote(dataset)
        return await self._execute(cmd)

    async def dataset_share(self, dataset: str) -> CommandResult:
        """Share dataset via NFS"""
        cmd = self.commands.dataset_share(dataset)
        return await self._execute(cmd)

    async def dataset_unshare(self, dataset: str) -> CommandResult:
        """Unshare dataset"""
        cmd = self.commands.dataset_unshare(dataset)
        return await self._execute(cmd)

    # ==================== SNAPSHOT OPERATIONS ====================

    async def snapshot_create(self, dataset: str, name: str,
                            recursive: bool = False) -> CommandResult:
        """Create a snapshot"""
        cmd = self.commands.snapshot_create(dataset, name, recursive)
        return await self._execute(cmd)

    async def snapshot_create_auto(self, dataset: str, tag: str,
                                  tag1: Optional[str] = None,
                                  recursive: bool = False) -> Tuple[CommandResult, str]:
        """Create auto-named snapshot with timestamp"""
        from datetime import datetime
        now = datetime.utcnow()
        name = tag
        if tag1:
            name += f'_{tag1}'
        name += f'_{now.strftime("%Y-%m-%d-%H-%M")}'

        result = await self.snapshot_create(dataset, name, recursive)
        return result, name

    async def snapshot_list(self, dataset: str) -> List[str]:
        """List snapshots for dataset, returns snapshot names"""
        cmd = self.commands.snapshot_list(dataset)
        result = await self._execute(cmd)
        if result.success:
            snapshots = []
            for line in result.stdout.splitlines():
                if '@' in line and line.split('@')[0] == dataset:
                    snapshots.append(line.split('@')[1])
            return snapshots
        return []

    async def snapshot_destroy(self, dataset: str, snapshot: str,
                              recursive: bool = False) -> CommandResult:
        """Destroy a snapshot"""
        cmd = self.commands.snapshot_destroy(dataset, snapshot, recursive)
        return await self._execute(cmd)

    async def snapshot_rollback(self, dataset: str, snapshot: str) -> CommandResult:
        """Rollback dataset to snapshot"""
        cmd = self.commands.snapshot_rollback(dataset, snapshot)
        return await self._execute(cmd)

    async def snapshot_hold(self, dataset: str, snapshot: str, tag: str,
                          recursive: bool = False) -> CommandResult:
        """Place a hold on snapshot"""
        cmd = self.commands.snapshot_hold(dataset, snapshot, tag, recursive)
        return await self._execute(cmd)

    async def snapshot_release(self, dataset: str, snapshot: str, tag: str,
                              recursive: bool = False) -> CommandResult:
        """Release a hold on snapshot"""
        cmd = self.commands.snapshot_release(dataset, snapshot, tag, recursive)
        return await self._execute(cmd)

    async def snapshot_list_holds(self, dataset: str, snapshot: str,
                                 recursive: bool = False) -> List[str]:
        """List holds on snapshot"""
        cmd = self.commands.snapshot_list_holds(dataset, snapshot, recursive)
        result = await self._execute(cmd)
        if result.success:
            holds = []
            for line in sorted(result.stdout.splitlines(), reverse=True):
                parts = line.split('\t')
                if len(parts) >= 2:
                    holds.append(parts[1])
            return holds
        return []

    async def snapshot_diff(self, snapshot1: str,
                          snapshot2: Optional[str] = None) -> Tuple[List, List, List, List]:
        """
        Compare snapshot differences.
        Returns (new, modified, deleted, renamed) tuples
        """
        cmd = self.commands.snapshot_diff(snapshot1, snapshot2)
        result = await self._execute(cmd)

        new, modified, deleted, renamed = [], [], [], []

        if result.success:
            for line in result.stdout.splitlines():
                args = line.split('\t')
                if len(args) < 2:
                    continue

                if args[0] == '+':
                    new.append((args[2], args[1]))
                elif args[0] == '-':
                    deleted.append((args[2], args[1]))
                elif args[0] == 'M':
                    modified.append((args[2], args[1]))
                elif args[0] == 'R' and len(args) >= 4:
                    renamed.append((args[2], args[3], args[1]))

        return new, modified, deleted, renamed

    # ==================== POOL OPERATIONS ====================

    async def pool_list(self) -> List[str]:
        """List ZFS pools"""
        cmd = self.commands.pool_list()
        result = await self._execute(cmd)
        if result.success:
            return [line.strip() for line in result.stdout.splitlines() if line.strip()]
        return []

    async def pool_get_properties(self, pool: str,
                                  property: str = "all") -> Dict[str, str]:
        """Get pool properties"""
        cmd = self.commands.pool_get_properties(pool, property)
        result = await self._execute(cmd)
        if result.success:
            props = {}
            for line in result.stdout.splitlines():
                parts = line.split('\t')
                if len(parts) >= 3:
                    props[parts[1]] = parts[2]
            return props
        return {}

    async def pool_set_property(self, pool: str,
                               property: str, value: str) -> CommandResult:
        """Set pool property"""
        cmd = self.commands.pool_set_property(pool, property, value)
        return await self._execute(cmd)

    async def pool_scrub_start(self, pool: str) -> CommandResult:
        """Start pool scrub"""
        cmd = self.commands.pool_scrub_start(pool)
        return await self._execute(cmd)

    async def pool_scrub_stop(self, pool: str) -> CommandResult:
        """Stop pool scrub"""
        cmd = self.commands.pool_scrub_stop(pool)
        return await self._execute(cmd)

    async def pool_status(self, pool: str, verbose: bool = True) -> CommandResult:
        """Get pool status"""
        cmd = self.commands.pool_status(pool, verbose)
        return await self._execute(cmd)

    async def pool_import(self, pool: Optional[str] = None,
                         force: bool = False,
                         mount: bool = True,
                         persist: str = 'id') -> CommandResult:
        """Import pool"""
        cmd = self.commands.pool_import(pool, force, mount, persist)
        return await self._execute(cmd)

    async def pool_export(self, pool: str, force: bool = False) -> CommandResult:
        """Export pool"""
        cmd = self.commands.pool_export(pool, force)
        return await self._execute(cmd)

    # ==================== BOOKMARK OPERATIONS ====================

    async def bookmark_create(self, snapshot: str, bookmark: str) -> CommandResult:
        """Create a bookmark from snapshot"""
        cmd = self.commands.bookmark_create(snapshot, bookmark)
        return await self._execute(cmd)

    async def bookmark_list(self, dataset: str) -> List[str]:
        """List bookmarks for dataset"""
        cmd = self.commands.bookmark_list(dataset)
        result = await self._execute(cmd)
        if result.success:
            return [line.split('\t')[0] for line in result.stdout.splitlines() if line.strip()]
        return []

    async def bookmark_destroy(self, bookmark: str) -> CommandResult:
        """Destroy a bookmark"""
        cmd = self.commands.bookmark_destroy(bookmark)
        return await self._execute(cmd)

    # ==================== CLONE OPERATIONS ====================

    async def clone_create(self, snapshot: str, target: str,
                          properties: Optional[Dict[str, str]] = None) -> CommandResult:
        """Create a clone from snapshot"""
        cmd = self.commands.clone_create(snapshot, target, properties)
        return await self._execute(cmd)

    # ==================== VOLUME OPERATIONS ====================

    async def volume_create(self, dataset: str,
                          size_gb: Optional[int] = None,
                          size_bytes: Optional[int] = None,
                          compression: str = 'lz4',
                          volblocksize: str = '8K',
                          sparse: bool = True) -> CommandResult:
        """Create a ZFS volume (zvol)"""
        cmd = self.commands.volume_create(dataset, size_gb, size_bytes,
                                         compression, volblocksize, sparse)
        return await self._execute(cmd)

    async def volume_list(self) -> List[str]:
        """List ZFS volumes"""
        cmd = self.commands.volume_list()
        result = await self._execute(cmd)
        if result.success:
            return [line.split('\t')[0] for line in result.stdout.splitlines() if line.strip()]
        return []

    async def volume_destroy(self, dataset: str) -> CommandResult:
        """Destroy a ZFS volume"""
        cmd = self.commands.volume_destroy(dataset)
        return await self._execute(cmd)

    # ==================== SEND/RECEIVE OPERATIONS ====================

    async def send_snapshot_stream(self, dataset: str, snapshot: str,
                                  from_snapshot: Optional[str] = None,
                                  recursive: bool = True,
                                  raw: bool = False,
                                  compressed: bool = False,
                                  resume_token: Optional[str] = None) -> asyncio.subprocess.Process:
        """
        Send snapshot as stream (returns process for piping).
        Use this for streaming operations.
        """
        cmd = self.commands.send_snapshot(dataset, snapshot, from_snapshot,
                                         recursive, raw, compressed, resume_token)
        return await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

    async def send_estimate(self, dataset: str, snapshot: str,
                          from_snapshot: Optional[str] = None,
                          recursive: bool = True,
                          raw: bool = False,
                          compressed: bool = False) -> Optional[int]:
        """Estimate send size in bytes"""
        cmd = self.commands.send_estimate(dataset, snapshot, from_snapshot,
                                         recursive, raw, compressed)
        result = await self._execute(cmd)
        if result.success:
            # Parse size from last line
            lines = result.stdout.splitlines()
            if lines:
                last_line = lines[-1]
                # Format: "size	1.23G"
                parts = last_line.split()
                if len(parts) >= 2:
                    size_str = parts[-1]
                    return self._parse_size(size_str)
        return None

    async def receive_snapshot_stream(self, dataset: str,
                                     force: bool = True,
                                     resumable: bool = False) -> asyncio.subprocess.Process:
        """
        Receive snapshot from stream (returns process for piping).
        Use this for streaming operations.
        """
        cmd = self.commands.receive_snapshot(dataset, force, resumable)
        return await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

    # ==================== DIAGNOSTIC OPERATIONS ====================

    async def check_dataset_exists(self, dataset: str) -> bool:
        """Check if dataset exists"""
        cmd = self.commands.check_dataset_exists(dataset)
        result = await self._execute(cmd)
        return result.success

    async def check_snapshot_exists(self, snapshot: str) -> bool:
        """Check if snapshot exists"""
        cmd = self.commands.check_snapshot_exists(snapshot)
        result = await self._execute(cmd)
        return result.success

    async def get_version(self) -> Optional[str]:
        """Get ZFS version"""
        cmd = self.commands.get_version()
        result = await self._execute(cmd)
        if result.success:
            return result.stdout.strip()
        return None

    async def get_pool_state(self, pool: str) -> CommandResult:
        """Get pool state"""
        cmd = self.commands.get_pool_state(pool)
        return await self._execute(cmd)

    async def get_operation_progress(self, pool: str) -> CommandResult:
        """Get operation progress (scrub/resilver)"""
        cmd = self.commands.get_operation_progress(pool)
        return await self._execute(cmd)

    # ==================== HELPER METHODS ====================

    @staticmethod
    def _parse_size(size_str: str) -> int:
        """Parse ZFS size string to bytes"""
        size_str = size_str.replace(',', '.')
        if size_str[-1] == 'K':
            return int(float(size_str[:-1]) * 1024)
        elif size_str[-1] == 'M':
            return int(float(size_str[:-1]) * 1024 * 1024)
        elif size_str[-1] == 'G':
            return int(float(size_str[:-1]) * 1024 * 1024 * 1024)
        elif size_str[-1] == 'T':
            return int(float(size_str[:-1]) * 1024 * 1024 * 1024 * 1024)
        else:
            return int(size_str)
