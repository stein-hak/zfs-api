"""
ZFS Command Builder - Pure command construction
Single source of truth for all ZFS command syntax
"""
from typing import List, Optional, Dict


class ZFSCommands:
    """
    Pure command builder with no execution logic.
    All methods are static and return command arrays ready for execution.
    """

    # ==================== DATASET OPERATIONS ====================

    @staticmethod
    def dataset_create(dataset: str, properties: Optional[Dict[str, str]] = None) -> List[str]:
        """Build zfs create command"""
        cmd = ['zfs', 'create']
        if properties:
            for key, value in properties.items():
                cmd.extend(['-o', f'{key}={value}'])
        cmd.append(dataset)
        return cmd

    @staticmethod
    def dataset_destroy(dataset: str, recursive: bool = False) -> List[str]:
        """Build zfs destroy command"""
        cmd = ['zfs', 'destroy']
        if recursive:
            cmd.append('-R')
        cmd.append(dataset)
        return cmd

    @staticmethod
    def dataset_list(dataset: Optional[str] = None) -> List[str]:
        """Build zfs list command"""
        cmd = ['zfs', 'list', '-H']
        if dataset:
            cmd.extend(['-r', dataset])
        return cmd

    @staticmethod
    def dataset_get_properties(dataset: str, property: str = "all") -> List[str]:
        """Build zfs get command"""
        return ['zfs', 'get', '-H', property, dataset]

    @staticmethod
    def dataset_set_property(dataset: str, property: str, value: str) -> List[str]:
        """Build zfs set command"""
        return ['zfs', 'set', f'{property}={value}', dataset]

    @staticmethod
    def dataset_get_space(dataset: str) -> List[str]:
        """Build command to get space usage"""
        return ['zfs', 'list', '-H', '-p', '-o', 'space', dataset]

    @staticmethod
    def dataset_mount(dataset: str) -> List[str]:
        """Build zfs mount command"""
        return ['zfs', 'mount', dataset]

    @staticmethod
    def dataset_rename(old_name: str, new_name: str) -> List[str]:
        """Build zfs rename command"""
        return ['zfs', 'rename', old_name, new_name]

    @staticmethod
    def dataset_promote(dataset: str) -> List[str]:
        """Build zfs promote command"""
        return ['zfs', 'promote', dataset]

    @staticmethod
    def dataset_share(dataset: str) -> List[str]:
        """Build zfs share command"""
        return ['zfs', 'share', dataset]

    @staticmethod
    def dataset_unshare(dataset: str) -> List[str]:
        """Build zfs unshare command"""
        return ['zfs', 'unshare', dataset]

    # ==================== SNAPSHOT OPERATIONS ====================

    @staticmethod
    def snapshot_create(dataset: str, name: str, recursive: bool = False) -> List[str]:
        """Build zfs snapshot command"""
        cmd = ['zfs', 'snapshot']
        if recursive:
            cmd.append('-r')
        cmd.append(f'{dataset}@{name}')
        return cmd

    @staticmethod
    def snapshot_list(dataset: str) -> List[str]:
        """Build command to list snapshots"""
        return ['zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name']

    @staticmethod
    def snapshot_destroy(dataset: str, snapshot: str, recursive: bool = False) -> List[str]:
        """Build zfs destroy command for snapshot"""
        cmd = ['zfs', 'destroy']
        if recursive:
            cmd.append('-R')
        cmd.append(f'{dataset}@{snapshot}')
        return cmd

    @staticmethod
    def snapshot_rollback(dataset: str, snapshot: str) -> List[str]:
        """Build zfs rollback command"""
        return ['zfs', 'rollback', '-r', f'{dataset}@{snapshot}']

    @staticmethod
    def snapshot_hold(dataset: str, snapshot: str, tag: str, recursive: bool = False) -> List[str]:
        """Build zfs hold command"""
        cmd = ['zfs', 'hold']
        if recursive:
            cmd.append('-r')
        cmd.extend([tag, f'{dataset}@{snapshot}'])
        return cmd

    @staticmethod
    def snapshot_release(dataset: str, snapshot: str, tag: str, recursive: bool = False) -> List[str]:
        """Build zfs release command"""
        cmd = ['zfs', 'release']
        if recursive:
            cmd.append('-r')
        cmd.extend([tag, f'{dataset}@{snapshot}'])
        return cmd

    @staticmethod
    def snapshot_list_holds(dataset: str, snapshot: str, recursive: bool = False) -> List[str]:
        """Build zfs holds command"""
        cmd = ['zfs', 'holds', '-H']
        if recursive:
            cmd.append('-r')
        cmd.append(f'{dataset}@{snapshot}')
        return cmd

    @staticmethod
    def snapshot_diff(snapshot1: str, snapshot2: Optional[str] = None) -> List[str]:
        """Build zfs diff command"""
        cmd = ['zfs', 'diff', '-HF', snapshot1]
        if snapshot2:
            cmd.append(snapshot2)
        return cmd

    # ==================== POOL OPERATIONS ====================

    @staticmethod
    def pool_list() -> List[str]:
        """Build zpool list command"""
        return ['zpool', 'list', '-H', '-o', 'name']

    @staticmethod
    def pool_get_properties(pool: str, property: str = "all") -> List[str]:
        """Build zpool get command"""
        return ['zpool', 'get', '-H', '-p', property, pool]

    @staticmethod
    def pool_set_property(pool: str, property: str, value: str) -> List[str]:
        """Build zpool set command"""
        return ['zpool', 'set', f'{property}={value}', pool]

    @staticmethod
    def pool_scrub_start(pool: str) -> List[str]:
        """Build zpool scrub command"""
        return ['zpool', 'scrub', pool]

    @staticmethod
    def pool_scrub_stop(pool: str) -> List[str]:
        """Build zpool scrub stop command"""
        return ['zpool', 'scrub', '-s', pool]

    @staticmethod
    def pool_status(pool: str, verbose: bool = True) -> List[str]:
        """Build zpool status command"""
        cmd = ['zpool', 'status']
        if verbose:
            cmd.append('-v')
        cmd.append(pool)
        return cmd

    @staticmethod
    def pool_import(pool: Optional[str] = None, force: bool = False,
                   mount: bool = True, persist: str = 'id') -> List[str]:
        """Build zpool import command"""
        cmd = ['zpool', 'import']

        if pool:
            cmd.append(pool)
        else:
            cmd.append('-a')  # Import all pools

        if force:
            cmd.append('-f')

        if not mount:
            cmd.append('-N')

        # Persistent device naming
        if persist == 'path':
            cmd.extend(['-d', '/dev/disk/by-path'])
        elif persist == 'id':
            cmd.extend(['-d', '/dev/disk/by-id'])
        elif persist:
            cmd.extend(['-d', persist])

        return cmd

    @staticmethod
    def pool_export(pool: str, force: bool = False) -> List[str]:
        """Build zpool export command"""
        cmd = ['zpool', 'export']
        if force:
            cmd.append('-f')
        cmd.append(pool)
        return cmd

    # ==================== BOOKMARK OPERATIONS ====================

    @staticmethod
    def bookmark_create(snapshot: str, bookmark: str) -> List[str]:
        """Build zfs bookmark command"""
        return ['zfs', 'bookmark', snapshot, bookmark]

    @staticmethod
    def bookmark_list(dataset: str) -> List[str]:
        """Build command to list bookmarks"""
        return ['zfs', 'list', '-t', 'bookmark', '-H', '-r', dataset]

    @staticmethod
    def bookmark_destroy(bookmark: str) -> List[str]:
        """Build zfs destroy command for bookmark"""
        return ['zfs', 'destroy', bookmark]

    # ==================== CLONE OPERATIONS ====================

    @staticmethod
    def clone_create(snapshot: str, target: str,
                    properties: Optional[Dict[str, str]] = None) -> List[str]:
        """Build zfs clone command"""
        cmd = ['zfs', 'clone']
        if properties:
            for key, value in properties.items():
                cmd.extend(['-o', f'{key}={value}'])
        cmd.extend([snapshot, target])
        return cmd

    # ==================== VOLUME (ZVOL) OPERATIONS ====================

    @staticmethod
    def volume_create(dataset: str,
                     size_gb: Optional[int] = None,
                     size_bytes: Optional[int] = None,
                     compression: str = 'lz4',
                     volblocksize: str = '8K',
                     sparse: bool = True) -> List[str]:
        """Build zfs create command for volume (zvol)"""
        cmd = ['zfs', 'create', '-o', f'compression={compression}', '-b', volblocksize]

        if sparse:
            cmd.append('-s')

        if size_gb:
            cmd.extend(['-V', f'{size_gb}G'])
        elif size_bytes:
            cmd.extend(['-V', str(size_bytes)])
        else:
            raise ValueError("Either size_gb or size_bytes must be specified")

        cmd.append(dataset)
        return cmd

    @staticmethod
    def volume_list() -> List[str]:
        """Build command to list volumes"""
        return ['zfs', 'list', '-t', 'volume', '-H']

    @staticmethod
    def volume_destroy(dataset: str) -> List[str]:
        """Build zfs destroy command for volume"""
        return ['zfs', 'destroy', dataset]

    # ==================== SEND/RECEIVE OPERATIONS ====================

    @staticmethod
    def send_snapshot(dataset: str, snapshot: str,
                     from_snapshot: Optional[str] = None,
                     recursive: bool = True,
                     raw: bool = False,
                     compressed: bool = False,
                     resume_token: Optional[str] = None) -> List[str]:
        """Build zfs send command"""
        cmd = ['zfs', 'send']

        # Resume token takes precedence
        if resume_token:
            cmd.extend(['-t', resume_token])
            return cmd

        # Encryption/compression flags
        if raw:
            cmd.append('-w')
        if compressed:
            cmd.append('-c')
        if recursive:
            cmd.append('-R')

        # Incremental send
        if from_snapshot:
            cmd.extend(['-I', f'{dataset}@{from_snapshot}'])

        cmd.append(f'{dataset}@{snapshot}')
        return cmd

    @staticmethod
    def send_estimate(dataset: str, snapshot: str,
                     from_snapshot: Optional[str] = None,
                     recursive: bool = True,
                     raw: bool = False,
                     compressed: bool = False) -> List[str]:
        """Build zfs send -nv (size estimation) command"""
        cmd = ['zfs', 'send']

        if raw:
            cmd.append('-w')
        if compressed:
            cmd.append('-c')
        if recursive:
            cmd.append('-R')

        cmd.append('-nv')  # Dry-run with verbose

        if from_snapshot:
            cmd.extend(['-I', f'{dataset}@{from_snapshot}'])

        cmd.append(f'{dataset}@{snapshot}')
        return cmd

    @staticmethod
    def receive_snapshot(dataset: str,
                        force: bool = True,
                        resumable: bool = False) -> List[str]:
        """Build zfs receive command"""
        cmd = ['zfs', 'receive']

        if force:
            cmd.append('-F')
        if resumable:
            cmd.append('-s')

        cmd.append(dataset)
        return cmd

    # ==================== DIAGNOSTIC OPERATIONS ====================

    @staticmethod
    def check_dataset_exists(dataset: str) -> List[str]:
        """Build command to check if dataset exists"""
        return ['zfs', 'list', '-H', '-p', dataset]

    @staticmethod
    def check_snapshot_exists(snapshot: str) -> List[str]:
        """Build command to check if snapshot exists"""
        return ['zfs', 'list', '-t', 'snapshot', '-H', snapshot]

    @staticmethod
    def get_version() -> List[str]:
        """Build zfs --version command"""
        return ['zfs', '--version']

    @staticmethod
    def get_pool_state(pool: str) -> List[str]:
        """Build zpool status command for pool state"""
        return ['zpool', 'status', pool]

    @staticmethod
    def get_operation_progress(pool: str) -> List[str]:
        """Build zpool status -v command for operation progress"""
        return ['zpool', 'status', '-v', pool]
