#!/usr/bin/python3
"""
Standalone ZFS migration tool that combines functionality from multiple modules.
This script allows for migrating ZFS datasets between local and remote hosts.
"""

import sys
import os
import argparse
import re
import shutil
import logging
from subprocess import Popen, PIPE
from datetime import datetime
from collections import OrderedDict
from typing import List, Dict, Tuple, Optional, Any, Union
import math
from packaging import version

# Configure default logging initially - will be updated after parsing args
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def format_bytes(bytes_value: int, precision: int = 2) -> str:
    """
    Convert bytes to human-readable string format.
    
    Args:
        bytes_value: Number of bytes to convert
        precision: Number of decimal places for formatting
        
    Returns:
        Formatted string with appropriate unit suffix (B, KB, MB, GB, TB)
    """
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']
    if bytes_value == 0:
        return f"0 {suffixes[0]}"
        
    suffix_index = math.floor(math.log(bytes_value, 1024))
    suffix_index = min(suffix_index, len(suffixes) - 1)  # Cap at largest suffix
    
    value = bytes_value / (1024 ** suffix_index)
    formatted = f"{value:.{precision}f} {suffixes[suffix_index]}"
    
    return formatted

# ====== Execute module ======
def execute(command, shell=False) -> Tuple[str, str, int]:
    """
    Execute a command and return its output.
    
    Args:
        command: List of command and arguments or string if shell=True
        shell: Whether to use shell execution
        
    Returns:
        Tuple of (stdout, stderr, return_code)
    """
    with Popen(command, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=shell) as p:
        output, err = p.communicate()
        rc = p.returncode
    
    return output, err, rc

def execute_pipe(command: Union[List[str], str]) -> Popen:
    """
    Execute a command and return the process object for piping.
    
    Args:
        command: Command string or list of command and arguments
        
    Returns:
        Popen process object
    """
    p = Popen(command, stdin=PIPE, stdout=PIPE, stderr=PIPE, 
              shell=isinstance(command, str), close_fds=True, universal_newlines=True)
    return p

# ====== Compression module ======
def compressor(fd, type='gzip', level=6, shell=False, out_fd=None):
    ret = None
    cmd = []
    if type == 'gzip':
        cmd = ['gzip', '-c']
    elif type == 'lz4':
        level = 1
        cmd = ['lz4c', '-c']
    elif type == 'bzip2':
        cmd = ['bzip2', '-c']
    elif type == 'xz':
        cmd = ['xz', '-c']
    elif type == 'zstd':
        cmd = ['zstd']

    if level and cmd:
        cmd.append('-' + str(level))

    if shell:
        ret = ''
        for i in cmd:
            ret += i + ' '
    else:
        ret = cmd
    
    if ret:
        if not out_fd:
            p = Popen(ret, stdin=fd, stdout=PIPE, shell=shell)
        else:
            p = Popen(ret, stdin=fd, stdout=out_fd, shell=shell)
        
        return p
    else:
        return None

def uncompressor(fd, type='gzip', shell=False, out_fd=None):
    ret = None
    cmd = []
    if type == 'gzip':
        cmd = ['gzip', '-d']
    elif type == 'lz4':
        cmd = ['lz4c', '-d']
    elif type == 'bzip2':
        cmd = ['bzip2', '-d']
    elif type == 'xz':
        cmd = ['xz', '-d']
    elif type == 'zstd':
        cmd = ['zstd', '-d', '-c']

    if shell:
        ret = ''
        for i in cmd:
            ret += i + ' '
    else:
        ret = cmd
        
    if ret:
        if not out_fd:
            p = Popen(ret, bufsize=128 * 1024 * 1024, stdin=fd, stdout=PIPE, shell=shell)
        else:
            p = Popen(ret, bufsize=128 * 1024 * 1024, stdin=fd, stdout=out_fd, shell=shell)
        return p
    else:
        return None

def uncompressor_file(file, type='gzip', shell=False):
    ret = None
    cmd = []
    if os.path.isfile(file):
        if type == 'gzip':
            cmd = ['zcat', '-c', file]
        elif type == 'lz4':
            cmd = ['lz4cat', '-c', file]
        elif type == 'bzip2':
            cmd = ['bzcat', '-c', file]
        elif type == 'xz':
            cmd = ['xzcat', '-c', file]
        elif type == 'zstd':
            cmd = ['xzcat', '-c', file]

        if shell:
            ret = ''
            for i in cmd:
                ret += i + ' '
        else:
            ret = cmd

        if ret:
            p = Popen(ret, bufsize=128 * 1024 * 1024, stdout=PIPE, shell=shell)
            return p
        else:
            return None

def uncompress_ssh_recv(fd, host, dataset, type=None, force=False, use_native_compression=False):
    """
    Send data to remote zfs receive command with decompression.
    
    Args:
        fd: File descriptor with data
        host: Remote host
        dataset: ZFS dataset name
        type: Compression type
        force: Force flag
        
    Returns:
        Process object
    """
    # First check the resume token status without trying to abort
    token_check_cmd = "ssh -oStrictHostKeyChecking=no root@%s 'zfs get -H receive_resume_token %s'" % (host, dataset)
    token_out, _, _ = execute(token_check_cmd, shell=True)
    if token_out:
        parts = token_out.split('\t')
        if len(parts) > 2 and "-" not in parts[2]:
            logger.info("Found existing resume token: %s" % parts[2])
    
    # Check for existing partial receive state
    partial_check_cmd = "ssh -oStrictHostKeyChecking=no root@%s 'zfs list %s/%%recv 2>&1 || echo \"No partial receive dataset\"'" % (host, dataset)
    partial_out, _, _ = execute(partial_check_cmd, shell=True)
    if "No partial receive dataset" not in partial_out:
        logger.info("Found partial receive dataset: %s" % partial_out)
    
    # Only try to abort if we're explicitly asked to do so
    if force:
        abort_cmd = "ssh -oStrictHostKeyChecking=no root@%s 'zfs receive -A %s 2>&1; echo $?'" % (host, dataset)
        abort_out, _, _ = execute(abort_cmd, shell=True)
        if "cannot abort" not in abort_out.lower():
            logger.info("Previous interrupted transfer aborted: %s" % abort_out)
        else:
            logger.error("Could not abort previous transfer: %s" % abort_out)
    
    # Construct the command with better error capturing
    cmd = f'ssh -oStrictHostKeyChecking=no root@{host} '

    # If using native ZFS compression, do not add decompression step
    if use_native_compression:
        cmd += f"'zfs recv -s -F {dataset} 2>&1'"
        if force:
            logger.debug("Using direct ZFS receive with native compression - skipping decompression step")
    # Otherwise add the appropriate decompression command based on type
    elif type == 'lz4':
        cmd += f"'lz4c -d | zfs recv -s -F {dataset} 2>&1'"
    elif type == 'gzip':
        cmd += f"'gzip -d | zfs recv -s -F {dataset} 2>&1'"
    elif type == 'bzip2':
        cmd += f"'bzip2 -d | zfs recv -s -F {dataset} 2>&1'"
    elif type == 'xz':
        cmd += f"'xz -d | zfs recv -s -F {dataset} 2>&1'"
    elif type == 'zstd':
        cmd += f"'zstd -d -c | zfs recv -s -F {dataset} 2>&1'"
    elif type is None:
        cmd += f"'zfs recv -s -F {dataset} 2>&1'"
    else:
        cmd += f"'zfs recv -s -F {dataset} 2>&1'"

    if cmd:
        # Start the receive process and capture both stdout and stderr
        logger.debug(f"Starting receive with command: {cmd}")
        p = Popen(cmd, stdin=fd, stdout=PIPE, stderr=PIPE, shell=True)
        return p
    else:
        return None

# ====== PV module ======
def pv(fd, verbose=True, limit=0, size=0, time=0, shell=False, out_fd=None, file=None, machine_readable=False):
    """
    Process visualization utility wrapper.
    
    Args:
        fd: Input file descriptor
        verbose: Show progress meter (True by default)
        limit: Rate limit in bytes per second
        size: Expected data size in bytes
        time: Expected transfer time in seconds
        shell: Use shell execution
        out_fd: Output file descriptor
        file: Output file path
        machine_readable: Force output for machine parsing (adds -f -i 1)
        
    Returns:
        Process object
    """
    if size and time:
        limit_calc = int(float(size)/float(time))
        limit = limit_calc

    if file:
        shell = True

    if shell:
        cmd = 'pv '
        if not verbose:
            cmd += '-q '
        else:
            # Add more informative flags for verbose output
            # -p: progress bar, -t: timer, -e: ETA, -r: rate counter, -b: bytes transferred, -W: wide output
            cmd += '-p -t -e -r -b -W '
        
        # Add machine-readable flags
        if machine_readable:
            # -f: force output even when not to terminal
            # -i 1: update every 1 second
            cmd += '-f -i 1 '
            
        if limit:
            cmd += '-L %s ' % str(limit)
        if size:
            cmd += '-s %s ' % str(size)
        if file:
            cmd += ' > %s' % file
    else:
        cmd = ['pv']
        if not verbose:
            cmd.append('-q')
        else:
            # Add more informative flags for verbose output
            for flag in ['-p', '-t', '-e', '-r', '-b', '-W']:
                cmd.append(flag)
        
        # Add machine-readable flags
        if machine_readable:
            cmd.extend(['-f', '-i', '1'])
            
        if limit:
            cmd.append('-L')
            cmd.append(str(limit))
        if size:
            cmd.append('-s')
            cmd.append(str(size))

    if cmd:
        # Log the pv command being executed
        import logging
        logger = logging.getLogger(__name__)
        if shell:
            logger.info(f"Executing pv command: {cmd}")
        else:
            logger.info(f"Executing pv command: {' '.join(cmd)}")
            
        if file:
            p = Popen(cmd, stdin=fd, shell=shell)
        else:
            if not out_fd:
                p = Popen(cmd, stdin=fd, stdout=PIPE, shell=shell)
            else:
                p = Popen(cmd, stdin=fd, stdout=out_fd, shell=shell)
        return p
    else:
        return None

def pv_file(file, verbose=True, limit=0, time=0):
    """
    File visualization utility wrapper.
    
    Args:
        file: Path to file
        verbose: Show progress meter (True by default)
        limit: Rate limit in bytes per second
        time: Expected transfer time in seconds
        
    Returns:
        Process object
    """
    cmd = []
    if os.path.isfile(file):
        size = os.path.getsize(file)

        if time:
            limit_calc = int(float(size) / float(time))
            limit = limit_calc

        cmd = ['pv', file]

        if not verbose:
            cmd.append('-q')
        else:
            # Add more informative flags for verbose output
            for flag in ['-p', '-t', '-e', '-r', '-b', '-W']:
                cmd.append(flag)
        if limit:
            cmd.append('-L')
            cmd.append(str(limit))
        if size:
            cmd.append('-s')
            cmd.append(str(size))

    if cmd:
        p = Popen(cmd, bufsize=128*1024*1024, stdout=PIPE)
        return p
    else:
        return None

# ====== ZFS module ======
class zfs():
    def get_zfs_version(self, remote_host=None):
        """
        Detect the ZFS version on the local or remote system.
        
        Args:
            remote_host (str, optional): Remote hostname or IP to check ZFS version.
                                         If None, local version is checked.
        
        Returns:
            version object: The detected ZFS version, or None if unable to detect.
        """
        try:
            if remote_host:
                cmd = ['ssh', '-oStrictHostKeyChecking=no', f'root@{remote_host}', 'zfs', '--version']
            else:
                cmd = ['zfs', '--version']
                
            out, err, rc = execute(cmd)
            
            if rc == 0 and out:
                # Parse version string like "zfs-2.1.5-1ubuntu6"
                ver_str = out.strip().split('-')[0].replace('zfs-', '')
                # Handle versions without the "zfs-" prefix
                if ver_str.startswith('0.') or ver_str.startswith('1.') or ver_str.startswith('2.'):
                    return version.parse(ver_str)
                else:
                    # Try to extract just numeric part if format is unexpected
                    match = re.search(r'(\d+\.\d+\.\d+)', out)
                    if match:
                        return version.parse(match.group(1))
            return None
        except Exception as e:
            logger.error(f"Error detecting ZFS version: {e}")
            return None
    
    def is_zfs(self, dataset):
        out, err, rc = execute(['zfs', 'list', '-H', '-p', dataset])
        list = []
        for i in out.split('\t'):
            list.append(i)
        if list and rc == 0 and list[0] == dataset:
            return True
        else:
            return False

    def type(self, dataset):
        props = self.get_all(dataset)
        if not 'origin' in props.keys():
            return [props['type'], 'original']
        else:
            return [props['type'], 'clone']

    def snapshot(self, dataset, snap, recurse=False):
        if not recurse:
            out, err, rc = execute(['zfs', 'snapshot', dataset + '@' + snap])
        else:
            out, err, rc = execute(['zfs', 'snapshot', '-r', dataset + '@' + snap])

        return rc

    def get_snapshots(self, dataset):
        """
        Get a list of snapshots for a ZFS dataset.
        
        Args:
            dataset: The ZFS dataset to get snapshots for
            
        Returns:
            list: List of snapshot names with newlines stripped
        """
        result = []
        out, err, rc = execute(['zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name'])
        if rc == 0:
            for i in out.splitlines():
                i = i.strip()  # Strip any trailing newlines or whitespace
                if dataset == i.split('@')[0]:
                    result.append(i.split('@')[1])
        return result

    def set(self, dataset, property, value):
        out, err, rc = execute(['zfs', 'set', property + '=' + value, dataset])
        return rc

    def get(self, dataset=None, property='all'):
        if dataset:
            out, err, rc = execute(['zfs', 'get', '-H', '-p', property, dataset])
            if out.split('\t')[2] != '-':
                value = out.split('\t')[2]
            else:
                value = None
            return value
        else:
            list = {}
            out, err, rc = execute(['zfs', 'get', '-H', property])
            for i in out.splitlines():
                if i.split('\t')[2] != '-':
                    list[i.split('\t')[0]] = i.split('\t')[2]
                else:
                    list[i.split('\t')[0]] = None
            return list

    def get_all(self, dataset):
        out, err, rc = execute(['zfs', 'get', '-H', 'all', dataset])
        list = {}
        for i in out.splitlines():
            list[i.split('\t')[1]] = i.split('\t')[2]
        return list
        
    def get_dataset_compression(self, dataset, remote_host=None):
        """
        Check if a dataset has compression enabled.
        
        Args:
            dataset (str): ZFS dataset name to check
            remote_host (str, optional): Remote hostname or IP if checking remote dataset
            
        Returns:
            str or None: Compression algorithm in use (e.g., 'lz4', 'gzip', etc.), 
                         'off' if compression is disabled, or None if error
        """
        try:
            if remote_host:
                cmd = ['ssh', '-oStrictHostKeyChecking=no', f'root@{remote_host}', 
                       'zfs', 'get', '-H', 'compression', dataset]
            else:
                cmd = ['zfs', 'get', '-H', 'compression', dataset]
                
            out, err, rc = execute(cmd)
            
            if rc == 0 and out:
                # Parse output like "tank/data    compression    lz4    default"
                compression = out.strip().split('\t')[2]
                return compression
            return None
        except Exception as e:
            logger.error(f"Error checking dataset compression: {e}")
            return None

    def destroy(self, dataset, recurse=False):
        if recurse == False:
            out, err, rc = execute(['zfs', 'destroy', dataset])
        else:
            out, err, rc = execute(['zfs', 'destroy', '-R', dataset])
        return rc

    def hold(self, dataset, snapshot, tag, recurse=False):
        if self.is_zfs(dataset):
            if recurse:
                out, err, rc = execute(['zfs', 'hold', '-r', tag, dataset + '@' + snapshot])
            else:
                out, err, rc = execute(['zfs', 'hold', tag, dataset + '@' + snapshot])
            return rc
        else:
            return -1

    def release(self, dataset, snapshot, tag, recurse=False):
        if self.is_zfs(dataset):
            if recurse:
                out, err, rc = execute(['zfs', 'release', '-r', tag, dataset + '@' + snapshot])
            else:
                out, err, rc = execute(['zfs', 'release', tag, dataset + '@' + snapshot])
            return rc
        else:
            return -1

    def holds(self, dataset, snapshot, recurse=False):
        holds = []
        if self.is_zfs(dataset):
            if recurse:
                out, err, rc = execute(['zfs', 'holds', '-H', '-r', dataset + '@' + snapshot])
            else:
                out, err, rc = execute(['zfs', 'holds', '-H', dataset + '@' + snapshot])
            if rc == 0 and out:
                for line in sorted(out.splitlines(), reverse=True):
                    holds.append(line.split('\t')[1])
        return holds

    def get_holds(self, dataset, recurse=False):
        snaps = self.get_snapshots(dataset)
        holds = OrderedDict()
        for s in snaps:
            hold = self.holds(dataset, s, recurse)
            if hold:
                holds[s] = hold
        return holds

    def send(self, dataset, snap, snap1=None, recurse=False, use_native_compression=False):
        """
        Generate a ZFS send command and return the process.
        
        Args:
            dataset: The ZFS dataset to send
            snap: The first snapshot name
            snap1: The second snapshot name for incremental send
            recurse: Whether to do a recursive send
            use_native_compression: Whether to use ZFS native compression (-c flag)
            
        Returns:
            A process object for the ZFS send command
        """
        cmd = []
        if self.is_zfs(dataset):
            snaps = self.get_snapshots(dataset)
            # Handle full send when snap is None
            if snap is None and snap1 and snap1 in snaps:
                cmd.append('zfs')
                cmd.append('send')
                
                # Add native compression flag if requested
                if use_native_compression:
                    cmd.append('-c')
                    
                if recurse:
                    cmd.append('-R')
                cmd.append(dataset + '@' + snap1)
            elif snap in snaps:
                cmd.append('zfs')
                cmd.append('send')
                
                # Add native compression flag if requested
                if use_native_compression:
                    cmd.append('-c')
                    
                if recurse:
                    cmd.append('-R')
                if not snap1:
                    cmd.append(dataset + '@' + snap)
                else:
                    if snap1 in snaps:
                        cmd.append('-I')
                        cmd.append(dataset + '@' + snap)
                        cmd.append(dataset + '@' + snap1)
                    else:
                        cmd = []
        if cmd:
            p = Popen(cmd, stdout=PIPE)
            return p
        else:
            return None

    def get_send_size(self, dataset, snap, snap1=None, recurse=False, use_native_compression=False):
        """
        Get the estimated size of a ZFS send stream.
        
        Args:
            dataset: The ZFS dataset to send
            snap: The first snapshot name
            snap1: The second snapshot name for incremental send
            recurse: Whether to do a recursive send
            use_native_compression: Whether to use ZFS native compression (-c flag)
            
        Returns:
            The estimated size in bytes, or None if estimation fails
        """
        cmd = []
        if self.is_zfs(dataset):
            snaps = self.get_snapshots(dataset)
            # Handle full send when snap is None
            if snap is None and snap1 and snap1 in snaps:
                cmd.append('zfs')
                cmd.append('send')
                
                # Add native compression flag if requested
                if use_native_compression:
                    cmd.append('-c')
                    
                if recurse:
                    cmd.append('-R')
                cmd.append('-nv')
                cmd.append(dataset + '@' + snap1)
            elif snap in snaps:
                cmd.append('zfs')
                cmd.append('send')
                
                # Add native compression flag if requested
                if use_native_compression:
                    cmd.append('-c')
                    
                if recurse:
                    cmd.append('-R')
                cmd.append('-nv')
                if not snap1:
                    cmd.append(dataset + '@' + snap)
                else:
                    if snap1 in snaps:
                        cmd.append('-I')
                        cmd.append(dataset + '@' + snap)
                        cmd.append(dataset + '@' + snap1)
                    else:
                        cmd = []
        if cmd:
            out, err, rc = execute(cmd)
            if rc == 0:
                return self.conv_space(out.splitlines()[-1].split()[-1])
            else:
                return None
        else:
            return None

    def conv_space(self, space):
        if space[-1] == 'K':
            return int(float(space[:-1].replace(',', '.')) * 1024)
        if space[-1] == 'M':
            return int(float(space[:-1].replace(',', '.')) * 1024 * 1024)
        if space[-1] == 'G':
            return int(float(space[:-1].replace(',', '.')) * 1024 * 1024 * 1024)
        if space[-1] == 'T':
            return int(float(space[:-1].replace(',', '.')) * 1024 * 1024 * 1024 * 1024)

    def recv(self, dataset, force=True):
        cmd = []
        cmd.append('zfs')
        cmd.append('recv')
        cmd.append(dataset)
        if force:
            cmd.append('-F')
        if cmd:
            p = Popen(cmd, stdin=PIPE)
            return p
        else:
            return None

    def recv_pipe(self, fd, dataset, force=True):
        cmd = []
        cmd.append('zfs')
        cmd.append('recv')
        cmd.append(dataset)
        if force:
            cmd.append('-F')
        if cmd:
            p = Popen(cmd, stdin=fd)
            return p
        else:
            return None

    def engociate_inc_send(self, source_snapshots, dest_snapshots=[]):
        """
        Find common snapshots between source and destination for incremental send.
        
        Args:
            source_snapshots: List of snapshots on the source
            dest_snapshots: List of snapshots on the destination
            
        Returns:
            tuple: (common_snapshot, latest_source_snapshot) for incremental send,
                  or (None, None) if no common snapshots found
        """
        init_snap = None
        last_snap = None
        
        # Clean up the snapshot names (strip any trailing newlines)
        clean_source_snapshots = [snap.strip() if isinstance(snap, str) else snap for snap in source_snapshots]
        clean_dest_snapshots = [snap.strip() if isinstance(snap, str) else snap for snap in dest_snapshots]
        
        logger.debug(f"Source snapshots: {clean_source_snapshots}")
        logger.debug(f"Destination snapshots (raw): {dest_snapshots}")
        logger.debug(f"Destination snapshots (cleaned): {clean_dest_snapshots}")
        
        if clean_source_snapshots and clean_dest_snapshots:
            # Find common snapshots between source and destination
            common_snaps = [snap for snap in clean_dest_snapshots if snap in clean_source_snapshots]
            if common_snaps:
                # Sort common snapshots to find the most recent
                init_snap = common_snaps[-1]  # Get the most recent common snapshot
                logger.debug(f"Found common snapshot: {init_snap}")
            else:
                # Try with case-insensitive matching as a fallback
                source_lower = [s.lower() for s in clean_source_snapshots]
                dest_lower = [d.lower() for d in clean_dest_snapshots]
                common_lower = [snap for snap in dest_lower if snap in source_lower]
                
                if common_lower:
                    # Find the original case version
                    idx = dest_lower.index(common_lower[-1])
                    init_snap = clean_dest_snapshots[idx]
                    logger.debug(f"Found common snapshot with case-insensitive match: {init_snap}")
                else:
                    logger.warning("No common snapshots found between source and destination")
        
        if init_snap and clean_source_snapshots:
            last_snap = clean_source_snapshots[-1]
            logger.debug(f"Source's latest snapshot: {last_snap}")
        
        return init_snap, last_snap

    def adaptive_send_remote(self, host, dataset, snap=None, snap1=None, recurse=False, 
                            compression=None, use_native_compression=False, resume_token=None):
        """
        Create a ZFS send process on a remote host with support for compression, incremental sends,
        and resume tokens.
        
        Args:
            host: Remote host name or IP address
            dataset: ZFS dataset to send from the remote host
            snap: Snapshot name (required unless resume_token is provided)
            snap1: Optional second snapshot for incremental send
            recurse: Whether to send recursively
            compression: Compression type to use on the remote side (gzip, bzip2, xz, lz4, zstd)
            use_native_compression: Whether to use ZFS native compression (-c flag)
            resume_token: Token to resume a previous send
            
        Returns:
            Process object for the SSH process with remote ZFS send
        """
        # Build the ZFS command for the remote send
        if resume_token:
            # Resume a previous interrupted send
            if use_native_compression:
                zfs_cmd = f"zfs send -c -t {resume_token}"
            else:
                zfs_cmd = f"zfs send -t {resume_token}"
        elif snap and snap1:
            # Incremental send between two snapshots
            if use_native_compression:
                zfs_cmd = f"zfs send {'-R ' if recurse else ''} -c -i {dataset}@{snap} {dataset}@{snap1}"
            else:
                zfs_cmd = f"zfs send {'-R ' if recurse else ''} -i {dataset}@{snap} {dataset}@{snap1}"
        elif snap:
            # Full send of a single snapshot
            if use_native_compression:
                zfs_cmd = f"zfs send {'-R ' if recurse else ''} -c {dataset}@{snap}"
            else:
                zfs_cmd = f"zfs send {'-R ' if recurse else ''} {dataset}@{snap}"
        else:
            # Cannot proceed without either a snapshot or resume token
            logger.error("Either snap or resume_token must be provided for remote send")
            return None
            
        # Add compression if requested and not using native compression
        if compression and not use_native_compression:
            # Select appropriate compression command based on type
            if compression == 'gzip':
                compress_cmd = "gzip -c"
            elif compression == 'bzip2':
                compress_cmd = "bzip2 -c"
            elif compression == 'xz':
                compress_cmd = "xz -c"
            elif compression == 'lz4':
                compress_cmd = "lz4c -c"
            elif compression == 'zstd':
                compress_cmd = "zstd -c"
            else:
                # Unknown compression type, fall back to no compression
                compress_cmd = None
                
            # Add compression to command if specified
            if compress_cmd:
                ssh_cmd = f"ssh -oStrictHostKeyChecking=no root@{host} '{zfs_cmd} | {compress_cmd}'"
            else:
                ssh_cmd = f"ssh -oStrictHostKeyChecking=no root@{host} '{zfs_cmd}'"
        else:
            # No compression
            ssh_cmd = f"ssh -oStrictHostKeyChecking=no root@{host} '{zfs_cmd}'"
            
        # Execute SSH process with the appropriate command
        logger.debug(f"Executing remote command: {ssh_cmd}")
        return Popen(ssh_cmd, stdout=PIPE, shell=True)
    
    def adaptive_recv(self, fd, dataset, compression=None, verbose=False, machine_readable=False, limit=0, time=0, size=0, force=True):
        """
        Receive a ZFS snapshot with adaptive decompression and progress visualization.
        
        Args:
            fd: Input file descriptor with ZFS stream data
            dataset: ZFS dataset to receive into or file path (if starting with /)
            compression: Compression type used in the stream (gzip, bzip2, xz, lz4, zstd)
            verbose: Whether to display progress information
            limit: Rate limit in bytes per second
            time: Time limit for transfer in seconds
            size: Expected data size in bytes (for progress estimation)
            force: Whether to use the -F flag with zfs receive
            
        Returns:
            Process object for the zfs receive operation or file writing
        """
        if not fd:
            logger.error("No input stream provided to adaptive_recv")
            return None
        
        # Determine if the dataset is a file path or ZFS dataset
        is_file = dataset.startswith('/')
        
        input_fd = fd
        
        # Apply decompression if needed
        if compression and compression != 'none':
            logger.info(f"Decompressing ZFS stream with {compression}")
            decompress_process = uncompressor(input_fd, type=compression)
            input_fd = decompress_process.stdout
        
        # Handle based on whether destination is a file or ZFS dataset
        if is_file:
            logger.info(f"Writing stream to file: {dataset}")
            # For files, use pv with file output for both visualization and writing
            if verbose:
                logger.info(f"Visualizing receive progress with pv and writing to file")
                file_proc = pv(input_fd, verbose=verbose, machine_readable=machine_readable, limit=limit, time=time, size=size, file=dataset)
                return file_proc
            else:
                # Without visualization, just write to file
                try:
                    out_file = open(dataset, 'wb')
                    cat_proc = Popen(["cat"], stdin=input_fd, stdout=out_file)
                    return cat_proc
                except Exception as e:
                    logger.error(f"Error writing to file {dataset}: {str(e)}")
                    return None
        else:
            # For ZFS datasets, use visualization then zfs receive
            if verbose:
                logger.info(f"Visualizing receive progress with pv")
                pv_process = pv(input_fd, verbose=verbose, machine_readable=machine_readable, limit=limit, time=time, size=size)
                input_fd = pv_process.stdout
            
            # Execute the zfs receive command
            logger.info(f"Receiving ZFS stream into dataset: {dataset}")
            recv_process = self.recv_pipe(input_fd, dataset, force=force)
            return recv_process
        
    def adaptive_send(self, dataset, snap, snap1=None, recurse=False, compression=None, verbose=False, machine_readable=False, limit=0,
                      time=0, out_fd=None, use_native_compression=False, resume_token=None):
        """
        Send a ZFS snapshot with adaptive compression and progress visualization.
        
        Args:
            dataset: ZFS dataset to send
            snap: Snapshot name
            snap1: Optional second snapshot for incremental send
            recurse: Whether to send recursively
            compression: Compression type to use
            verbose: Whether to display progress information
            limit: Rate limit for transfer
            time: Time limit for transfer
            out_fd: Output file descriptor
            use_native_compression: Whether to use ZFS native compression (-c flag)
            resume_token: Token to resume a previous send
            
        Returns:
            Process object for the send operation
        """
        if resume_token:
            if verbose:
                logger.info(f'Using resume token: {resume_token}')
            # For resume tokens, we use a different send command
            cmd = ['zfs', 'send', '-t', resume_token]
            # Add native compression if requested
            if use_native_compression:
                cmd.insert(2, '-c')  # Add after 'send'
                if verbose:
                    logger.info("Using ZFS native compression with -c flag")
            
            if verbose:
                logger.debug(f"Executing command: {' '.join(cmd)}")
            p = Popen(cmd, stdout=PIPE)
            send = p
            size = None  # We don't know the size when resuming
        else:
            if use_native_compression:
                # Use send method with native compression
                if verbose:
                    logger.info("Using ZFS native compression with -c flag")
                
                # Get estimated size and process with native compression
                size = self.get_send_size(dataset, snap, snap1, recurse, use_native_compression=True)
                send = self.send(dataset, snap, snap1, recurse, use_native_compression=True)
            else:
                # Traditional send without native compression
                size = self.get_send_size(dataset, snap, snap1, recurse, use_native_compression=False)
                send = self.send(dataset, snap, snap1, recurse, use_native_compression=False)

        if send:
            if verbose and size:
                logger.info(f"Starting ZFS send with size estimate: {size} bytes ({format_bytes(size)})")
            elif verbose:
                logger.info("Starting ZFS send (size unknown)")
                
            if compression and not use_native_compression:
                if verbose:
                    logger.info(f"Using {compression} compression for ZFS stream")
                # Use pv for progress visualization first
                piper = pv(send.stdout, verbose=verbose, machine_readable=machine_readable, limit=limit, size=size, time=time)
                # Then apply compression
                if out_fd:
                    compr = compressor(piper.stdout, compression, out_fd=out_fd)
                else:
                    compr = compressor(piper.stdout, compression)
                return compr
            else:
                if verbose and not use_native_compression:
                    logger.info("Sending ZFS stream without compression")
                elif verbose and use_native_compression:
                    logger.info("Sending ZFS stream with native compression")
                    
                # Just use pv for progress visualization
                if out_fd:
                    piper = pv(send.stdout, verbose=verbose, machine_readable=machine_readable, limit=limit, size=size, time=time, out_fd=out_fd)
                else:
                    piper = pv(send.stdout, verbose=verbose, machine_readable=machine_readable, limit=limit, size=size, time=time)
                return piper

class zpool():
    def list(self):
        list = []
        out, err, rc = execute(['zpool', 'list', '-H', '-o', 'name'])
        for i in out.splitlines():
            list.append(i)
        return list

    def get(self, zpool, property='all'):
        if zpool:
            out, err, rc = execute(['zpool', 'get', '-H', '-p', property, zpool])
            value = out.split('\t')[2]
            return value
        else:
            list = {}
            out, err, rc = execute(['zpool', 'get', '-H', '-p', property])
            for i in out.splitlines():
                if i.split('\t')[2] != '-':
                    list[i.split('\t')[0]] = i.split('\t')[2]
            return list

    def get_all(self, zpool):
        out, err, rc = execute(['zpool', 'get', '-H', '-p', 'all', zpool])
        list = {}
        for i in out.splitlines():
            list[i.split('\t')[1]] = i.split('\t')[2]
        return list

    def set(self, zpool, property, value):
        out, err, rc = execute(['zpool', 'set', property + '=' + value, zpool])
        return rc

# ====== SSH module ======
class SSHConnectionError(Exception):
    """Exception raised for SSH connection errors."""
    pass

class SSH:
    """Base SSH connection class with improved security and error handling."""
    
    def __init__(self, host: str, user: str = "root", passwd: Optional[str] = None, 
                 deploy_key: bool = True, key_path: Optional[str] = None):
        """
        Initialize SSH connection with host.
        
        Args:
            host: Hostname or IP address to connect to
            user: Username for the connection
            passwd: Password for password authentication (if not using key)
            deploy_key: Whether to automatically deploy SSH key
            key_path: Path to SSH key file
        """
        self.state = 0
        self.host = host
        self.user = user
        self.detected_private_key_path = None
        self.detected_public_key_path = None
        
        # Check if we have paramiko available
        try:
            import paramiko
            try:
                from ping import fping
            except ImportError:
                # Fallback if ping module is not available
                logger.warning("Python 'ping' module not available, using subprocess ping instead")
                
                # Define a fallback fping function using subprocess
                def fping(hosts):
                    """
                    Fallback fping function using subprocess.
                    
                    Args:
                        hosts: List of hostnames or IPs to ping
                        
                    Returns:
                        Tuple of (alive_hosts, dead_hosts)
                    """
                    alive = []
                    dead = []
                    for host in hosts:
                        try:
                            # Use different ping commands based on OS
                            if os.name == "nt":  # Windows
                                cmd = ["ping", "-n", "1", "-w", "1000", host]
                            else:  # Linux/Unix
                                cmd = ["ping", "-c", "1", "-W", "1", host]
                                
                            rc = Popen(cmd, stdout=PIPE, stderr=PIPE).wait()
                            if rc == 0:
                                alive.append(host)
                            else:
                                dead.append(host)
                        except Exception as e:
                            logger.warning(f"Error pinging host {host}: {str(e)}")
                            dead.append(host)
                    
                    return alive, dead
                
            try:
                from scp import SCPClient
            except ImportError:
                # Fallback if scp module is not available
                logger.warning("Python 'scp' module not available, secure file transfers may be limited")
                SCPClient = None
            
            # Use environment variables as fallback for credentials
            if passwd is None:
                passwd = os.environ.get("SSH_PASSWORD", "")
            
            if key_path is None:
                key_path = os.environ.get("SSH_KEY_PATH")
                if not key_path:
                    # Try to detect available SSH keys
                    key_path = self._detect_ssh_key()
            
            # Check if host is reachable
            try:
                alive, dead = fping([host])
                if host not in alive:
                    logger.error(f"Host {host} is not reachable (ping failed)")
                    return
            except Exception as e:
                logger.error(f"Error checking if host {host} is reachable: {str(e)}")
                logger.info("Continuing without ping check, will attempt SSH connection anyway")
                # Continue with the connection attempt even if ping fails
                
            # Initialize SSH client
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Try password authentication first if password is provided
            if passwd:
                try:
                    self.client.connect(
                        hostname=host, 
                        username=user, 
                        password=passwd,
                        look_for_keys=False,
                        allow_agent=False
                    )
                    self.client.get_transport().window_size = 3 * 1024 * 1024
                    self.state = 1
                    logger.info(f"Connected to {host} using password authentication")
                    
                    # Deploy key if requested
                    if deploy_key:
                        logger.info(f"Auto deploying key for user {user}")
                        ret = self.copy_key()
                        if ret:
                            logger.info("Key deployed successfully")
                        else:
                            logger.error("Error deploying key")
                    return
                except paramiko.AuthenticationException:
                    logger.warning(f"Password authentication failed for {host}")
                except paramiko.SSHException as e:
                    logger.error(f"SSH connection error during password auth: {str(e)}")
                    
            # Try key-based authentication
            try:
                # Create a new client for the second attempt
                self.client.close()
                self.client = paramiko.SSHClient()
                self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                
                # Try with autoaddpolicy but with limited key checking
                # Disable DSA keys by limiting key type searches
                try:
                    # First attempt with explicit key file but disabled DSA key search
                    self.client.connect(
                        hostname=host, 
                        username=user, 
                        key_filename=key_path,
                        look_for_keys=False,  # Don't look for other keys first
                        allow_agent=False     # Don't use ssh-agent
                    )
                    self.client.get_transport().window_size = 3 * 1024 * 1024
                    self.state = 1
                    logger.info(f"Connected to {host} using key file authentication")
                except (paramiko.AuthenticationException, ValueError, paramiko.SSHException) as inner_e:
                    logger.debug(f"First key attempt failed: {str(inner_e)}")
                    
                    # Second attempt with system keys but careful search
                    self.client.close()
                    self.client = paramiko.SSHClient()
                    self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    
                    # Try to connect with RSA keys only
                    try:
                        self.client.connect(
                            hostname=host, 
                            username=user,
                            look_for_keys=True,
                            allow_agent=True,
                            disabled_algorithms={'pubkeys': ['dss', 'dss-cert-v01@openssh.com']}  # Disable DSA keys
                        )
                        self.client.get_transport().window_size = 3 * 1024 * 1024
                        self.state = 1
                        logger.info(f"Connected to {host} using system key authentication (DSA disabled)")
                    except (paramiko.AuthenticationException, ValueError, paramiko.SSHException) as inner_e2:
                        logger.debug(f"Second key attempt failed: {str(inner_e2)}")
                        raise inner_e2
                        
            except (paramiko.AuthenticationException, ValueError) as e:
                # Auth failed, try password prompt if not already tried
                if not passwd:
                    logger.warning(f"Key authentication failed: {str(e)}")
                    logger.warning("Attempting password authentication")
                    try:
                        import getpass
                        passwd_input = getpass.getpass(f"Password for {user}@{host}: ")
                        
                        # Try with password
                        self.client.close()
                        self.client = paramiko.SSHClient()
                        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                        
                        self.client.connect(
                            hostname=host, 
                            username=user, 
                            password=passwd_input,
                            look_for_keys=False,
                            allow_agent=False
                        )
                        self.client.get_transport().window_size = 3 * 1024 * 1024
                        self.state = 1
                        logger.info(f"Connected to {host} using password authentication")
                    except (paramiko.AuthenticationException, ValueError, paramiko.SSHException, ImportError) as pwd_e:
                        logger.error(f"Password authentication failed: {str(pwd_e)}")
                else:
                    logger.error(f"Key authentication failed: {str(e)}")
            except paramiko.SSHException as e:
                logger.error(f"SSH connection error: {str(e)}")
                
        except ImportError as e:
            # Fallback to subprocess ssh for systems without paramiko
            logger.warning(f"Paramiko not available: {e}. To use SSH with Python libraries, install: pip install paramiko scp")
            logger.warning("Falling back to subprocess ssh (requires ssh client and key authentication set up)")
            self.use_subprocess = True
            # Test SSH connection
            out, err, rc = execute(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5', 
                                  f'{user}@{host}', 'echo', 'test'])
            if rc == 0:
                self.state = 1
                logger.info(f"Connected to {host} using subprocess ssh")
            else:
                logger.error(f"SSH connection failed: {err}")
    
    def execute(self, cmd: str) -> List[str]:
        """
        Execute command on remote host.
        
        Args:
            cmd: Command to execute
            
        Returns:
            List of output lines
        """
        if self.state != 1:
            logger.error("SSH connection not established")
            return []
        
        try:
            if hasattr(self, 'use_subprocess') and self.use_subprocess:
                # Use subprocess for SSH
                ssh_cmd = ['ssh', f'{self.user}@{self.host}', cmd]
                out, err, rc = execute(ssh_cmd)
                if rc != 0:
                    logger.warning(f"Command '{cmd}' produced errors: {err}")
                return out.splitlines()
            else:
                # Use paramiko
                out = []
                stdin, stdout, stderr = self.client.exec_command(cmd)
                for line in stdout.readlines():
                    # Handle both string and bytes output
                    if isinstance(line, bytes):
                        out.append(line.decode('utf-8', errors='replace'))
                    else:
                        out.append(line)
                        
                # Check for errors
                err = stderr.read()
                if err:
                    logger.warning(f"Command '{cmd}' produced errors: {err.decode('utf-8', errors='replace')}")
                return out
        except Exception as e:
            logger.error(f"Error executing command '{cmd}': {str(e)}")
            return []
    
    def _detect_ssh_key(self):
        """
        Detect available SSH private key in order of preference.
        
        Returns:
            Path to detected private key or None
        """
        ssh_dir = os.path.expanduser("~/.ssh")
        key_types = [
            ("id_ed25519", "id_ed25519.pub"),
            ("id_rsa", "id_rsa.pub"),
            ("id_ecdsa", "id_ecdsa.pub"),
            ("id_dsa", "id_dsa.pub")
        ]
        
        for private_key, public_key in key_types:
            private_path = os.path.join(ssh_dir, private_key)
            public_path = os.path.join(ssh_dir, public_key)
            
            if os.path.exists(private_path):
                logger.debug(f"Detected SSH key: {private_path}")
                self.detected_private_key_path = private_path
                if os.path.exists(public_path):
                    self.detected_public_key_path = public_path
                return private_path
        
        logger.debug("No SSH keys detected")
        return None
    
    def copy_key(self) -> bool:
        """
        Copy SSH public key to remote host for key-based authentication.
        
        Returns:
            True if successful, False otherwise
        """
        # Try to find public key path
        public_key_path = None
        
        # First check if we already detected a public key
        if hasattr(self, 'detected_public_key_path') and self.detected_public_key_path:
            public_key_path = self.detected_public_key_path
        else:
            # Try to detect SSH keys if not already done
            ssh_dir = os.path.expanduser("~/.ssh")
            for key_file in ["id_ed25519.pub", "id_rsa.pub", "id_ecdsa.pub"]:
                path = os.path.join(ssh_dir, key_file)
                if os.path.exists(path):
                    public_key_path = path
                    break
        
        if not public_key_path:
            logger.error("No public key found in ~/.ssh/ (checked for id_ed25519.pub, id_rsa.pub, id_ecdsa.pub)")
            return False
        
        logger.info(f"Using SSH public key: {public_key_path}")
        
        try:
            with open(public_key_path) as f:
                key = f.read().strip()
        except (IOError, OSError) as e:
            logger.error(f"Error reading public key from {public_key_path}: {str(e)}")
            return False

        if not key:
            return False
            
        try:
            # Safely escape the key to prevent command injection
            import shlex
            escaped_key = shlex.quote(key)
            
            self.execute('mkdir -p ~/.ssh/')
            self.execute(f'echo {escaped_key} >> ~/.ssh/authorized_keys')
            self.execute('chmod 644 ~/.ssh/authorized_keys')
            self.execute('chmod 700 ~/.ssh/')
            return True
        except Exception as e:
            logger.error(f"Error deploying key: {str(e)}")
            return False
    
    def close(self):
        """Close SSH and SCP connections."""
        if hasattr(self, 'client') and self.client and not hasattr(self, 'use_subprocess'):
            self.client.close()
        if hasattr(self, 'scp_client') and self.scp_client:
            self.scp_client.close()
    
    def get_remote_zfs_property(self, dataset, property_name):
        """
        Get ZFS property from a remote dataset using Paramiko.
        
        Args:
            dataset: ZFS dataset name
            property_name: Name of the property to retrieve
            
        Returns:
            Property value or None if not found
        """
        if self.state != 1:
            logger.error("SSH connection not established")
            return None
        
        cmd = f'zfs get -H -p {property_name} {dataset}'
        output = self.execute(cmd)
        
        if output and len(output) > 0:
            parts = output[0].split('\t')
            if len(parts) > 2:
                return parts[2]
        
        return None
    
    def list_remote_snapshots(self, dataset):
        """
        List snapshots of a remote dataset using Paramiko.
        
        Args:
            dataset: ZFS dataset name
            
        Returns:
            List of snapshot names
        """
        if self.state != 1:
            logger.error("SSH connection not established")
            return []
        
        cmd = f'zfs list -t snapshot -H -o name | grep {dataset}@'
        output = self.execute(cmd)
        
        # Process snapshot names to extract the part after @
        snapshots = []
        for snap in output:
            if '@' in snap:
                snapshots.append(snap.split('@')[1].strip())
        
        return snapshots
    
    def abort_receive(self, dataset):
        """
        Abort an incomplete ZFS receive operation on the remote host.
        
        Args:
            dataset: ZFS dataset name
            
        Returns:
            True if successful, False otherwise
        """
        if self.state != 1:
            logger.error("SSH connection not established")
            return False
        
        cmd = f'zfs receive -A {dataset} 2>&1; echo $?'
        output = self.execute(cmd)
        
        if output and "cannot abort" not in ' '.join(output).lower():
            logger.info(f"Previous interrupted transfer aborted: {output}")
            return True
        else:
            logger.error(f"Could not abort previous transfer: {output}")
            return False
    
    def check_remote_dataset_exists(self, dataset):
        """
        Check if a dataset exists on the remote host.
        
        Args:
            dataset: ZFS dataset name
            
        Returns:
            True if the dataset exists, False otherwise
        """
        if self.state != 1:
            logger.error("SSH connection not established")
            return False
        
        cmd = f'zfs list -H {dataset}'
        output = self.execute(cmd)
        
        return len(output) > 0
        
    def estimate_send_size(self, dataset, snapshot, snapshot2=None):
        """
        Estimate the size of a ZFS send operation on a remote host.
        
        Args:
            dataset: ZFS dataset name
            snapshot: First snapshot name
            snapshot2: Optional second snapshot for incremental send
            
        Returns:
            Estimated size in bytes or None on error
        """
        if self.state != 1:
            logger.error("SSH connection not established")
            return None
            
        try:
            if snapshot2:
                # Incremental send
                cmd = f'zfs send -nv -i {dataset}@{snapshot} {dataset}@{snapshot2}'
            else:
                # Full send
                cmd = f'zfs send -nv {dataset}@{snapshot}'
                
            output = self.execute(cmd)
            
            # Parse the size from output, which should contain "size is X"
            size_str = None
            for line in output:
                if 'size is' in line:
                    # Extract the full size string including unit
                    parts = line.split('size is')[1].strip().split()
                    if parts:
                        size_str = parts[0]
                    break
                    
            if size_str:
                try:
                    # Check if the size string has a unit suffix (K, M, G, T, P, E)
                    if size_str[-1].upper() in 'KMGTPE':
                        unit = size_str[-1].upper()
                        # Handle potential decimal values
                        value = float(size_str[:-1])
                        
                        # Convert to bytes based on unit
                        multipliers = {
                            'K': 1024,
                            'M': 1024**2,
                            'G': 1024**3,
                            'T': 1024**4,
                            'P': 1024**5,
                            'E': 1024**6
                        }
                        
                        bytes_value = int(value * multipliers[unit])
                        if self.verbose:
                            logger.info(f"Parsed size {size_str} as {bytes_value} bytes")
                        return bytes_value
                    else:
                        # If there's no unit, assume it's already in bytes
                        return int(size_str)
                except (ValueError, TypeError) as e:
                    logger.error(f"Failed to parse size from: {size_str} - {str(e)}")
                    return None
            else:
                logger.error("No size information found in command output")
                return None
        except Exception as e:
            logger.error(f"Error estimating send size: {str(e)}")
            return None
    
    def __del__(self):
        """Destructor to ensure connections are closed."""
        self.close()

class ssh_send(SSH):
    def __init__(self, host, dataset=None, recv_dataset=None, snapshot=None, compress_type='auto', 
                limit=0, time=0, recurse=False, verbose=False, machine_readable=False, user='root', passwd=None, 
                oneshot=False, create_snapshot=False, update_bootfs=False, sync=False, reverse=False):
        """Initialize SSH connection for sending ZFS datasets."""
        SSH.__init__(self, host=host, user=user, passwd=passwd)

        self.dataset = dataset
        self.recv_dataset = recv_dataset
        self.snapshot = snapshot
        self.compress_type = compress_type
        self.limit = limit
        self.time = time
        self.recurse = recurse
        self.verbose = verbose
        self.machine_readable = machine_readable
        self.oneshot = oneshot
        self.create_snapshot = create_snapshot
        self.host = host
        self.update_bootfs = update_bootfs
        self.sync = sync
        self.backup_stream = False
        self.reverse = reverse
        self.use_native_compression = False  # Default to not using native compression
        
        # Initialize ZFS and Zpool
        self.zfs = zfs()
        self.zpool = zpool()
        
        # If we have a dataset, perform ZFS compression negotiation
        if self.dataset:
            self.check_compression_support()

        self.get_auto_compression()

        if self.update_bootfs:
            logger.info('Updating bootfs for server %s' % self.host)
            if not self.dataset:
                bootfs_raw = self.zpool.get(zpool=None, property='bootfs')
                if len(bootfs_raw.keys()) == 1:
                    self.dataset = list(bootfs_raw.values())[0].split('@')[0]
                else:
                    if 'syspool' in bootfs_raw.keys():
                        self.dataset = bootfs_raw['syspool'].split('@')[0]
                    elif 'archive' in bootfs_raw.keys():
                        self.dataset = bootfs_raw['archive'].split('@')[0]

                logger.info('Local bootfs dataset %s' % self.dataset)

            if not self.recv_dataset:
                bootfs_raw = self.get_remote_bootfs()
                if len(bootfs_raw.keys()) == 1:
                    self.recv_dataset = list(bootfs_raw.values())[0].split('@')[0]
                else:
                    if 'syspool' in bootfs_raw.keys():
                        self.recv_dataset = bootfs_raw['syspool'].split('@')[0]
                    elif 'archive' in bootfs_raw.keys():
                        self.recv_dataset = bootfs_raw['archive'].split('@')[0]

                logger.info('Remote bootfs dataset %s' % self.recv_dataset)

        if self.zfs.is_zfs(self.dataset):
            self.state = 1
            backup = self.zfs.get(self.dataset, 'control:autobackup')
            if backup and backup == 'active':
                if self.verbose:
                    logger.info('Setting up backup stream')
                self.backup_stream = True
                self.sync = True
                if not self.recv_dataset:
                    self.recv_dataset = self.dataset

    def check_compression_support(self):
        """
        Check if native ZFS compression can be used based on ZFS versions and dataset compression.
        
        This implements the negotiation logic:
        1. If source is not a ZFS dataset, use existing behavior
        2. If source is a ZFS dataset:
           a. Check ZFS versions on both sides (local and remote)
           b. Check if source dataset has compression enabled
           c. Use native compression if versions >= 2.0 and source has compression
        """
        # Check if source is a ZFS dataset
        if not self.zfs.is_zfs(self.dataset):
            if self.verbose:
                logger.warning("Source is not a ZFS dataset, using external compression if needed")
            return
        
        # Get ZFS version on local system
        local_zfs_version = self.zfs.get_zfs_version()
        if not local_zfs_version:
            if self.verbose:
                logger.warning("Unable to detect local ZFS version, using external compression")
            return
        
        # Get ZFS version on remote system
        remote_zfs_version = self.zfs.get_zfs_version(remote_host=self.host)
        if not remote_zfs_version:
            if self.verbose:
                logger.warning(f"Unable to detect ZFS version on remote host {self.host}, using external compression")
            return
        
        # Check if both local and remote ZFS versions are >= 2.0
        if local_zfs_version >= version.parse("2.0") and remote_zfs_version >= version.parse("2.0"):
            # Check if source dataset has compression enabled
            compression_setting = self.zfs.get_dataset_compression(self.dataset)
            if compression_setting and compression_setting != 'off':
                if self.verbose:
                    logger.info(f"ZFS version {local_zfs_version} (local) and {remote_zfs_version} (remote) detected "
                          f"with {compression_setting} compression. Using native ZFS compression with -c flag")
                self.use_native_compression = True
                # If we're using native compression, we should disable external compression
                if self.compress_type:
                    if self.verbose:
                        logger.info("Disabling external compression in favor of ZFS native compression")
                    self.compress_type = None
            else:
                if self.verbose:
                    logger.info(f"Source dataset has no compression. Using external compression if needed")
        else:
            if self.verbose:
                logger.info(f"ZFS versions: local={local_zfs_version}, remote={remote_zfs_version}. "
                      f"Version 2.0+ required on both sides for native compression")
    
    def get_auto_compression(self):
        """Auto-detect best compression algorithm."""
        if self.compress_type == 'auto':
            out, err, rc = execute(['zstd', '-h'])
            out = self.execute('type zstd')
            if rc == 0 and out:
                logger.info('Found auto compression type: zstd')
                self.compress_type = 'zstd'
            else:
                out, err, rc = execute(['lz4c', '-h'])
                out = self.execute('type lz4c')
                if rc == 0 and out:
                    logger.info('Found auto compression type: lz4')
                    self.compress_type = 'lz4'
                else:
                    logger.info('No compression type found, using uncompressed stream')
                    self.compress_type = None

    def is_zfs(self, dataset):
        """Check if dataset exists on remote host."""
        try:
            out = self.execute('zfs list -H -p ' + dataset)
            list_items = []
            
            if not out:
                return False
                
            # Handle different types of output
            if isinstance(out, list):
                if not out:  # Empty list
                    return False
                line = out[0]  # Use first line of list
            elif isinstance(out, str):
                line = out
            else:
                if self.verbose:
                    logger.warning(f"Unexpected type from zfs list command: {type(out)}")
                return False
                
            # Handle the case where line might not be splittable
            try:
                for i in line.split('\t'):
                    list_items.append(i)
            except (AttributeError, TypeError):
                if self.verbose:
                    logger.warning(f"Unable to split ZFS output: {line}")
                return False
                
            if list_items and list_items[0] == dataset:
                return True
            else:
                return False
        except Exception as e:
            if self.verbose:
                logger.error(f"Error checking if dataset exists: {e}")
            return False

    def get_snapshots(self, dataset):
        """Get list of snapshots for a dataset on remote host."""
        result = []
        out = self.execute('zfs list -t snapshot -H -o name ')
        if isinstance(out, list):
            lines = out
        else:
            lines = out.splitlines()
            
        for i in lines:
            if dataset == i.split('@')[0]:
                result.append(i.split('@')[1])

        return result

    def destroy(self, dataset):
        """Destroy a dataset on remote host."""
        out = self.execute('zfs destroy -r %s' % dataset)
        return out

    def get_remote_bootfs(self):
        """Get bootfs property from remote zpools."""
        list = {}
        out = self.execute('zpool get bootfs -H -p')
        if isinstance(out, list):
            lines = out
        else:
            lines = out.splitlines()
            
        for i in lines:
            if i.split('\t')[2] != '-':
                list[i.split('\t')[0]] = i.split('\t')[2]
        return list

    def zfs_set_remote(self, dataset, property, value):
        """Set ZFS property on remote dataset."""
        cmd = 'zfs set %s=%s %s' % (property, value, dataset)
        out = self.execute(cmd)
        return out

    def zfs_hold_remote(self, dataset, tag, snapshot):
        """Hold a snapshot on remote host."""
        cmd = 'zfs hold %s %s@%s' % (tag, dataset, snapshot)
        out = self.execute(cmd)
        return out

    def zfs_get_remote(self, dataset, property):
        """Get ZFS property from remote dataset."""
        cmd = 'zfs get -H -p %s %s' % (property, dataset)
        out = self.execute(cmd)
        
        # Handle empty result
        if not out:
            return None
        
        # Safely handle list output type
        if isinstance(out, list):
            if not out:  # Empty list
                return None
            line = out[0].rstrip()
        else:
            line = out.rstrip()
        
        # Parse the output
        try:
            parts = line.split('\t')
            if len(parts) >= 3 and parts[2] != '-':
                value = parts[2]
            else:
                value = None
            return value
        except Exception as e:
            # Log the error but don't use self.verbose since it might not be available
            logger.error(f"Error parsing ZFS property: {str(e)}")
            return None

    def cleanup_sync(self):
        """Clean up sync tags on snapshots."""
        holds = self.zfs.get_holds(self.dataset)
        host_holds = OrderedDict()

        for snap in holds.keys():
            tags = holds[snap]
            for tag in tags:
                try:
                    parts = tag.split('_')
                    if parts[0] == 'sync' and parts[2] == self.host:
                        host_holds[snap] = tag
                except:
                    pass

        if self.verbose and host_holds:
            logger.info('Found %i sync points for host %s' % (len(host_holds.keys()), self.host))
            last_snap = list(host_holds.keys())[-1]
            logger.info('Latest was snapshot %s ' % (last_snap))

        for i in list(host_holds.keys())[:-1]:
            self.zfs.release(self.dataset, i, host_holds[i])

    def send_snapshot(self):
        """Send snapshot to remote host."""
        if self.verbose:
            logger.info(f"Starting ZFS send to remote host {self.host}")
            
        if self.is_zfs(self.recv_dataset):
            # Try to get resume token from remote dataset using Paramiko
            try:
                resume_token = self.get_remote_zfs_property(self.recv_dataset, 'receive_resume_token')
            except Exception as e:
                if self.verbose:
                    logger.error(f"Error getting resume token: {str(e)}")
                resume_token = None
                
            # Only proceed with resume if we have a valid token
            if resume_token and resume_token != "-":
                if self.verbose:
                    logger.info('Resuming interrupted send')
                
                # Use adaptive_send with resume token for better consistency
                if self.verbose:
                    logger.info(f"Using resume token for ZFS send")
                
                # Use the adaptive_send function with resume_token parameter
                send = self.zfs.adaptive_send(dataset=self.dataset, snap=None, recurse=self.recurse,
                                         verbose=self.verbose, compression=self.compress_type,
                                         limit=self.limit, time=self.time, 
                                         use_native_compression=self.use_native_compression,
                                         resume_token=resume_token, machine_readable=self.machine_readable)
                
                if not send:
                    logger.error('Failed to create ZFS send process with resume token')
                    return
                
                if self.verbose:
                    logger.info(f"Using compression: {self.compress_type}")
                
                recv = uncompress_ssh_recv(send.stdout, host=self.host, type=self.compress_type, use_native_compression=self.use_native_compression,
                                       dataset=self.recv_dataset)

                # Capture stdout and stderr to diagnose issues
                stdout, stderr = recv.communicate()
                rc = recv.returncode
                if rc != 0:
                    logger.warning('Resuming interrupted send failed. Trying basic incremental send')
                    if stdout:
                        logger.info(f"Remote stdout: {stdout.decode('utf-8') if isinstance(stdout, bytes) else stdout}")
                    if stderr:
                        logger.error(f"Remote stderr: {stderr.decode('utf-8') if isinstance(stderr, bytes) else stderr}")
                    
                    # Get focused diagnostic information about the resume token
                    logger.info("Checking receive_resume_token...")
                    token_cmd = f"ssh -oStrictHostKeyChecking=no root@{self.host} 'zfs get -H receive_resume_token {self.recv_dataset}'"
                    token_out, token_err, _ = execute(token_cmd, shell=True)
                    if token_out:
                        logger.info(f"Resume token: {token_out}")
                    
                    # Check for partial receive state
                    recv_cmd = f"ssh -oStrictHostKeyChecking=no root@{self.host} 'zfs list {self.recv_dataset}/%recv 2>&1 || echo \"No partial receive dataset found\"'"
                    recv_out, recv_err, _ = execute(recv_cmd, shell=True)
                    if "No partial receive dataset found" not in recv_out:
                        logger.info(f"Partial receive dataset exists: {recv_out}")
                    
                    # Try to run a command to abort any partial receive state
                    logger.info("Attempting to abort partial receive state...")
                    abort_cmd = f"ssh -oStrictHostKeyChecking=no root@{self.host} 'zfs receive -A {self.recv_dataset}; echo $?'"
                    abort_out, abort_err, abort_rc = execute(abort_cmd, shell=True)
                    logger.info(f"Abort result: {abort_out}")
                    if abort_err:
                        logger.error(f"Abort errors: {abort_err}")
                        
                        # If dataset is busy, collect diagnostic info without trying to fix it
                        if "dataset is busy" in abort_err:
                            logger.warning("Dataset is busy, collecting diagnostic information...")
                            
                            # Check what processes might be using the dataset
                            proc_cmd = f"ssh -oStrictHostKeyChecking=no root@{self.host} 'ls -l /proc/*/fd/* 2>/dev/null | grep %recv || echo \"No process FDs found\"'"
                            proc_out, _, _ = execute(proc_cmd, shell=True)
                            logger.info(f"Processes using dataset: {proc_out}")
                            
                            # Check mounted datasets
                            mount_cmd = f"ssh -oStrictHostKeyChecking=no root@{self.host} 'mount | grep zfs | grep {self.recv_dataset}'"
                            mount_out, _, _ = execute(mount_cmd, shell=True)
                            logger.info(f"Mount information: {mount_out}")
                            
                            # ZFS holds
                            holds_cmd = f"ssh -oStrictHostKeyChecking=no root@{self.host} 'zfs get -H all {self.recv_dataset}/%recv 2>&1 || echo \"Could not get dataset properties\"'"
                            holds_out, _, _ = execute(holds_cmd, shell=True)
                            logger.info(f"Dataset properties: {holds_out}")
                    
                    # Try to report on the resume token but don't clear it
                    token_cmd = f"ssh -oStrictHostKeyChecking=no root@{self.host} 'zfs get -H receive_resume_token {self.recv_dataset}'"
                    token_out, _, _ = execute(token_cmd, shell=True)
                    logger.info(f"Current token status: {token_out}")
                    recv_snaps = self.get_snapshots(self.recv_dataset)
                    if recv_snaps:
                        source_snaps = self.zfs.get_snapshots(self.dataset)
                        snap, snap1 = self.zfs.engociate_inc_send(source_snaps, recv_snaps)

                        if snap != snap1:
                            if self.verbose:
                                logger.info('Performing incremental send from %s to %s' % (snap, snap1))

                            send = self.zfs.adaptive_send(dataset=self.dataset, snap=snap, snap1=snap1, recurse=self.recurse,
                                                     verbose=self.verbose, compression=self.compress_type,
                                                     limit=self.limit, time=self.time, use_native_compression=self.use_native_compression)

                            if self.verbose:
                                logger.info(f"Using compression: {self.compress_type} with pv for progress visualization")
                                
                            recv = uncompress_ssh_recv(send.stdout, host=self.host, type=self.compress_type, use_native_compression=self.use_native_compression, dataset=self.recv_dataset)

                            recv.communicate()
                            if recv.returncode != 0:
                                logger.error(f"Remote ZFS receive failed with return code {recv.returncode}")
                                return 1

            else:
                recv_snaps = self.get_snapshots(self.recv_dataset)

                if recv_snaps:
                    source_snaps = self.zfs.get_snapshots(self.dataset)
                    snap, snap1 = self.zfs.engociate_inc_send(source_snaps, recv_snaps)

                    if snap and snap1 and snap != snap1:
                        if self.verbose:
                            logger.info('Performing incremental send from %s to %s' % (snap, snap1))

                        send = self.zfs.adaptive_send(dataset=self.dataset, snap=snap, snap1=snap1, recurse=self.recurse,
                                                 verbose=self.verbose, compression=self.compress_type,
                                                 limit=self.limit, time=self.time, use_native_compression=self.use_native_compression,
                                                 machine_readable=self.machine_readable)

                        if self.verbose:
                            logger.info(f"Using compression: {self.compress_type} with pv for visualization")

                        recv = uncompress_ssh_recv(send.stdout, host=self.host, type=self.compress_type, use_native_compression=self.use_native_compression, dataset=self.recv_dataset)

                        logger.info("Waiting for receive to complete...")
                        recv.communicate()

                        if self.sync:
                            tag = "sync" + '_' + datetime.now().strftime('%Y-%m-%d-%H-%M-%S') + '_' + self.host
                            logger.info(f"SYNC: Creating holds with tag '{tag}' for snapshot '{snap1}'")
                            logger.info(f"SYNC: Local dataset='{self.dataset}', Remote dataset='{self.recv_dataset}'")
                            
                            local_rc = self.zfs.hold(self.dataset, snapshot=snap1, tag=tag)
                            logger.info(f"SYNC: Local hold result: {local_rc}")
                            
                            remote_out = self.zfs_hold_remote(self.recv_dataset, snapshot=snap1, tag=tag)
                            logger.info(f"SYNC: Remote hold result: {remote_out}")
                            
                            self.cleanup_sync()
                            logger.info(f"SYNC: Cleanup completed")

                        if self.update_bootfs:
                            pool = self.recv_dataset.split('/')[0]
                            bootfs = self.recv_dataset + '@' + snap1
                            cmd = 'zpool set bootfs=%s %s' % (bootfs, pool)
                            out = self.execute(cmd)

                        if self.backup_stream:
                            self.zfs_set_remote(self.recv_dataset, 'control:autobackup', 'passive')

                    else:
                        # Check if snap is None (no common snapshots)
                        if snap is None:
                            if self.verbose:
                                logger.info('No common snapshots found between source and destination')
                            # If no common snapshots and create_snapshot is enabled, do a full send
                            if self.create_snapshot:
                                now = datetime.now()
                                date = now.strftime('%y%m%d')
                                hour = now.strftime('%H')
                                minsec = now.strftime('%M%S')
                                snap1 = 'migrate-%s-%s-%s' % (date, hour, minsec)
                                if self.verbose:
                                    logger.info('Creating snapshot for send %s' % snap1)
                                self.zfs.snapshot(self.dataset, snap1, self.recurse)
                                if self.verbose:
                                    logger.info('Sending full snapshot %s' % snap1)
                                # Full send since no common snapshot
                                send = self.zfs.adaptive_send(dataset=self.dataset, snap=None, snap1=snap1,
                                                          recurse=self.recurse,
                                                          verbose=self.verbose, compression=self.compress_type,
                                                          limit=self.limit, time=self.time,
                                                          use_native_compression=self.use_native_compression,
                                                          machine_readable=self.machine_readable)
                                recv = uncompress_ssh_recv(send.stdout, host=self.host, type=self.compress_type, use_native_compression=self.use_native_compression,
                                                       dataset=self.recv_dataset)
                                recv.communicate()
                                
                                if self.sync:
                                    tag = "sync" + '_' + datetime.now().strftime('%Y-%m-%d-%H-%M-%S') + '_' + self.host
                                    logger.info(f"SYNC: (incremental path) Creating holds with tag '{tag}' for snapshot '{snap1}'")
                                    logger.info(f"SYNC: Local dataset='{self.dataset}', Remote dataset='{self.recv_dataset}'")
                                    
                                    local_rc = self.zfs.hold(self.dataset, snapshot=snap1, tag=tag)
                                    logger.info(f"SYNC: Local hold result: {local_rc}")
                                    
                                    remote_out = self.zfs_hold_remote(self.recv_dataset, snapshot=snap1, tag=tag)
                                    logger.info(f"SYNC: Remote hold result: {remote_out}")
                                    
                                    self.cleanup_sync()
                                    logger.info(f"SYNC: Cleanup completed")

                                if self.backup_stream:
                                    self.zfs_set_remote(self.recv_dataset, 'control:autobackup', 'passive')
                            else:
                                # No common snapshots and create_snapshot is disabled
                                # Check if we got here because dataset exists but has no common snapshots
                                # In that case, we should abort. Only proceed if dataset doesn't exist.
                                # recv_snaps being empty despite is_zfs returning true means dataset exists but empty
                                logger.error('No common snapshots found between source and destination datasets')
                                logger.error('Cannot perform incremental send. Aborting.')
                                return
                        else:
                            if self.verbose:
                                logger.info('Target dataset is up to date on %s' % snap)

                            if self.create_snapshot:
                                now = datetime.now()
                                date = now.strftime('%y%m%d')
                                hour = now.strftime('%H')
                                minsec = now.strftime('%M%S')
                                snap1 = 'migrate-%s-%s-%s' % (date, hour, minsec)
                                if self.verbose:
                                    logger.info('Creating snapshot for send %s' % snap1)
                                self.zfs.snapshot(self.dataset, snap1, self.recurse)
                                if self.verbose:
                                    logger.info('Performing incremental send from %s to %s' % (snap, snap1))
                                send = self.zfs.adaptive_send(dataset=self.dataset, snap=snap, snap1=snap1,
                                                          recurse=self.recurse,
                                                          verbose=self.verbose, compression=self.compress_type,
                                                          limit=self.limit, time=self.time,
                                                          use_native_compression=self.use_native_compression,
                                                          machine_readable=self.machine_readable)
                                recv = uncompress_ssh_recv(send.stdout, host=self.host, type=self.compress_type, use_native_compression=self.use_native_compression,
                                                       dataset=self.recv_dataset)

                                recv.communicate()

                                if self.sync:
                                    tag = "sync" + '_' + datetime.now().strftime('%Y-%m-%d-%H-%M-%S') + '_' + self.host
                                    logger.info(f"SYNC: (full send path) Creating holds with tag '{tag}' for snapshot '{snap1}'")
                                    logger.info(f"SYNC: Local dataset='{self.dataset}', Remote dataset='{self.recv_dataset}'")
                                    
                                    local_rc = self.zfs.hold(self.dataset, snapshot=snap1, tag=tag)
                                    logger.info(f"SYNC: Local hold result: {local_rc}")
                                    
                                    remote_out = self.zfs_hold_remote(self.recv_dataset, snapshot=snap1, tag=tag)
                                    logger.info(f"SYNC: Remote hold result: {remote_out}")
                                    
                                    self.cleanup_sync()
                                    logger.info(f"SYNC: Cleanup completed")

                                if self.backup_stream:
                                    self.zfs_set_remote(self.recv_dataset, 'control:autobackup', 'passive')

                else:
                    if self.verbose:
                        logger.warning('Unable to setup incremental stream for zfs send. Recv dataset has no snapshots')
                    pass

        else:
            if self.snapshot and self.snapshot in self.zfs.get_snapshots(self.dataset):
                if self.verbose:
                    logger.info('Sending full snapshot %s' % self.snapshot)
                send = self.zfs.adaptive_send(dataset=self.dataset, snap=self.snapshot, recurse=self.recurse, verbose=self.verbose,
                                         machine_readable=self.machine_readable,
                                         compression=self.compress_type,
                                         limit=self.limit, time=self.time,
                                         use_native_compression=self.use_native_compression)

                recv = uncompress_ssh_recv(send.stdout, host=self.host, type=self.compress_type, use_native_compression=self.use_native_compression, dataset=self.recv_dataset)

                recv.communicate()
                
                if self.sync:
                    tag = "sync" + '_' + datetime.now().strftime('%Y-%m-%d-%H-%M-%S') + '_' + self.host
                    logger.info(f"SYNC: (full snapshot existing) Creating holds with tag '{tag}' for snapshot '{self.snapshot}'")
                    logger.info(f"SYNC: Local dataset='{self.dataset}', Remote dataset='{self.recv_dataset}'")
                    
                    local_rc = self.zfs.hold(self.dataset, snapshot=self.snapshot, tag=tag)
                    logger.info(f"SYNC: Local hold result: {local_rc}")
                    
                    remote_out = self.zfs_hold_remote(self.recv_dataset, snapshot=self.snapshot, tag=tag)
                    logger.info(f"SYNC: Remote hold result: {remote_out}")
                    
                    self.cleanup_sync()
                    logger.info(f"SYNC: Cleanup completed")
                
                if self.backup_stream:
                    self.zfs_set_remote(self.recv_dataset, 'control:autobackup', 'passive')
            else:
                if not self.create_snapshot and self.zfs.get_snapshots(self.dataset):
                    snap_list = self.zfs.get_snapshots(self.dataset)
                    if snap_list:
                        snap = snap_list[-1]
                        if self.verbose:
                            logger.info('Sending full snapshot %s' % snap)
                        send = self.zfs.adaptive_send(dataset=self.dataset, snap=None, snap1=snap, recurse=self.recurse,
                                                  verbose=self.verbose,
                                                  compression=self.compress_type,
                                                  limit=self.limit, time=self.time,
                                                  use_native_compression=self.use_native_compression,
                                                  machine_readable=self.machine_readable)

                        recv = uncompress_ssh_recv(send.stdout, host=self.host, type=self.compress_type, use_native_compression=self.use_native_compression,
                                               dataset=self.recv_dataset)

                        recv.communicate()

                        if self.sync:
                            tag = "sync" + '_' + datetime.now().strftime('%Y-%m-%d-%H-%M-%S') + '_' + self.host
                            logger.info(f"SYNC: (existing snapshot path) Creating holds with tag '{tag}' for snapshot '{snap}'")
                            logger.info(f"SYNC: Local dataset='{self.dataset}', Remote dataset='{self.recv_dataset}'")
                            
                            local_rc = self.zfs.hold(self.dataset, snapshot=snap, tag=tag)
                            logger.info(f"SYNC: Local hold result: {local_rc}")
                            
                            remote_out = self.zfs_hold_remote(self.recv_dataset, snapshot=snap, tag=tag)
                            logger.info(f"SYNC: Remote hold result: {remote_out}")
                            
                            self.cleanup_sync()
                            logger.info(f"SYNC: Cleanup completed")

                        if self.backup_stream:
                            self.zfs_set_remote(self.recv_dataset, 'control:autobackup', 'passive')

                else:
                    now = datetime.now()
                    date = now.strftime('%y%m%d')
                    hour = now.strftime('%H')
                    minsec = now.strftime('%M%S')
                    snap_name = 'migrate-%s-%s-%s' % (date, hour, minsec)
                    if self.verbose:
                        logger.info('Creating snapshot for send %s' % snap_name)
                    self.zfs.snapshot(self.dataset, snap_name, self.recurse)

                    if self.verbose:
                        logger.info('Sending full snapshot %s' % snap_name)

                    send = self.zfs.adaptive_send(dataset=self.dataset, snap=snap_name, recurse=self.recurse,
                                              verbose=self.verbose,
                                              compression=self.compress_type,
                                              limit=self.limit, time=self.time,
                                              use_native_compression=self.use_native_compression,
                                              machine_readable=self.machine_readable)

                    if self.verbose:
                        logger.info(f"Using compression: {self.compress_type} with pv for visualization")
                        
                    recv = uncompress_ssh_recv(send.stdout, host=self.host, type=self.compress_type, use_native_compression=self.use_native_compression,
                                           dataset=self.recv_dataset)
                                           
                    logger.info("Waiting for receive to complete...")
                    recv.communicate()

                    if self.oneshot:
                        if self.verbose:
                            logger.info('Oneshot send. Cleaning up snapshot %s' % snap_name)
                        self.zfs.destroy(self.dataset + '@' + snap_name, recurse=self.recurse)
                        self.destroy(self.recv_dataset + '@' + snap_name)

                    elif self.sync:
                        tag = "sync" + '_' + datetime.now().strftime('%Y-%m-%d-%H-%M-%S') + '_' + self.host
                        logger.info(f"SYNC: (new snapshot path) Creating holds with tag '{tag}' for snapshot '{snap_name}'")
                        logger.info(f"SYNC: Local dataset='{self.dataset}', Remote dataset='{self.recv_dataset}'")
                        
                        local_rc = self.zfs.hold(self.dataset, snapshot=snap_name, tag=tag)
                        logger.info(f"SYNC: Local hold result: {local_rc}")
                        
                        remote_out = self.zfs_hold_remote(self.recv_dataset, snapshot=snap_name, tag=tag)
                        logger.info(f"SYNC: Remote hold result: {remote_out}")
                        
                        self.cleanup_sync()
                        logger.info(f"SYNC: Cleanup completed")

                    if self.backup_stream:
                        self.zfs_set_remote(self.recv_dataset, 'control:autobackup', 'passive')
        
        return 0

class SSHReceive(SSH):
    def __init__(self, host, dataset=None, recv_dataset=None, snapshot=None, compress_type='auto', 
                limit=0, time=0, recurse=False, verbose=False, oneshot=False, create_snapshot=False, 
                update_bootfs=False, passwd=None):
        """
        Initialize parameters for receiving ZFS snapshots from a remote host.
        """
        # Initialize the SSH base class
        super().__init__(host=host, user="root", passwd=passwd)
        
        self.dataset = dataset
        self.recv_dataset = recv_dataset
        self.snapshot = snapshot
        self.compress_type = compress_type
        self.limit = limit
        self.time = time
        self.recurse = recurse
        self.verbose = verbose
        self.oneshot = oneshot
        self.create_snapshot = create_snapshot
        self.host = host
        self.update_bootfs = update_bootfs
        self.state = 0
        self.use_native_compression = False
        
        self.zfs = zfs()
        self.zpool = zpool()
        
        if self.verbose:
            logger.info(f"Initializing SSH receive from {host}:{dataset} to {recv_dataset}")
        
        # Auto-detect compression
        self.get_auto_compression()
        
        # Set the state to 1 to indicate success
        self.state = 1
        
    def get_auto_compression(self):
        """Auto-detect best compression algorithm."""
        if self.compress_type == 'auto':
            out, err, rc = execute(['zstd', '-h'])
            if rc == 0:
                logger.info('Found auto compression type: zstd')
                self.compress_type = 'zstd'
            else:
                out, err, rc = execute(['lz4c', '-h'])
                if rc == 0:
                    logger.info('Found auto compression type: lz4')
                    self.compress_type = 'lz4'
                else:
                    logger.info('No compression type found, using uncompressed stream')
                    self.compress_type = None
                    
    def check_compression_support(self):
        """
        Check if native ZFS compression can be used based on ZFS versions and dataset compression.
        
        This implements the negotiation logic:
        1. If source is not a ZFS dataset, use existing behavior
        2. If source is a ZFS dataset:
           a. Check ZFS versions on both sides (local and remote)
           b. Check if source dataset has compression enabled
           c. Use native compression if versions >= 2.0 and source has compression
        """
        # Default to not using native compression
        self.use_native_compression = False
        
        # Check if source is a ZFS dataset
        if not self.check_remote_dataset_exists(self.dataset):
            if self.verbose:
                logger.warning("Source dataset does not exist, using external compression if needed")
            return
        
        # Get ZFS version on local system
        local_zfs_version = self.zfs.get_zfs_version()
        if not local_zfs_version:
            if self.verbose:
                logger.warning("Unable to detect local ZFS version, using external compression")
            return
        
        # Get ZFS version on remote system
        remote_zfs_version = self.zfs.get_zfs_version(remote_host=self.host)
        if not remote_zfs_version:
            if self.verbose:
                logger.warning(f"Unable to detect ZFS version on remote host {self.host}, using external compression")
            return
            
        # Check if both versions support native compression (version >= 2.0)
        try:
            local_major_version = int(str(local_zfs_version).split('.')[0])
            remote_major_version = int(str(remote_zfs_version).split('.')[0])
            
            if local_major_version >= 2 and remote_major_version >= 2:
                # Both systems support native compression
                # Get compression setting on remote dataset
                compression_setting = self.get_remote_zfs_property(self.dataset, 'compression')
                if compression_setting and compression_setting not in ['off', 'none']:
                    # Dataset has compression enabled, use native compression
                    self.use_native_compression = True
                    if self.verbose:
                        logger.info(f"Using ZFS native compression: remote={remote_zfs_version}, local={local_zfs_version}, dataset compression={compression_setting}")
                else:
                    if self.verbose:
                        logger.info(f"Remote dataset compression is disabled ({compression_setting}), using external compression if needed")
            else:
                if self.verbose:
                    logger.info(f"Native compression not supported by ZFS versions (local={local_zfs_version}, remote={remote_zfs_version}), using external compression if needed")
        except Exception as e:
            if self.verbose:
                logger.warning(f"Error checking ZFS version compatibility: {str(e)}")
            # Fall back to external compression on errors
            self.use_native_compression = False
    
    def create_remote_snapshot(self, dataset, recurse=False):
        """
        Create a snapshot on the remote host with a standardized name format.
        
        Args:
            dataset: ZFS dataset to snapshot
            recurse: Whether to create snapshot recursively
            
        Returns:
            str: Name of the created snapshot or None on failure
        """
        if self.state != 1:
            logger.error("SSH connection not established")
            return None
            
        try:
            # Generate snapshot name with standard format: migrate-YYMMDD-HH-MMSS
            now = datetime.now()
            date = now.strftime('%y%m%d')
            hour = now.strftime('%H')
            minsec = now.strftime('%M%S')
            snap_name = f'migrate-{date}-{hour}-{minsec}'
            
            # Build command with or without recursion flag
            if recurse:
                cmd = f'zfs snapshot -r {dataset}@{snap_name}'
            else:
                cmd = f'zfs snapshot {dataset}@{snap_name}'
                
            # Execute the snapshot command
            output = self.execute(cmd)
            
            # Check for errors in the output
            if any('error' in line.lower() for line in output):
                error_msg = '; '.join(output)
                logger.error(f"Failed to create snapshot on remote host: {error_msg}")
                return None
                
            if self.verbose:
                logger.info(f"Created snapshot {snap_name} on remote dataset {dataset}")
                
            return snap_name
            
        except Exception as e:
            logger.error(f"Error creating remote snapshot: {str(e)}")
            return None
                    
    def format_size(self, size):
        """Format size in bytes to human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
            if size < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} PB"
        
    def get_remote_send_size(self, dataset_name, snapshot, snapshot2=None):
        """Get the estimated size of a ZFS send operation from a remote host."""
        if self.verbose:
            logger.debug(f"Getting size estimate for {dataset_name}@{snapshot}")
            
        try:
            # For full send (single snapshot)
            if not snapshot2:
                size_cmd = f"ssh -oStrictHostKeyChecking=no root@{self.host} 'zfs send -nv {dataset_name}@{snapshot}'"
                if self.verbose:
                    logger.debug(f"Getting size estimate with: {size_cmd}")
                    
                out, err, rc = execute(size_cmd, shell=True)
                
                if rc != 0:
                    if self.verbose:
                        logger.error(f"Error getting size estimation: {err}")
                        logger.debug(f"Output: {out}")
                    return None
                
                # Parse the size from the output, which should be in the last line
                # Format is typically: "total estimated size is 123456789 (123GB)"
                if err:
                    for line in err.splitlines():
                        if "estimated size" in line or "size is" in line:
                            # Extract the number and unit from the line (e.g., "251G", "1.5T")
                            size_match = re.search(r'size is (\d+\.?\d*)([KMGTP]?)', line)
                            if size_match:
                                size_num = float(size_match.group(1))
                                unit = size_match.group(2)
                                # Convert to bytes based on unit
                                if unit == 'K':
                                    return int(size_num * 1024)
                                elif unit == 'M':
                                    return int(size_num * 1024 * 1024)
                                elif unit == 'G':
                                    return int(size_num * 1024 * 1024 * 1024)
                                elif unit == 'T':
                                    return int(size_num * 1024 * 1024 * 1024 * 1024)
                                elif unit == 'P':
                                    return int(size_num * 1024 * 1024 * 1024 * 1024 * 1024)
                                else:
                                    return int(size_num)  # Assume bytes if no unit
                            
                            # Try alternate formats (just in case)
                            size_match = re.search(r'size.*?(\d+\.?\d*)([KMGTP]?)', line)
                            if size_match:
                                size_num = float(size_match.group(1))
                                unit = size_match.group(2)
                                # Convert to bytes based on unit
                                if unit == 'K':
                                    return int(size_num * 1024)
                                elif unit == 'M':
                                    return int(size_num * 1024 * 1024)
                                elif unit == 'G':
                                    return int(size_num * 1024 * 1024 * 1024)
                                elif unit == 'T':
                                    return int(size_num * 1024 * 1024 * 1024 * 1024)
                                elif unit == 'P':
                                    return int(size_num * 1024 * 1024 * 1024 * 1024 * 1024)
                                else:
                                    return int(size_num)  # Assume bytes if no unit
                                
                # Check stdout too as some ZFS versions output to stdout instead of stderr
                if out:
                    for line in out.splitlines():
                        if "estimated size" in line or "size is" in line:
                            # Extract the number and unit from the line (e.g., "251G", "1.5T")
                            size_match = re.search(r'size is (\d+\.?\d*)([KMGTP]?)', line)
                            if size_match:
                                size_num = float(size_match.group(1))
                                unit = size_match.group(2)
                                # Convert to bytes based on unit
                                if unit == 'K':
                                    return int(size_num * 1024)
                                elif unit == 'M':
                                    return int(size_num * 1024 * 1024)
                                elif unit == 'G':
                                    return int(size_num * 1024 * 1024 * 1024)
                                elif unit == 'T':
                                    return int(size_num * 1024 * 1024 * 1024 * 1024)
                                elif unit == 'P':
                                    return int(size_num * 1024 * 1024 * 1024 * 1024 * 1024)
                                else:
                                    return int(size_num)  # Assume bytes if no unit
                            
                            # Try alternate formats (just in case)
                            size_match = re.search(r'size.*?(\d+\.?\d*)([KMGTP]?)', line)
                            if size_match:
                                size_num = float(size_match.group(1))
                                unit = size_match.group(2)
                                # Convert to bytes based on unit
                                if unit == 'K':
                                    return int(size_num * 1024)
                                elif unit == 'M':
                                    return int(size_num * 1024 * 1024)
                                elif unit == 'G':
                                    return int(size_num * 1024 * 1024 * 1024)
                                elif unit == 'T':
                                    return int(size_num * 1024 * 1024 * 1024 * 1024)
                                elif unit == 'P':
                                    return int(size_num * 1024 * 1024 * 1024 * 1024 * 1024)
                                else:
                                    return int(size_num)  # Assume bytes if no unit
            
            # For incremental send (two snapshots)
            else:
                size_cmd = f"ssh -oStrictHostKeyChecking=no root@{self.host} 'zfs send -nv -i {dataset_name}@{snapshot} {dataset_name}@{snapshot2}'"
                if self.verbose:
                    logger.debug(f"Getting incremental size estimate with: {size_cmd}")
                    
                out, err, rc = execute(size_cmd, shell=True)
                
                if rc != 0:
                    if self.verbose:
                        logger.error(f"Error getting incremental size estimation: {err}")
                    return None
                
                # Parse the size using the same approach as full send
                if err:
                    for line in err.splitlines():
                        if "estimated size" in line or "size is" in line:
                            # Extract the number and unit from the line (e.g., "251G", "1.5T")
                            size_match = re.search(r'size is (\d+\.?\d*)([KMGTP]?)', line)
                            if size_match:
                                size_num = float(size_match.group(1))
                                unit = size_match.group(2)
                                # Convert to bytes based on unit
                                if unit == 'K':
                                    return int(size_num * 1024)
                                elif unit == 'M':
                                    return int(size_num * 1024 * 1024)
                                elif unit == 'G':
                                    return int(size_num * 1024 * 1024 * 1024)
                                elif unit == 'T':
                                    return int(size_num * 1024 * 1024 * 1024 * 1024)
                                elif unit == 'P':
                                    return int(size_num * 1024 * 1024 * 1024 * 1024 * 1024)
                                else:
                                    return int(size_num)  # Assume bytes if no unit
                            
                            # Try alternate formats (just in case)
                            size_match = re.search(r'size.*?(\d+\.?\d*)([KMGTP]?)', line)
                            if size_match:
                                size_num = float(size_match.group(1))
                                unit = size_match.group(2)
                                # Convert to bytes based on unit
                                if unit == 'K':
                                    return int(size_num * 1024)
                                elif unit == 'M':
                                    return int(size_num * 1024 * 1024)
                                elif unit == 'G':
                                    return int(size_num * 1024 * 1024 * 1024)
                                elif unit == 'T':
                                    return int(size_num * 1024 * 1024 * 1024 * 1024)
                                elif unit == 'P':
                                    return int(size_num * 1024 * 1024 * 1024 * 1024 * 1024)
                                else:
                                    return int(size_num)  # Assume bytes if no unit
                                
                # Check stdout too as some ZFS versions output to stdout instead of stderr
                if out:
                    for line in out.splitlines():
                        if "estimated size" in line or "size is" in line:
                            # Extract the number and unit from the line (e.g., "251G", "1.5T")
                            size_match = re.search(r'size is (\d+\.?\d*)([KMGTP]?)', line)
                            if size_match:
                                size_num = float(size_match.group(1))
                                unit = size_match.group(2)
                                # Convert to bytes based on unit
                                if unit == 'K':
                                    return int(size_num * 1024)
                                elif unit == 'M':
                                    return int(size_num * 1024 * 1024)
                                elif unit == 'G':
                                    return int(size_num * 1024 * 1024 * 1024)
                                elif unit == 'T':
                                    return int(size_num * 1024 * 1024 * 1024 * 1024)
                                elif unit == 'P':
                                    return int(size_num * 1024 * 1024 * 1024 * 1024 * 1024)
                                else:
                                    return int(size_num)  # Assume bytes if no unit
                            
                            # Try alternate formats (just in case)
                            size_match = re.search(r'size.*?(\d+\.?\d*)([KMGTP]?)', line)
                            if size_match:
                                size_num = float(size_match.group(1))
                                unit = size_match.group(2)
                                # Convert to bytes based on unit
                                if unit == 'K':
                                    return int(size_num * 1024)
                                elif unit == 'M':
                                    return int(size_num * 1024 * 1024)
                                elif unit == 'G':
                                    return int(size_num * 1024 * 1024 * 1024)
                                elif unit == 'T':
                                    return int(size_num * 1024 * 1024 * 1024 * 1024)
                                elif unit == 'P':
                                    return int(size_num * 1024 * 1024 * 1024 * 1024 * 1024)
                                else:
                                    return int(size_num)  # Assume bytes if no unit
            
            return None
        except Exception as e:
            if self.verbose:
                logger.error(f"Exception during size estimation: {str(e)}")
            return None
                    
    def receive_snapshot(self):
        """
        Receive a snapshot from a remote host.
        
        Algorithm:
        1. Check local dataset/file configuration
        2. Check remote host and dataset configuration
        3. Determine transfer parameters (snapshots, compression)
        4. Set up send and receive processes
        5. Execute transfer with monitoring
        6. Handle post-transfer operations
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if self.verbose:
                logger.info(f"Starting receive snapshot process from {self.host}:{self.dataset}")
            
            # ============= STEP 1: Check local dataset/file configuration =============
            
            # Determine if destination is a file or ZFS dataset
            is_file_destination = self.recv_dataset.startswith('/')
            resume_token = None
            local_snapshots = []
            
            # For ZFS datasets (not files), check if it exists and get snapshots/resume token
            if not is_file_destination:
                # Check if the local dataset exists
                if self.zfs.is_zfs(self.recv_dataset):
                    if self.verbose:
                        logger.info(f"Local dataset {self.recv_dataset} exists, checking for resume token or snapshots")
                    
                    # Get resume token from local dataset using ZFS class
                    resume_token_raw = self.zfs.get(self.recv_dataset, 'receive_resume_token')
                    if resume_token_raw and resume_token_raw != "-":
                        resume_token = resume_token_raw
                        if self.verbose:
                            logger.info(f"Found resume token for dataset {self.recv_dataset}")
                    
                    # If no resume token, get local snapshots for incremental send negotiation
                    if not resume_token:
                        local_snapshots = self.zfs.get_snapshots(self.recv_dataset)
                        if self.verbose and local_snapshots:
                            logger.info(f"Found {len(local_snapshots)} local snapshots for incremental negotiation")
                    
                else:
                    if self.verbose:
                        logger.info(f"Local dataset {self.recv_dataset} does not exist, will create it")
            else:
                if self.verbose:
                    logger.info(f"Destination is a file: {self.recv_dataset}")
                    
            # ============= STEP 2: Check remote host and dataset configuration =============
            
            # Verify remote dataset exists and get snapshots
            if not self.check_remote_dataset_exists(self.dataset):
                logger.error(f"Error: Remote dataset {self.dataset} not found")
                return False
            
            # Get remote snapshots for either specified snapshot or negotiation
            remote_snapshots = self.list_remote_snapshots(self.dataset)
            if not remote_snapshots:
                logger.error(f"Error: No snapshots found on remote dataset {self.dataset}")
                return False
            
            # ============= STEP 3: Determine transfer parameters (snapshots, compression) =============
            
            # Initialize transfer parameters
            remote_snap = None
            remote_snap1 = None
            
            # If create_snapshot flag is set, create a new snapshot on the remote host
            if self.create_snapshot:
                if self.verbose:
                    logger.info(f"Creating new snapshot on remote dataset {self.dataset}")
                
                # Create snapshot on remote host
                new_snapshot = self.create_remote_snapshot(self.dataset, self.recurse)
                if not new_snapshot:
                    logger.error("Failed to create snapshot on remote host")
                    return False
                
                # Update remote snapshots list
                remote_snapshots.append(new_snapshot)
                remote_snapshots.sort()
                
                # Set as the latest snapshot to use
                remote_snap = new_snapshot
                if self.verbose:
                    logger.info(f"Using newly created snapshot: {remote_snap}")
                
                # If we have local snapshots, try to find common one for incremental send
                if not is_file_destination and local_snapshots:
                    # Use engociate_inc_send to find common snapshots for incremental send
                    common_snap, _ = self.zfs.engociate_inc_send(remote_snapshots, local_snapshots)
                    if common_snap and common_snap != remote_snap:
                        # Found a common snapshot for incremental send
                        remote_snap1 = remote_snap  # New snapshot is the target
                        remote_snap = common_snap   # Common snapshot is the source
                        if self.verbose:
                            logger.info(f"Using incremental transfer: {remote_snap} to {remote_snap1}")
            # If a specific snapshot was requested, verify it exists
            elif self.snapshot:
                if self.snapshot not in remote_snapshots:
                    logger.error(f"Error: Specified snapshot {self.snapshot} not found on remote dataset")
                    return False
                remote_snap = self.snapshot
            else:
                # If we have resume token, just use that without specifying snapshots
                if resume_token:
                    if self.verbose:
                        logger.info("Using resume token for continuing interrupted transfer")
                # Otherwise, determine snapshot(s) to use
                else:
                    # Find common snapshots for incremental transfer
                    if not is_file_destination and local_snapshots:
                        # Use engociate_inc_send to find common snapshots
                        common_snap, latest_remote_snap = self.zfs.engociate_inc_send(remote_snapshots, local_snapshots)
                        
                        if common_snap and latest_remote_snap:
                            if common_snap == latest_remote_snap:
                                # Common snapshot and latest remote snapshot are the same
                                # Dataset is already up to date, abort transfer
                                logger.info(f"Dataset {self.recv_dataset} is already up to date with latest snapshot {latest_remote_snap}. Aborting transfer.")
                                return True
                            else:
                                # Found snapshots for incremental transfer
                                remote_snap = common_snap
                                remote_snap1 = latest_remote_snap
                                if self.verbose:
                                    logger.info(f"Using incremental transfer: {remote_snap} to {remote_snap1}")
                        else:
                            # No viable incremental transfer, use latest snapshot
                            remote_snapshots.sort()
                            remote_snap = remote_snapshots[-1]
                            if self.verbose:
                                logger.info(f"Using full transfer with snapshot: {remote_snap}")
                    else:
                        # For file destinations or new datasets, use latest snapshot
                        remote_snapshots.sort()
                        remote_snap = remote_snapshots[-1]
                        if self.verbose:
                            logger.info(f"Using latest remote snapshot: {remote_snap}")
            
            # Get transfer size estimation from remote host for progress display
            try:
                if resume_token:
                    # Cannot estimate size for resume token transfers
                    self._transfer_size = 0
                elif remote_snap1:
                    # Incremental send size - use estimate_send_size from SSH class
                    self._transfer_size = self.estimate_send_size(self.dataset, remote_snap, remote_snap1)
                else:
                    # Full send size - use estimate_send_size from SSH class
                    self._transfer_size = self.estimate_send_size(self.dataset, remote_snap)
                    
                if self._transfer_size:
                    logger.info(f"Estimated transfer size: {self._transfer_size} bytes ({self.format_size(self._transfer_size)})")
            except Exception as e:
                logger.warning(f"Unable to estimate transfer size: {str(e)}")
                self._transfer_size = 0
            
            # Negotiate compression - use native if available, otherwise external
            self.check_compression_support()  # This updates self.use_native_compression
                
            # ============= STEP 4: Set up send and receive processes =============
            
            if self.verbose:
                logger.info(f"Starting remote ZFS send using adaptive_send_remote")
                if self.use_native_compression:
                    logger.info("Using ZFS native compression for transfer")
                elif self.compress_type:
                    logger.info(f"Using external {self.compress_type} compression for transfer")
                else:
                    logger.info("Using uncompressed transfer")
            
            # Create the remote send process
            ssh_proc = self.zfs.adaptive_send_remote(
                host=self.host,
                dataset=self.dataset,
                snap=remote_snap,
                snap1=remote_snap1,
                recurse=self.recurse,
                compression=self.compress_type,
                use_native_compression=self.use_native_compression,
                resume_token=resume_token
            )
            
            if not ssh_proc:
                logger.error("Failed to create remote send process")
                return False
            
            # Get the estimated size for progress visualization
            transfer_size = getattr(self, '_transfer_size', 0)
            
            # Set up the receive process using our adaptive receiver
            recv_proc = self.zfs.adaptive_recv(
                fd=ssh_proc.stdout,
                dataset=self.recv_dataset,
                compression=self.compress_type if not self.use_native_compression else None,
                verbose=self.verbose,
                limit=self.limit,
                time=self.time,
                size=transfer_size,
                force=True
            )
            
            # ============= STEP 5: Execute transfer with monitoring =============
            
            # Close the SSH process stdout to allow it to receive SIGPIPE if needed
            ssh_proc.stdout.close()
            
            # Wait for completion
            logger.info("Waiting for receive to complete...")
            recv_proc.communicate()
            
            # ============= STEP 6: Handle post-transfer operations =============
            
            if recv_proc.returncode == 0:
                if self.verbose:
                    logger.info(f"Successfully received snapshot to {self.recv_dataset}")
                    
                # Update bootfs if requested (only for ZFS datasets)
                if self.update_bootfs and not is_file_destination:
                    try:
                        pool = self.recv_dataset.split('/')[0]
                        snaps = self.zfs.get_snapshots(self.recv_dataset)
                        if snaps:
                            last_snap = snaps[-1]
                            bootfs = f"{self.recv_dataset}@{last_snap}"
                            logger.info(f'Updating bootfs to {bootfs}')
                            self.zpool.set(pool, 'bootfs', bootfs)
                    except Exception as e:
                        logger.error(f"Error updating bootfs: {str(e)}")
                        
                # Remove snapshot if oneshot (only for ZFS datasets)
                if self.oneshot and not is_file_destination:
                    try:
                        snaps = self.zfs.get_snapshots(self.recv_dataset)
                        if snaps:
                            last_snap = snaps[-1]
                            logger.info(f'Oneshot receive. Cleaning up snapshot {last_snap}')
                            self.zfs.destroy(f"{self.recv_dataset}@{last_snap}", recurse=self.recurse)
                    except Exception as e:
                        logger.error(f"Error removing snapshot: {str(e)}")
                        
                return True
            else:
                logger.error(f"Error receiving snapshot to {self.recv_dataset}")
                return False
                
        except Exception as e:
            logger.error(f"Error during receive_snapshot: {str(e)}")
            if self.verbose:
                import traceback
                logger.error(traceback.format_exc())
            return False

class local_send():
    def __init__(self, source, destination, snap=None, snap1=None, create_snapshot=False, oneshot=False, recurse=False, 
                 verbose=False, machine_readable=False, update_bootfs=False, update_file_snapshot=None):
        self.source = source
        self.dest = destination
        self.snap = snap
        self.snap1 = snap1
        self.create_snapshot = create_snapshot
        self.oneshot = oneshot
        self.verbose = verbose
        self.machine_readable = machine_readable
        self.recurse = recurse
        self.state = 0
        self.update_bootfs = update_bootfs
        self.update_file_snapshot = update_file_snapshot
        self.bootfs = None
        self.use_native_compression = False  # Default to not using native compression
        
        # Initialize ZFS
        self.zfs = zfs()
        
        # Perform ZFS compression negotiation
        self.check_compression_support()

        if self.update_bootfs or self.update_file_snapshot:
            zp = zpool()
            bootfs_raw = zp.get(zpool=None, property='bootfs')

            if len(bootfs_raw.keys()) == 1:
                self.bootfs = list(bootfs_raw.values())[0].split('@')[0]
            else:
                if 'syspool' in bootfs_raw.keys():
                    self.bootfs = bootfs_raw['syspool'].split('@')[0]
                elif 'archive' in bootfs_raw.keys():
                    self.bootfs = bootfs_raw['archive'].split('@')[0]

        if self.update_bootfs and self.bootfs:
            if not self.dest:
                self.dest = self.bootfs
                logger.info('Found bootfs for update %s' % self.dest)

        if self.update_file_snapshot and self.bootfs:
            if not self.source:
                self.source = self.bootfs
                logger.info('Found bootfs to create file update %s ' % self.source)

        try:
            if self.zfs.is_zfs(self.source) or self.is_file(self.source):
                state = 1
        except:
           pass
           
    def check_compression_support(self):
        """
        Check if native ZFS compression can be used based on ZFS versions and dataset compression.
        
        This implements the negotiation logic:
        1. If source or destination is a file, use existing behavior
        2. If both source and destination are ZFS datasets:
           a. Check ZFS versions on both sides (local system)
           b. Check if source dataset has compression enabled
           c. Use native compression if versions >= 2.0 and source has compression
        """
        src_type, src_compression = self.get_dataset_type(self.source)
        dst_type, dst_compression = self.get_dataset_type(self.dest)
        
        # If either source or destination is a file, use existing behavior (external compression)
        if src_type == 'file' or dst_type == 'file':
            if self.verbose:
                logger.info("File detected in transfer, using external compression if needed")
            self.use_native_compression = False
            return
            
        # If both are ZFS datasets, check ZFS versions and dataset compression
        if src_type == 'zfs' and dst_type == 'zfs':
            # Get ZFS version on local system (both source and destination are local)
            zfs_version = self.zfs.get_zfs_version()
            if not zfs_version:
                if self.verbose:
                    logger.info("Unable to detect ZFS version, using external compression")
                return
                
            # Check if version >= 2.0
            if zfs_version >= version.parse("2.0"):
                # Check if source dataset has compression enabled
                if self.zfs.is_zfs(self.source):
                    compression_setting = self.zfs.get_dataset_compression(self.source)
                    if compression_setting and compression_setting != 'off':
                        if self.verbose:
                            logger.info(f"ZFS version {zfs_version} detected with {compression_setting} compression. "
                                  f"Using native ZFS compression with -c flag")
                        self.use_native_compression = True
                    else:
                        if self.verbose:
                            logger.info(f"Source dataset has no compression. Using external compression if needed")
            else:
                if self.verbose:
                    logger.info(f"ZFS version {zfs_version} detected. Version 2.0+ required for native compression")

    def is_file(self, dataset):
        if dataset[0] == '/':
            return True
        else:
            return False

    def get_dataset_type(self, dataset):
        type = None
        compression = None
        if self.is_file(dataset):
            type = 'file'
            try:
                ext = dataset.split('.')[1]
                if ext == 'img':
                    compression = None
                elif ext == 'gz':
                    compression = 'gzip'
                elif ext == 'lz4':
                    compression = 'lz4'
                elif ext == 'bz2':
                    compression = 'bzip2'
                elif ext == 'xz':
                    compression = 'xz'
                elif ext == 'zstd':
                    compression == 'zstd'
                else:
                    compression = None
            except:
                pass
        else:
            type = 'zfs'

        return type, compression

    def send_snapshot(self):
        src_type, src_compression = self.get_dataset_type(self.source)
        dst_type, dst_compression = self.get_dataset_type(self.dest)

        if src_type == 'file' and dst_type == "zfs":
            p = pv_file(file=self.source, verbose=self.verbose)

            if p:
                if src_compression:
                    p1 = uncompressor(fd=p.stdout, type=src_compression)
                    p2 = self.zfs.recv_pipe(fd=p1.stdout, dataset=self.dest)
                    p2.communicate()
                else:
                    p2 = self.zfs.recv_pipe(fd=p.stdout, dataset=self.dest)
                    p2.communicate()

                if self.dest == self.bootfs:
                    snaps = self.zfs.get_snapshots(self.dest)
                    last_snap = snaps[-1]
                    bootfs = self.dest + '@' + last_snap
                    zpool_name = self.dest.split('/')[0]
                    logger.info('Updating bootfs to %s' % bootfs)
                    zp = zpool()
                    zp.set(zpool_name, 'bootfs', bootfs)

        elif src_type == 'zfs' and dst_type == "file":
            if self.zfs.is_zfs(self.source):
                snaps = self.zfs.get_snapshots(self.source)
                if self.update_file_snapshot:
                    found_snap = None
                    version = self.update_file_snapshot.split('.')[0]
                    for snap in snaps:
                        snap_version = snap.split('.')[0]
                        if version == snap_version:
                            found_snap = snap
                            latest_snap = snaps[-1]
                            if found_snap != latest_snap:
                                self.snap = found_snap
                                self.snap1 = latest_snap
                            else:
                                found_snap = None
                                self.snap = None
                                self.snap1 = None
                    if found_snap:
                        logger.info('Creating update file from %s to %s' % (self.snap, self.snap1))

                if self.snap and self.snap in snaps and not self.snap1:
                    if self.verbose:
                        logger.info('Sending full snapshot %s to file %s ' % (self.snap, self.dest))
                    send = self.zfs.adaptive_send(dataset=self.source, snap=self.snap, recurse=self.recurse,
                                                  verbose=self.verbose, machine_readable=self.machine_readable, compression=dst_compression,
                                                  use_native_compression=self.use_native_compression)
                    out_file = open(self.dest, 'wb+')

                    while send.poll() is None:
                        data = send.stdout.read(4096)
                        if data:
                            out_file.write(data)
                            out_file.flush()

                    # Read any remaining data after process ends
                    remaining_data = send.stdout.read()
                    if remaining_data:
                        out_file.write(remaining_data)
                        out_file.flush()

                    out_file.close()
                    send.wait()  # Ensure process has fully completed

                elif self.snap and self.snap1 and (self.snap in snaps and self.snap1 in snaps):
                    if self.verbose:
                        logger.info('Performing incremental send from %s to %s' % (self.snap, self.snap1))
                    send = self.zfs.adaptive_send(dataset=self.source, snap=self.snap, snap1=self.snap1,
                                                  recurse=self.recurse,
                                                  verbose=self.verbose, compression=dst_compression,
                                                  use_native_compression=self.use_native_compression)
                    out_file = open(self.dest, 'wb+')

                    while send.poll() is None:
                        data = send.stdout.read(4096)
                        if data:
                            out_file.write(data)
                            out_file.flush()

                    # Read any remaining data after process ends
                    remaining_data = send.stdout.read()
                    if remaining_data:
                        out_file.write(remaining_data)
                        out_file.flush()

                    out_file.close()
                    send.wait()  # Ensure process has fully completed

                else:
                    if not self.update_file_snapshot:
                        if not self.create_snapshot:
                            if snaps:
                                snap = snaps[-1]
                                if self.verbose:
                                    logger.info('Sending full snapshot %s to file %s ' % (snap, self.dest))
                                send = self.zfs.adaptive_send(dataset=self.source, snap=snap, recurse=self.recurse,
                                                              verbose=self.verbose,
                                                              machine_readable=self.machine_readable,
                                                              compression=dst_compression,
                                                              use_native_compression=self.use_native_compression)
                                out_file = open(self.dest, 'wb+')

                                while send.poll() is None:
                                    data = send.stdout.read(4096)
                                    if data:
                                        out_file.write(data)
                                        out_file.flush()

                                # Read any remaining data after process ends
                                remaining_data = send.stdout.read()
                                if remaining_data:
                                    out_file.write(remaining_data)
                                    out_file.flush()

                                out_file.close()
                                send.wait()  # Ensure process has fully completed

                        else:
                            now = datetime.now()
                            date = now.strftime('%y%m%d')
                            hour = now.strftime('%H')
                            minsec = now.strftime('%M%S')
                            snap_name = 'migrate-%s-%s-%s' % (date, hour, minsec)
                            if self.verbose:
                                logger.info('Creating snapshot for send %s' % snap_name)
                            self.zfs.snapshot(self.source, snap_name, self.recurse)
                            if self.verbose:
                                logger.info('Sending full snapshot %s' % snap_name)

                            send = self.zfs.adaptive_send(dataset=self.source, snap=snap_name, recurse=self.recurse,
                                                          verbose=self.verbose,
                                                          compression=dst_compression,
                                                          use_native_compression=self.use_native_compression)

                            out_file = open(self.dest, 'wb+')

                            while send.poll() is None:
                                data = send.stdout.read(4096)
                                if data:
                                    out_file.write(data)
                                    out_file.flush()

                            # Read any remaining data after process ends
                            remaining_data = send.stdout.read()
                            if remaining_data:
                                out_file.write(remaining_data)
                                out_file.flush()

                            out_file.close()
                            send.wait()  # Ensure process has fully completed

                            if self.oneshot:
                                logger.info('Oneshot send. Cleaning up snapshot %s' % snap_name)
                                self.zfs.destroy(self.source + '@' + snap_name, recurse=self.recurse)

        elif src_type == 'zfs' and dst_type == "zfs":
            if self.zfs.is_zfs(self.dest):
                recv_snaps = self.zfs.get_snapshots(self.dest)
                if recv_snaps:
                    source_snaps = self.zfs.get_snapshots(self.source)
                    snap, snap1 = self.zfs.engociate_inc_send(source_snaps, recv_snaps)
                    if snap and snap1:
                        if snap != snap1:
                            if self.verbose:
                                logger.info('Performing incremental send from %s to %s' % (snap, snap1))
                            send = self.zfs.adaptive_send(dataset=self.source, snap=snap, snap1=snap1,
                                                          recurse=self.recurse,
                                                          verbose=self.verbose, compression=None,
                                                          use_native_compression=self.use_native_compression,
                                                          machine_readable=self.machine_readable)

                            recv = self.zfs.recv_pipe(send.stdout, dataset=self.dest)

                            recv.communicate()
                            if recv.returncode != 0:
                                logger.error(f"ZFS receive failed with return code {recv.returncode}")
                                return 1

                        else:
                            if self.verbose:
                                if snap:
                                    logger.info('Target dataset is up to date on snapshot: %s' % snap)
                                else:
                                    logger.info('Target dataset is up to date with latest snapshot')
                    else:
                        # No common snapshots found for incremental send
                        if self.verbose:
                            logger.warning('No common snapshots found between source and destination for incremental send')

                            if self.create_snapshot:
                                now = datetime.now()
                                date = now.strftime('%y%m%d')
                                hour = now.strftime('%H')
                                minsec = now.strftime('%M%S')
                                snap1 = 'migrate-%s-%s-%s' % (date, hour, minsec)
                                logger.info('Creating snapshot for send %s' % snap1)
                                self.zfs.snapshot(self.source, snap1, self.recurse)
                                
                                # Check for valid source snapshot before attempting incremental send
                                if snap is None:
                                    logger.error('Cannot perform incremental send: source snapshot is None')
                                    # Fall back to a full send of the new snapshot
                                    logger.info(f'Falling back to full send of snapshot {snap1}')
                                    send = self.zfs.adaptive_send(dataset=self.source, snap=snap1,
                                                                  recurse=self.recurse,
                                                                  verbose=self.verbose, machine_readable=self.machine_readable, compression=None,
                                                                  use_native_compression=self.use_native_compression)
                                else:
                                    logger.info('Performing incremental send from %s to %s' % (snap, snap1))
                                    send = self.zfs.adaptive_send(dataset=self.source, snap=snap, snap1=snap1,
                                                                  recurse=self.recurse,
                                                                  verbose=self.verbose, machine_readable=self.machine_readable, compression=None,
                                                                  use_native_compression=self.use_native_compression)
                                recv = self.zfs.recv_pipe(send.stdout, dataset=self.dest)

                                recv.communicate()
                                if recv.returncode != 0:
                                    logger.error(f"ZFS receive failed with return code {recv.returncode}")
                                    return 1


                else:
                    if self.verbose:
                        logger.warning('Unable to setup incremental stream for zfs send. Recv dataset has no snapshots')
                    pass


            else:
                if self.snap and self.snap in self.zfs.get_snapshots(self.source):
                    if self.verbose:
                        logger.info('Sending full snapshot %s' % self.snap)
                    send = self.zfs.adaptive_send(dataset=self.source, snap=self.snap, recurse=self.recurse,
                                                  verbose=self.verbose, compression=None,
                                                  use_native_compression=self.use_native_compression,
                                                  machine_readable=self.machine_readable)

                    recv = self.zfs.recv_pipe(send.stdout, dataset=self.dest)
                    recv.communicate()
                    if recv.returncode != 0:
                        logger.error(f"ZFS receive failed with return code {recv.returncode}")
                        return 1

                else:
                    if not self.create_snapshot:
                        snap_list = self.zfs.get_snapshots(self.source)
                        if snap_list:
                            snap = snap_list[-1]
                            if self.verbose:
                                logger.info('Sending full snapshot %s' % snap)
                            send = self.zfs.adaptive_send(dataset=self.source, snap=snap, recurse=self.recurse,
                                                          verbose=self.verbose,
                                                          compression=None,
                                                          use_native_compression=self.use_native_compression,
                                                          machine_readable=self.machine_readable)

                            recv = self.zfs.recv_pipe(send.stdout, dataset=self.dest)
                            recv.communicate()
                            if recv.returncode != 0:
                                logger.error(f"ZFS receive failed with return code {recv.returncode}")
                                return 1

                    else:
                        now = datetime.now()
                        date = now.strftime('%y%m%d')
                        hour = now.strftime('%H')
                        minsec = now.strftime('%M%S')
                        snap_name = 'migrate-%s-%s-%s' % (date, hour, minsec)
                        if self.verbose:
                            logger.info('Creating snapshot for send %s' % snap_name)
                        self.zfs.snapshot(self.source, snap_name, self.recurse)
                        if self.verbose:
                            logger.info('Sending full snapshot %s' % snap_name)

                        send = self.zfs.adaptive_send(dataset=self.source, snap=snap_name, recurse=self.recurse,
                                                      verbose=self.verbose,
                                                      compression=None,
                                                      use_native_compression=self.use_native_compression,
                                                      machine_readable=self.machine_readable)

                        recv = self.zfs.recv_pipe(send.stdout, dataset=self.dest)
                        recv.communicate()
                        if recv.returncode != 0:
                            logger.error(f"ZFS receive failed with return code {recv.returncode}")
                            return 1

                        if self.oneshot:
                            logger.info('Oneshot send. Cleaning up snapshot %s' % snap_name)
                            self.zfs.destroy(self.source + '@' + snap_name, recurse=self.recurse)
                            self.zfs.destroy(self.dest + '@' + snap_name, recurse=self.recurse)
        
        return 0

def fping(hosts):
    """
    Simple fping replacement for systems without the fping tool.
    
    Args:
        hosts: List of hosts to ping
        
    Returns:
        Tuple of (alive, dead) host lists
    """
    # For now, always assume the host is alive to avoid connection issues
    return hosts, []

def main():
    """
    Main entry point for the ZFS migration tool.
    
    Parses command line arguments and initiates migration operations.
    """
    # Check for required packages and log instructions
    try:
        import packaging.version
    except ImportError:
        logger.warning("'packaging' module not found. For version comparison, install: pip install packaging")
        
    try:
        import paramiko
        import scp
    except ImportError as e:
        logger.warning(f"SSH Python libraries not found ({e}).")
        logger.warning("For better SSH support with password authentication, install: pip install paramiko scp")
        logger.warning("Fallback to subprocess ssh will be used, requiring SSH client and key-based authentication")
    parser = argparse.ArgumentParser(description='Usage: standalone_migrate.py -s pool/dataset -d newpool/dataset or -s ip:pool/dataset -d newpool/dataset')
    parser.add_argument('-s', '--source', default=None, 
                        help='Source dataset. Can be in format "pool/dataset" for local or "ip:pool/dataset" for remote')
    parser.add_argument('-r', '--remote', default=None, help='Remote host (can be comma-separated for multiple targets)')
    parser.add_argument('-l', '--limit', default=0, help='Transfer speed limit')
    parser.add_argument('-t', '--time', default=0, help='Transfer time limit')
    parser.add_argument('-d', '--dest', default=0, 
                        help='Destination dataset. Can be in format "pool/dataset" for local or "ip:pool/dataset" for remote')
    parser.add_argument('-c', '--compression', default=None, help='Compression algorithm: bzip2, gzip, xz, lz4, zstd')
    parser.add_argument('--snap', action='store_true', help='Create new snapshot if needed')
    parser.add_argument('-R', '--recursive', action='store_true', help='Send dataset recursively')
    parser.add_argument('-o', '--oneshot', action='store_true', help='Remove temporary snapshots')
    parser.add_argument('--snap_after', action='store_true', help='Create init snapshot for recv dataset')
    parser.add_argument('--update', action='store_true', help='Guess and update bootfs on remote host')
    parser.add_argument('--sync', action='store_true', help='Keep latest local snapshot for replication with given host')
    parser.add_argument('--update_from_snap', help='Snapshot from which to create update file')
    # By default, the tool shows a progress bar with transfer statistics
    parser.add_argument('-q', '--quiet', action='store_true', default=False, 
                        help='Quiet output (disable progress visualization)')
    parser.add_argument('--machine-readable', action='store_true', default=False,
                        help='Force pv output for machine parsing (adds -f -i 1 flags)')
    parser.add_argument('-p', '--password', help='SSH password for remote host')
    parser.add_argument('--ask-pass', action='store_true', default=False, 
                        help='Prompt for SSH password interactively (more secure than -p)')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='Set the logging level (default: INFO)')

    args = parser.parse_args()
    
    # Debug: Log the sync argument value
    print(f"DEBUG: args.sync = {args.sync}", flush=True)
    logger.info(f"Parsed arguments - sync: {args.sync}")
    
    # Configure logging based on command line arguments
    log_level = getattr(logging, args.log_level.upper())
    logging.getLogger().setLevel(log_level)
    logger.setLevel(log_level)
    logger.debug(f"Log level set to {args.log_level}")

    # Process time and limit settings
    # Convert limit from MB/s to bytes/s (user specifies in MB/s)
    if not args.time:
        limit = int(args.limit) * 1024 * 1024 if args.limit else 0
        time = 0
    else:
        limit = 0
        time = int(args.time) if args.time else 0
    
    # Parse source - check if it contains a remote host specification
    source_remote = None
    source_dataset = None
    snap = None
    
    if args.source:
        # Check if source is in format ip:dataset or ip:dataset@snapshot
        if ':' in args.source and not args.source.startswith('/'):
            # Format is ip:dataset or ip:dataset@snapshot - split into remote host and dataset
            source_parts = args.source.split(':', 1)
            if len(source_parts) == 2 and source_parts[0] and source_parts[1]:
                source_remote = source_parts[0]
                if '@' in source_parts[1]:
                    source_dataset = source_parts[1].split('@')[0]
                    snap = source_parts[1].split('@')[1]
                else:
                    source_dataset = source_parts[1]
                
                if not args.quiet:
                    logger.info(f"Parsed remote source: host={source_remote}, dataset={source_dataset}, snapshot={snap}")
        else:
            # Local dataset format: dataset or dataset@snapshot
            if '@' in args.source:
                source_dataset = args.source.split('@')[0]
                snap = args.source.split('@')[1]
            else:
                source_dataset = args.source
    
    # Parse destination - check if it contains a remote host specification
    dest_remote = None
    dest_dataset = args.dest
    
    if isinstance(args.dest, str) and ':' in args.dest and not args.dest.startswith('/'):
        # Format is ip:dataset - split into remote host and dataset
        dest_parts = args.dest.split(':', 1)
        if len(dest_parts) == 2 and dest_parts[0] and dest_parts[1]:
            dest_remote = dest_parts[0]
            dest_dataset = dest_parts[1]
            if not args.quiet:
                logger.info(f"Parsed remote destination: host={dest_remote}, dataset={dest_dataset}")

    # Get compression setting
    compression = args.compression if args.compression else 'auto'
    
    # Handle different migration scenarios
    
    # 1. Remote source to local destination: ip:pool/dataset -> pool/dataset
    if source_remote and not dest_remote:
        if not args.quiet:
            logger.info(f"Remote to local migration: {source_remote}:{source_dataset} -> {dest_dataset}")
            
        # Get password if provided or prompt if requested
        passwd = None
        if args.password:
            passwd = args.password
        elif args.ask_pass:
            try:
                import getpass
                passwd = getpass.getpass(f"SSH Password for {source_remote}: ")
            except ImportError:
                logger.warning("Cannot import getpass module for password prompt")
                logger.warning("Please use the -p/--password option instead")
        
        # Check for any SSH key, not just RSA
        ssh_dir = os.path.expanduser("~/.ssh")
        ssh_key_exists = any(
            os.path.exists(os.path.join(ssh_dir, key))
            for key in ["id_ed25519", "id_rsa", "id_ecdsa", "id_dsa"]
        )
        
        if not passwd and not ssh_key_exists and not args.ask_pass:
            logger.error("No SSH key found and no password provided.")
            logger.error("Please provide a password with the -p option or use --ask-pass to be prompted.")
            return 1
            
        receiver = SSHReceive(
            host=source_remote,
            dataset=source_dataset,
            recv_dataset=dest_dataset,
            snapshot=snap,
            compress_type=compression,
            limit=limit,
            time=time,
            recurse=args.recursive,
            verbose=not args.quiet,
            machine_readable=args.machine_readable,
            oneshot=args.oneshot,
            create_snapshot=args.snap,
            update_bootfs=args.update,
            passwd=passwd
        )
        
        if receiver.state == 1:
            receiver.receive_snapshot()
            
    # 2. Local source to remote destination: pool/dataset -> ip:pool/dataset
    #    OR specified remote (-r option) to another remote
    elif (not source_remote and dest_remote) or (args.remote and not source_remote):
        if not args.quiet:
            if dest_remote:
                logger.info(f"Local to remote migration: {source_dataset} -> {dest_remote}:{dest_dataset}")
            else:
                logger.info(f"Local to remote migration: {source_dataset} -> {args.remote}:{dest_dataset}")
                
        # Get password if provided or prompt if requested
        passwd = None
        if args.password:
            passwd = args.password
        elif args.ask_pass:
            try:
                import getpass
                remote_for_prompt = dest_remote if dest_remote else args.remote.split(',')[0] if args.remote else "unknown"
                passwd = getpass.getpass(f"SSH Password for {remote_for_prompt}: ")
            except ImportError:
                logger.warning("Cannot import getpass module for password prompt")
                logger.warning("Please use the -p/--password option instead")
        
        # Check for any SSH key, not just RSA
        ssh_dir = os.path.expanduser("~/.ssh")
        ssh_key_exists = any(
            os.path.exists(os.path.join(ssh_dir, key))
            for key in ["id_ed25519", "id_rsa", "id_ecdsa", "id_dsa"]
        )
        
        if not passwd and not ssh_key_exists and not args.ask_pass:
            logger.error("No SSH key found and no password provided.")
            logger.error("Please provide a password with the -p option or use --ask-pass to be prompted.")
            return 1
            
        # Use -r if specified, otherwise use destination remote  
        remotes = args.remote.split(',') if args.remote else [dest_remote]
        
        for remote in remotes:
            sender = ssh_send(
                host=remote,
                dataset=source_dataset,
                snapshot=snap,
                limit=limit,
                time=time,
                recv_dataset=dest_dataset,
                verbose=not args.quiet,
                machine_readable=args.machine_readable,
                compress_type=compression,
                oneshot=args.oneshot,
                create_snapshot=args.snap,
                update_bootfs=args.update,
                recurse=args.recursive,
                sync=args.sync,
                passwd=passwd
            )

            if sender.state == 1:
                result = sender.send_snapshot()
        if result != 0:
            return result
    
    # 3. Local source to local destination: pool/dataset -> pool/dataset
    elif not source_remote and not dest_remote and not args.remote:
        if not args.quiet:
            logger.info(f"Local to local migration: {source_dataset} -> {dest_dataset}")
            
        sender = local_send(
            source=source_dataset,
            destination=dest_dataset,
            snap=snap,
            create_snapshot=args.snap,
            oneshot=args.oneshot,
            recurse=args.recursive,
            verbose=not args.quiet,
            machine_readable=args.machine_readable,
            update_bootfs=args.update,
            update_file_snapshot=args.update_from_snap
        )
        
        result = sender.send_snapshot()
        if result != 0:
            return result
    
    # 4. Remote source to remote destination: ip1:pool/dataset -> ip2:pool/dataset
    # This is not directly supported - would need to be implemented via a temporary local dataset
    else:
        logger.error("Error: Direct remote-to-remote migration is not supported.")
        logger.error("You need to migrate first to a local dataset, then to the remote destination.")
        return 1
        
    return 0


if __name__ == '__main__':
    sys.exit(main())