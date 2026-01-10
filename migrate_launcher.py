#!/usr/bin/env python3
"""
Launcher for standalone_migrate.py with pluggable progress handlers
"""

import subprocess
import asyncio
import json
import re
import sys
import time
from typing import Optional, Dict, Any, List, Callable
from datetime import datetime, timezone
from abc import ABC, abstractmethod

class ProgressHandler(ABC):
    """Base class for progress handlers"""
    
    @abstractmethod
    async def on_start(self, task_id: str, command: List[str]) -> None:
        """Called when migration starts"""
        pass
    
    @abstractmethod
    async def on_progress(self, progress_data: Dict[str, Any]) -> None:
        """Called on progress updates"""
        pass
    
    @abstractmethod
    async def on_log(self, message: str) -> None:
        """Called for log messages"""
        pass
    
    @abstractmethod
    async def on_complete(self, task_id: str, return_code: int, elapsed: float) -> None:
        """Called when migration completes"""
        pass
    
    @abstractmethod
    async def on_error(self, task_id: str, error: str) -> None:
        """Called on errors"""
        pass

class StdoutHandler(ProgressHandler):
    """Output JSON to stdout"""
    
    async def on_start(self, task_id: str, command: List[str]) -> None:
        print(json.dumps({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'task_id': task_id,
            'status': 'started',
            'command': ' '.join(command)
        }), flush=True)
    
    async def on_progress(self, progress_data: Dict[str, Any]) -> None:
        print(json.dumps(progress_data), flush=True)
    
    async def on_log(self, message: str) -> None:
        print(json.dumps({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'status': 'log',
            'message': message
        }), flush=True)
    
    async def on_complete(self, task_id: str, return_code: int, elapsed: float) -> None:
        print(json.dumps({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'task_id': task_id,
            'status': 'completed' if return_code == 0 else 'failed',
            'return_code': return_code,
            'elapsed_seconds': int(elapsed)
        }), flush=True)
    
    async def on_error(self, task_id: str, error: str) -> None:
        print(json.dumps({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'task_id': task_id,
            'status': 'error',
            'error': error
        }), flush=True)

class RedisHandler(ProgressHandler):
    """Store progress in Redis"""
    
    def __init__(self, redis_client, ttl: int = 3600):
        self.redis = redis_client
        self.ttl = ttl
    
    async def on_start(self, task_id: str, command: List[str]) -> None:
        await self.redis.hset(f"migration:task:{task_id}", mapping={
            'status': 'running',
            'started_at': datetime.now(timezone.utc).isoformat(),
            'command': ' '.join(command)
        })
        await self.redis.expire(f"migration:task:{task_id}", self.ttl)
    
    async def on_progress(self, progress_data: Dict[str, Any]) -> None:
        task_id = progress_data['task_id']
        await self.redis.hset(f"migration:progress:{task_id}", mapping={
            'bytes_transferred': progress_data.get('bytes_transferred', 0),
            'bytes_total': progress_data.get('bytes_total', 0),
            'percentage': progress_data.get('percentage', 0),
            'rate_mbps': progress_data.get('rate_mbps', 0),
            'eta_seconds': progress_data.get('eta_seconds', 0),
            'updated_at': datetime.now(timezone.utc).isoformat()
        })
        await self.redis.expire(f"migration:progress:{task_id}", self.ttl)
    
    async def on_log(self, message: str) -> None:
        # Optionally store logs in Redis
        pass
    
    async def on_complete(self, task_id: str, return_code: int, elapsed: float) -> None:
        status = 'completed' if return_code == 0 else 'failed'
        await self.redis.hset(f"migration:task:{task_id}", mapping={
            'status': status,
            'completed_at': datetime.now(timezone.utc).isoformat(),
            'return_code': return_code,
            'elapsed_seconds': int(elapsed)
        })
    
    async def on_error(self, task_id: str, error: str) -> None:
        await self.redis.hset(f"migration:task:{task_id}", mapping={
            'status': 'error',
            'error': error,
            'error_at': datetime.now(timezone.utc).isoformat()
        })

class CallbackHandler(ProgressHandler):
    """Custom callback handler"""
    
    def __init__(self, 
                 on_start_cb: Optional[Callable] = None,
                 on_progress_cb: Optional[Callable] = None,
                 on_complete_cb: Optional[Callable] = None,
                 on_error_cb: Optional[Callable] = None):
        self.on_start_cb = on_start_cb
        self.on_progress_cb = on_progress_cb
        self.on_complete_cb = on_complete_cb
        self.on_error_cb = on_error_cb
    
    async def on_start(self, task_id: str, command: List[str]) -> None:
        if self.on_start_cb:
            await self.on_start_cb(task_id, command)
    
    async def on_progress(self, progress_data: Dict[str, Any]) -> None:
        if self.on_progress_cb:
            await self.on_progress_cb(progress_data)
    
    async def on_log(self, message: str) -> None:
        pass  # Implement if needed
    
    async def on_complete(self, task_id: str, return_code: int, elapsed: float) -> None:
        if self.on_complete_cb:
            await self.on_complete_cb(task_id, return_code, elapsed)
    
    async def on_error(self, task_id: str, error: str) -> None:
        if self.on_error_cb:
            await self.on_error_cb(task_id, error)

class MigrateLauncher:
    """Launch and monitor standalone_migrate.py"""
    
    def __init__(self, handlers: List[ProgressHandler], task_id: Optional[str] = None):
        self.handlers = handlers
        self.task_id = task_id or f"migrate_{int(time.time())}"
        self.start_time = time.time()
        self.total_size = None
        self.process = None  # Store the subprocess reference
    
    def parse_pv_line(self, line: str) -> Optional[Dict[str, Any]]:
        """Parse a line of pv output"""
        
        # Pattern for bytes at start of line
        bytes_match = re.match(r'^(\d+(?:[,\.]\d+)?)\s*([KMGT]i?B)', line)
        if not bytes_match:
            return None
            
        # Parse bytes transferred
        value = float(bytes_match.group(1).replace(',', '.'))
        unit = bytes_match.group(2).upper().replace('IB', 'B')
        
        multipliers = {
            'B': 1, 'KB': 1024, 'MB': 1024**2,
            'GB': 1024**3, 'TB': 1024**4
        }
        
        bytes_transferred = int(value * multipliers.get(unit, 1))
        
        # Parse transfer rate
        # The pattern should match formats like: [1,06GiB/s] or [ 179MiB/s]
        rate_match = re.search(r'\[\s*(\d+(?:[,\.]\d+)?)\s*([KMGT]i?B)/s\]', line)
        if rate_match:
            rate_value = float(rate_match.group(1).replace(',', '.'))
            rate_unit = rate_match.group(2).upper().replace('IB', 'B')
            rate_bytes = rate_value * multipliers.get(rate_unit, 1)
            rate_mbps = rate_bytes / (1024**2)
        else:
            rate_mbps = 0
        
        # Parse percentage
        percent_match = re.search(r'\s(\d+)%', line)
        percentage = int(percent_match.group(1)) if percent_match else 0
        
        # Parse ETA
        eta_seconds = 0
        eta_match = re.search(r'ETA\s+(\d+:\d+:\d+)', line)
        if eta_match:
            time_parts = eta_match.group(1).split(':')
            eta_seconds = int(time_parts[0]) * 3600 + int(time_parts[1]) * 60 + int(time_parts[2])
        
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'task_id': self.task_id,
            'status': 'progress',
            'bytes_transferred': bytes_transferred,
            'bytes_total': self.total_size,
            'percentage': percentage,
            'rate_mbps': round(rate_mbps, 2),
            'eta_seconds': eta_seconds,
            'elapsed_seconds': int(time.time() - self.start_time)
        }
    
    async def notify_handlers(self, method: str, *args, **kwargs):
        """Call method on all handlers"""
        for handler in self.handlers:
            method_func = getattr(handler, method)
            await method_func(*args, **kwargs)
    
    async def run(self, cmd: List[str]) -> int:
        """Run migration command and monitor progress"""
        
        # Ensure --machine-readable is in the command
        if '--machine-readable' not in cmd:
            # Find where to insert it (after the script name)
            for i, arg in enumerate(cmd):
                if arg.endswith('standalone_migrate.py'):
                    cmd.insert(i + 1, '--machine-readable')
                    break
        
        # Notify start
        await self.notify_handlers('on_start', self.task_id, cmd)
        
        # Start the process with a new process group
        self.process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True  # Create new process group
        )
        
        # Read stderr for pv output
        buffer = b''
        last_progress = None
        
        try:
            while True:
                char = await self.process.stderr.read(1)
                if not char:
                    break
                
                buffer += char
                
                if char in (b'\r', b'\n'):
                    line = buffer.decode('utf-8', errors='ignore').strip()
                    buffer = b''
                    
                    if line:
                        # Check if this line contains size information
                        size_match = re.search(r'Starting ZFS send with size estimate: (\d+) bytes', line)
                        if size_match:
                            self.total_size = int(size_match.group(1))
                        
                        # Try to parse as progress
                        progress = self.parse_pv_line(line)
                        if progress and progress != last_progress:
                            # Update progress with total size if we have it
                            if progress and self.total_size:
                                progress['bytes_total'] = self.total_size
                                # Recalculate percentage based on actual total
                                progress['percentage'] = round((progress['bytes_transferred'] / self.total_size) * 100, 1)
                            await self.notify_handlers('on_progress', progress)
                            last_progress = progress
                        else:
                            # Log all non-progress output including what failed to parse
                            await self.notify_handlers('on_log', line)
                            
        except Exception as e:
            await self.notify_handlers('on_error', self.task_id, str(e))
        
        # Wait for completion
        return_code = await self.process.wait()
        elapsed = time.time() - self.start_time
        
        # Notify completion
        await self.notify_handlers('on_complete', self.task_id, return_code, elapsed)
        
        return return_code
    
    async def cancel(self):
        """Cancel the running migration"""
        if self.process and self.process.returncode is None:
            import os
            import signal
            
            try:
                # Kill the entire process group
                pgid = os.getpgid(self.process.pid)
                os.killpg(pgid, signal.SIGTERM)
                
                try:
                    # Wait up to 5 seconds for graceful shutdown
                    await asyncio.wait_for(self.process.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    # If it doesn't stop, use SIGKILL on the process group
                    os.killpg(pgid, signal.SIGKILL)
                    await self.process.wait()
            except ProcessLookupError:
                # Process already dead
                pass
            
            await self.notify_handlers('on_error', self.task_id, 'Migration cancelled by user')
            return True
        return False

# Convenience function for simple use cases
async def run_migration_with_progress(cmd: List[str], 
                                    handlers: Optional[List[ProgressHandler]] = None,
                                    task_id: Optional[str] = None) -> int:
    """Run migration with specified handlers (default: stdout)"""
    if handlers is None:
        handlers = [StdoutHandler()]
    
    launcher = MigrateLauncher(handlers, task_id)
    return await launcher.run(cmd)

# Example usage
async def example():
    """Example of using the launcher"""
    
    # Example 1: Output to stdout (JSON lines)
    await run_migration_with_progress([
        'python3', '/home/stein/python/migrate/standalone_migrate.py',
        '-s', 'pool/source@snap',
        '-d', 'pool/dest'
    ])
    
    # Example 2: Multiple handlers
    import aioredis
    redis = await aioredis.from_url("redis://localhost")
    
    handlers = [
        StdoutHandler(),  # Also output to stdout
        RedisHandler(redis),  # Store in Redis
        CallbackHandler(  # Custom callbacks
            on_progress_cb=lambda data: print(f"Progress: {data['percentage']}%")
        )
    ]
    
    await run_migration_with_progress(
        ['python3', 'standalone_migrate.py', '-s', 'pool/data', '-d', 'backup/data'],
        handlers=handlers,
        task_id="backup_job_123"
    )

if __name__ == "__main__":
    # For command-line use, default to stdout handler
    if len(sys.argv) < 2:
        print("Usage: migrate_launcher.py <migrate_command...>", file=sys.stderr)
        sys.exit(1)
    
    result = asyncio.run(run_migration_with_progress(sys.argv[1:]))
    sys.exit(result)