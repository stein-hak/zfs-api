#!/usr/bin/env python3
"""
Async Background Task Manager - Uses AsyncReliableRedis for non-blocking operations
"""

import asyncio
import uuid
import time
import json
import logging
from typing import Dict, Any, Optional, Callable, List
from datetime import datetime, timezone
from enum import Enum
from dataclasses import dataclass, asdict, field
from async_redis_reliable import AsyncReliableRedis
import subprocess

logger = logging.getLogger(__name__)

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

@dataclass
class Task:
    id: str
    type: str
    status: TaskStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    progress: Optional[Dict[str, Any]] = None
    params: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['status'] = self.status.value
        data['created_at'] = self.created_at.isoformat()
        if self.started_at:
            data['started_at'] = self.started_at.isoformat()
        if self.completed_at:
            data['completed_at'] = self.completed_at.isoformat()
        return data

class AsyncBackgroundTaskManager:
    def __init__(self, redis_client: AsyncReliableRedis, max_workers: int = 4):
        self.redis = redis_client
        self.max_workers = max_workers
        self.workers: List[asyncio.Task] = []
        self.task_handlers: Dict[str, Callable] = {}
        self.running_tasks: Dict[str, Any] = {}  # Store launcher instances for cancellation
        self._running = False
        
    def register_handler(self, task_type: str, handler: Callable):
        """Register a handler function for a specific task type"""
        self.task_handlers[task_type] = handler
        
    async def create_task(self, task_type: str, params: Dict[str, Any]) -> str:
        """Create a new background task"""
        task_id = str(uuid.uuid4())
        task = Task(
            id=task_id,
            type=task_type,
            status=TaskStatus.PENDING,
            created_at=datetime.now(timezone.utc),
            params=params
        )
        
        # Store task in Redis
        await self._save_task(task)
        
        # Add to queue
        queue_result = await self.redis.rpush('zfs_migration_queue', task_id)
        logger.info(f"Added task {task_id} to queue, queue length now: {queue_result}")
        
        logger.info(f"Created task {task_id} of type {task_type}")
        return task_id
        
    async def get_task(self, task_id: str) -> Optional[Task]:
        """Get task details"""
        data = await self.redis.hgetall(f"task:{task_id}")
        if not data:
            return None
            
        # Convert Redis data to Task object
        task_data = {k.decode() if isinstance(k, bytes) else k: 
                     v.decode() if isinstance(v, bytes) else v 
                     for k, v in data.items()}
        
        # Parse JSON fields
        for field in ['result', 'progress', 'params']:
            if field in task_data and task_data[field]:
                try:
                    task_data[field] = json.loads(task_data[field])
                except:
                    pass
                    
        # Parse datetime fields
        task_data['created_at'] = datetime.fromisoformat(task_data['created_at'])
        if task_data.get('started_at'):
            task_data['started_at'] = datetime.fromisoformat(task_data['started_at'])
        if task_data.get('completed_at'):
            task_data['completed_at'] = datetime.fromisoformat(task_data['completed_at'])
            
        # Parse status
        task_data['status'] = TaskStatus(task_data['status'])
        
        return Task(**task_data)
        
    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a running task"""
        task = await self.get_task(task_id)
        if not task:
            return False
            
        # If task is not running, check if it just completed with cancellation
        if task.status != TaskStatus.RUNNING:
            # If it completed very recently (within 5 seconds) and has the cancelled flag, 
            # consider it a successful cancellation
            if (task.status == TaskStatus.COMPLETED and 
                task.completed_at and 
                task.result and 
                task.result.get('cancelled') and
                (datetime.now(timezone.utc) - task.completed_at).total_seconds() < 5):
                logger.info(f"Task {task_id} was successfully cancelled")
                return True
            return False
            
        # Cancel the running task
        cancelled = False
        if task_id in self.running_tasks:
            launcher = self.running_tasks[task_id]
            try:
                # If it's a MigrateLauncher instance, use its cancel method
                if hasattr(launcher, 'cancel'):
                    await launcher.cancel()
                    cancelled = True
                # Otherwise try to terminate it as a process
                elif hasattr(launcher, 'terminate'):
                    launcher.terminate()
                    await asyncio.sleep(0.5)
                    if launcher.returncode is None:
                        launcher.kill()
                    cancelled = True
            except Exception as e:
                logger.error(f"Error cancelling task {task_id}: {e}")
            finally:
                if task_id in self.running_tasks:
                    del self.running_tasks[task_id]
        
        # Only update status if we actually cancelled something
        if cancelled:
            try:
                # Re-fetch task to avoid race conditions
                task = await self.get_task(task_id)
                if task and task.status == TaskStatus.RUNNING:
                    task.status = TaskStatus.CANCELLED
                    task.completed_at = datetime.now(timezone.utc)
                    await self._save_task(task)
            except Exception as e:
                logger.warning(f"Could not update task status after cancellation: {e}")
        
        logger.info(f"Cancelled task {task_id}")
        return True
        
    async def update_task_progress(self, task_id: str, progress: Dict[str, Any]):
        """Update task progress"""
        task = await self.get_task(task_id)
        if not task:
            return
            
        task.progress = progress
        await self._save_task(task)
        
    async def start(self):
        """Start the background task manager"""
        if self._running:
            return
            
        self._running = True
        logger.info(f"Starting async background task manager with {self.max_workers} workers")
        
        # Start worker tasks
        for i in range(self.max_workers):
            worker = asyncio.create_task(self._worker(i))
            self.workers.append(worker)
            
    async def stop(self):
        """Stop the background task manager"""
        if not self._running:
            return
            
        self._running = False
        logger.info("Stopping async background task manager")
        
        # Cancel all workers
        for worker in self.workers:
            worker.cancel()
            
        # Wait for workers to finish
        await asyncio.gather(*self.workers, return_exceptions=True)
        self.workers.clear()
        
        # Terminate any running processes
        for process in self.running_tasks.values():
            try:
                process.terminate()
                await asyncio.sleep(0.1)
                if process.returncode is None:
                    process.kill()
            except:
                pass
        self.running_tasks.clear()
        
    async def _worker(self, worker_id: int):
        """Worker coroutine that processes tasks from the queue"""
        logger.info(f"Worker {worker_id} started")
        
        try:
            while self._running:
                try:
                    # Get task from queue (blocking with timeout)
                    task_id = await asyncio.wait_for(
                        self._get_next_task(),
                        timeout=1.0
                    )
                    
                    if not task_id:
                        continue
                    
                    logger.info(f"Worker {worker_id} picked up task: {task_id}")
                        
                    # Process the task
                    await self._process_task(task_id)
                    
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Worker {worker_id} error: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Worker {worker_id} crashed: {e}", exc_info=True)
                    
        logger.info(f"Worker {worker_id} stopped")
        
    async def _get_next_task(self) -> Optional[str]:
        """Get next task from queue"""
        try:
            # Use async blpop
            result = await self.redis.blpop('zfs_migration_queue', timeout=1)
            logger.info(f"blpop returned: {result}")
            if result:
                # result is a tuple (queue_name, value)
                task_id = result[1].decode() if isinstance(result[1], bytes) else result[1]
                logger.info(f"blpop returned task: {task_id}")
                return task_id
            return None
        except Exception as e:
            logger.error(f"Error in _get_next_task: {e}", exc_info=True)
            return None
        
    async def _process_task(self, task_id: str):
        """Process a single task"""
        task = await self.get_task(task_id)
        if not task:
            logger.error(f"Task {task_id} not found")
            return
            
        if task.status != TaskStatus.PENDING:
            logger.warning(f"Task {task_id} is not pending (status: {task.status})")
            return
            
        # Update task status to running
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now(timezone.utc)
        await self._save_task(task)
        
        logger.info(f"Processing task {task_id} of type {task.type}")
        
        try:
            # Get handler for this task type
            handler = self.task_handlers.get(task.type)
            if not handler:
                raise ValueError(f"No handler registered for task type: {task.type}")
                
            # Execute the handler
            result = await handler(task_id, task.params, self)
            
            # Update task status to completed
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now(timezone.utc)
            task.result = result
            await self._save_task(task)
            
            logger.info(f"Task {task_id} completed successfully")
            
        except Exception as e:
            # Update task status to failed
            task.status = TaskStatus.FAILED
            task.completed_at = datetime.now(timezone.utc)
            task.error = str(e)
            await self._save_task(task)
            
            logger.error(f"Task {task_id} failed: {e}", exc_info=True)
            
    async def _save_task(self, task: Task):
        """Save task to Redis"""
        data = task.to_dict()
        
        # Convert complex fields to JSON
        for field in ['result', 'progress', 'params']:
            if field in data and data[field] is not None:
                data[field] = json.dumps(data[field])
        
        # Remove None values and convert to strings
        clean_data = {}
        for k, v in data.items():
            if v is not None:
                # Convert to string if not already
                clean_data[k] = str(v) if not isinstance(v, str) else v
                
        # Save to Redis
        key = f"task:{task.id}"
        
        # Use async hset
        result = await self.redis.hset(key, mapping=clean_data)
        logger.debug(f"Saved task {key}, fields: {len(clean_data)}, result: {result}")
        
        # Set expiration (7 days)
        expire_result = await self.redis.expire(key, 7 * 24 * 3600)
        logger.debug(f"Set expiration for {key}, result: {expire_result}")


# Migration-specific handler
async def handle_migration_task(task_id: str, params: Dict[str, Any], 
                               manager: AsyncBackgroundTaskManager) -> Dict[str, Any]:
    """Handle ZFS migration tasks"""
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from migrate_launcher import MigrateLauncher, StdoutHandler, CallbackHandler
    
    # Build command from params
    cmd = ['python3', 'standalone_migrate.py']
    
    logger.info(f"DEBUG: Building migration command for task {task_id}")
    logger.info(f"DEBUG: Sync parameter value: {params.get('sync', 'NOT_SET')}")
    
    if params.get('source'):
        cmd.extend(['-s', params['source']])
    if params.get('remote'):
        cmd.extend(['-r', params['remote']])
    if params.get('destination'):
        cmd.extend(['-d', params['destination']])
    if params.get('limit'):
        # Parse limit value (e.g., "1M" -> "1", "100" -> "100")
        limit_str = str(params['limit'])
        if limit_str.upper().endswith('M'):
            limit_str = limit_str[:-1]
        elif limit_str.upper().endswith('MB'):
            limit_str = limit_str[:-2]
        cmd.extend(['-l', limit_str])
    if params.get('compression'):
        cmd.extend(['-c', params['compression']])
    if params.get('recursive'):
        cmd.append('--recursive')
    if params.get('sync', True):  # Default to True for sync
        cmd.append('--sync')
        logger.info(f"DEBUG: Added --sync to command")
    else:
        logger.info(f"DEBUG: NOT adding --sync to command")
    
    logger.info(f"DEBUG: Final command: {' '.join(cmd)}")
    
    # Create custom handler to update task progress
    class TaskProgressHandler(StdoutHandler):
        async def on_progress(self, progress_data: Dict[str, Any]) -> None:
            await manager.update_task_progress(task_id, progress_data)
            # Also log to stdout
            await super().on_progress(progress_data)
    
    # Run migration with progress tracking
    launcher = MigrateLauncher([TaskProgressHandler()], task_id=task_id)
    
    # Store the launcher in running_tasks for cancellation
    manager.running_tasks[task_id] = launcher
    
    try:
        start_time = time.time()
        return_code = await launcher.run(cmd)
        elapsed = time.time() - start_time
        
        if return_code == -15:  # SIGTERM
            # Task was cancelled, this is expected
            return {
                'return_code': return_code,
                'elapsed_seconds': elapsed,
                'cancelled': True
            }
        elif return_code != 0:
            raise Exception(f"Migration failed with return code {return_code}")
    finally:
        # Remove from running tasks when done
        if task_id in manager.running_tasks:
            del manager.running_tasks[task_id]
    
    return {
        'return_code': return_code,
        'elapsed_seconds': elapsed
    }


# Example usage
async def example():
    """Example of using the async background task manager"""
    
    # Initialize async Redis
    redis_client = AsyncReliableRedis(
        host='localhost',
        port=6379,
        decode_responses=False
    )
    await redis_client.connect()
    
    # Create task manager
    task_manager = AsyncBackgroundTaskManager(redis_client)
    
    # Register migration handler
    task_manager.register_handler('migration', handle_migration_task)
    
    # Start the manager
    await task_manager.start()
    
    # Create a migration task
    task_id = await task_manager.create_task('migration', {
        'source': 'pool/source@snap',
        'destination': 'pool/dest',
        'limit': 10,
        'recursive': True
    })
    
    print(f"Created task: {task_id}")
    
    # Monitor progress
    while True:
        task = await task_manager.get_task(task_id)
        if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
            break
            
        if task.progress:
            print(f"Progress: {task.progress}")
            
        await asyncio.sleep(1)
    
    # Stop the manager
    await task_manager.stop()
    
    # Close Redis connection
    await redis_client.close()


if __name__ == "__main__":
    asyncio.run(example())