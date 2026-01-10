#!/usr/bin/env python3
"""
Migration API endpoints for aiohttp

DEPRECATION NOTICE: These REST endpoints are kept for backward compatibility.
New implementations should use the JSON-RPC methods instead:
- migration_create
- migration_list  
- migration_get
- migration_cancel
- migration_progress

The JSON-RPC methods provide the same functionality with a consistent API format.
"""

from aiohttp import web
import json
import logging
from typing import Optional, Dict, Any
from datetime import datetime
from async_background_task_manager import AsyncBackgroundTaskManager, TaskStatus

logger = logging.getLogger(__name__)

# Global task manager instance (set by main app)
task_manager: Optional[AsyncBackgroundTaskManager] = None

async def create_migration(request: web.Request) -> web.Response:
    """Create a new migration task"""
    try:
        data = await request.json()
        
        # Validate input
        if 'source' not in data:
            return web.json_response(
                {'error': 'Source dataset is required'}, 
                status=400
            )
        if 'destination' not in data:
            return web.json_response(
                {'error': 'Destination dataset is required'}, 
                status=400
            )
            
        # Create task parameters
        params = {
            'source': data['source'],
            'destination': data['destination'],
        }
        
        if 'remote' in data:
            params['remote'] = data['remote']
        if 'limit' in data:
            params['limit'] = data['limit']
        if 'compression' in data:
            params['compression'] = data['compression']
        if 'recursive' in data:
            params['recursive'] = data['recursive']
            
        # Create background task
        task_id = await task_manager.create_task('migration', params)
        
        return web.json_response({
            'task_id': task_id,
            'status': 'created',
            'message': 'Migration task created successfully'
        })
        
    except Exception as e:
        logger.error(f"Error creating migration: {e}")
        return web.json_response(
            {'error': str(e)}, 
            status=500
        )

async def list_tasks(request: web.Request) -> web.Response:
    """List migration tasks"""
    try:
        status = request.query.get('status')
        limit = int(request.query.get('limit', 100))
        
        # Get all task keys from Redis
        keys = await task_manager.redis.keys('task:*') or []
        tasks = []
        
        for key in keys:
            task_id = key.decode().split(":", 1)[1] if isinstance(key, bytes) else key.split(":", 1)[1]
            task = await task_manager.get_task(task_id)
            if task:
                # Filter by status if specified
                if status and task.status.value != status:
                    continue
                    
                tasks.append({
                    'id': task.id,
                    'type': task.type,
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
        
        return web.json_response({
            'tasks': tasks,
            'total': len(tasks)
        })
        
    except Exception as e:
        logger.error(f"Error listing tasks: {e}")
        return web.json_response(
            {'error': str(e)}, 
            status=500
        )

async def get_task(request: web.Request) -> web.Response:
    """Get task details"""
    try:
        task_id = request.match_info['task_id']
        task = await task_manager.get_task(task_id)
        
        if not task:
            return web.json_response(
                {'error': 'Task not found'}, 
                status=404
            )
            
        return web.json_response({
            'id': task.id,
            'type': task.type,
            'status': task.status.value,
            'created_at': task.created_at.isoformat(),
            'started_at': task.started_at.isoformat() if task.started_at else None,
            'completed_at': task.completed_at.isoformat() if task.completed_at else None,
            'error': task.error,
            'progress': task.progress,
            'params': task.params
        })
        
    except Exception as e:
        logger.error(f"Error getting task: {e}")
        return web.json_response(
            {'error': str(e)}, 
            status=500
        )

async def cancel_task(request: web.Request) -> web.Response:
    """Cancel a running task"""
    try:
        task_id = request.match_info['task_id']
        success = await task_manager.cancel_task(task_id)
        
        if not success:
            return web.json_response(
                {'error': 'Task cannot be cancelled'}, 
                status=400
            )
            
        return web.json_response({
            'message': 'Task cancelled successfully'
        })
        
    except Exception as e:
        logger.error(f"Error cancelling task: {e}")
        return web.json_response(
            {'error': str(e)}, 
            status=500
        )

async def get_task_progress(request: web.Request) -> web.Response:
    """Get task progress"""
    try:
        task_id = request.match_info['task_id']
        task = await task_manager.get_task(task_id)
        
        if not task:
            return web.json_response(
                {'error': 'Task not found'}, 
                status=404
            )
            
        return web.json_response({
            'task_id': task_id,
            'status': task.status.value,
            'progress': task.progress or {},
            'error': task.error
        })
        
    except Exception as e:
        logger.error(f"Error getting task progress: {e}")
        return web.json_response(
            {'error': str(e)}, 
            status=500
        )

def setup_migration_routes(app: web.Application, prefix: str = '/api/v1/migrations'):
    """Setup migration routes on the app"""
    app.router.add_post(f'{prefix}/', create_migration)
    app.router.add_get(f'{prefix}/tasks', list_tasks)
    app.router.add_get(f'{prefix}/tasks/{{task_id}}', get_task)
    app.router.add_delete(f'{prefix}/tasks/{{task_id}}', cancel_task)
    app.router.add_get(f'{prefix}/tasks/{{task_id}}/progress', get_task_progress)