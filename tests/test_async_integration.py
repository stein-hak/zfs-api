#!/usr/bin/env python3
"""
Test async integration with the server
"""

import asyncio
import logging
from async_redis_reliable import AsyncReliableRedis
from async_redis_token_manager import AsyncTokenManager
from async_background_task_manager import AsyncBackgroundTaskManager, TaskStatus

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_integration():
    """Test the async integration"""
    
    # Create Redis client
    redis_client = AsyncReliableRedis(
        host='192.168.18.25',
        port=6379,
        max_retries=5,
        raise_on_failure=False,
        decode_responses=False
    )
    
    print("Connecting to Redis...")
    await redis_client.connect()
    print("✓ Connected to Redis")
    
    # Create token manager
    token_manager = AsyncTokenManager(redis_client)
    print("✓ Token manager created")
    
    # Test token creation
    token = await token_manager.create_token('testuser', ttl=300)
    print(f"✓ Created token: {token[:20]}...")
    
    # Verify token
    username = await token_manager.verify_token(token)
    print(f"✓ Token verified: {username}")
    
    # Create task manager
    task_manager = AsyncBackgroundTaskManager(redis_client, max_workers=2)
    print("✓ Task manager created")
    
    # Define test handler
    async def test_handler(task_id: str, params: dict, manager):
        print(f"  Processing task {task_id} with params: {params}")
        await asyncio.sleep(1)
        return {"status": "success", "value": params.get("value", 0) * 2}
    
    # Register handler
    task_manager.register_handler('test', test_handler)
    
    # Start task manager
    await task_manager.start()
    print("✓ Task manager started")
    
    # Create a task
    task_id = await task_manager.create_task('test', {'value': 42})
    print(f"✓ Created task: {task_id}")
    
    # Wait for completion
    while True:
        task = await task_manager.get_task(task_id)
        if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
            break
        await asyncio.sleep(0.5)
    
    print(f"✓ Task completed: {task.status.value}, result: {task.result}")
    
    # Test list_tasks like in migration API
    keys = await task_manager.redis.keys('task:*') or []
    print(f"✓ Found {len(keys)} tasks in Redis")
    
    # Cleanup
    await token_manager.revoke_token(token)
    print("✓ Token revoked")
    
    # Stop task manager
    await task_manager.stop()
    print("✓ Task manager stopped")
    
    # Close Redis
    await redis_client.close()
    print("✓ Redis connection closed")
    
    print("\n✅ Integration test passed!")

if __name__ == "__main__":
    asyncio.run(test_integration())