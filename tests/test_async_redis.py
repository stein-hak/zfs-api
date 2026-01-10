#!/usr/bin/env python3
"""
Test script for AsyncReliableRedis implementation
"""

import asyncio
import logging
import sys
import time
from async_redis_reliable import AsyncReliableRedis, AsyncConnectionFailureError
from async_redis_token_manager import AsyncTokenManager
from async_background_task_manager import AsyncBackgroundTaskManager, TaskStatus

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_basic_operations():
    """Test basic Redis operations"""
    print("\n=== Testing Basic Operations ===")
    
    redis = AsyncReliableRedis(
        host='192.168.18.25',
        port=6379,
        max_retries=3
    )
    
    await redis.connect()
    
    # Test set/get
    await redis.set('test:key1', 'value1', ex=60)
    value = await redis.get('test:key1')
    print(f"✓ Set/Get test: {value}")
    
    # Test hash operations
    await redis.hset('test:hash1', mapping={
        'field1': 'value1',
        'field2': 'value2',
        'field3': 'value3'
    })
    hash_data = await redis.hgetall('test:hash1')
    print(f"✓ Hash operations: {len(hash_data)} fields")
    
    # Test list operations
    await redis.rpush('test:list1', 'item1', 'item2', 'item3')
    list_len = await redis.llen('test:list1')
    print(f"✓ List operations: {list_len} items")
    
    # Test set operations
    await redis.sadd('test:set1', 'member1', 'member2', 'member3')
    members = await redis.smembers('test:set1')
    print(f"✓ Set operations: {len(members)} members")
    
    # Test expiration
    await redis.expire('test:key1', 30)
    ttl = await redis.ttl('test:key1')
    print(f"✓ TTL test: {ttl} seconds")
    
    # Cleanup
    await redis.delete('test:key1', 'test:hash1', 'test:list1', 'test:set1')
    
    # Check stats
    stats = redis.get_stats()
    print(f"✓ Connection stats: {stats}")
    
    await redis.close()
    print("✓ Basic operations test completed")


async def test_token_manager():
    """Test async token manager"""
    print("\n=== Testing Async Token Manager ===")
    
    redis = AsyncReliableRedis(
        host='192.168.18.25',
        port=6379,
        decode_responses=False
    )
    await redis.connect()
    
    token_manager = AsyncTokenManager(redis)
    
    # Create a token
    token = await token_manager.create_token('testuser', ttl=300)
    print(f"✓ Created token: {token[:20]}...")
    
    # Verify token
    username = await token_manager.verify_token(token)
    print(f"✓ Token verified for: {username}")
    
    # List user tokens
    tokens = await token_manager.list_user_tokens('testuser')
    print(f"✓ User has {len(tokens)} active tokens")
    
    # Get token info
    info = await token_manager.get_token_info(token)
    print(f"✓ Token info: expires_in={info['expires_in']}s")
    
    # Create API key
    api_key = await token_manager.create_api_key(
        'test-key',
        ['read', 'write'],
        ttl=600
    )
    print(f"✓ Created API key: {api_key[:20]}...")
    
    # Verify API key
    key_data = await token_manager.verify_api_key(api_key)
    print(f"✓ API key verified: {key_data['name']}, permissions={key_data['permissions']}")
    
    # Get stats
    stats = await token_manager.get_stats()
    print(f"✓ Token manager stats: {stats.get('tokens_created_total', {})}")
    
    # Cleanup
    await token_manager.revoke_token(token)
    await token_manager.revoke_api_key(api_key)
    
    await redis.close()
    print("✓ Token manager test completed")


async def test_background_task_manager():
    """Test async background task manager"""
    print("\n=== Testing Async Background Task Manager ===")
    
    redis = AsyncReliableRedis(
        host='192.168.18.25',
        port=6379,
        decode_responses=False
    )
    await redis.connect()
    
    task_manager = AsyncBackgroundTaskManager(redis, max_workers=2)
    
    # Define a simple test handler
    async def test_handler(task_id: str, params: dict, manager):
        print(f"  Handler: Processing task {task_id}")
        
        # Simulate some work
        for i in range(5):
            await manager.update_task_progress(task_id, {
                'step': i + 1,
                'total': 5,
                'percentage': (i + 1) * 20
            })
            await asyncio.sleep(0.5)
        
        return {'status': 'success', 'result': params.get('value', 0) * 2}
    
    # Register handler
    task_manager.register_handler('test', test_handler)
    
    # Start the manager
    await task_manager.start()
    print("✓ Task manager started")
    
    # Create some tasks
    task_ids = []
    for i in range(3):
        task_id = await task_manager.create_task('test', {'value': i + 1})
        task_ids.append(task_id)
        print(f"✓ Created task {i+1}: {task_id}")
    
    # Monitor tasks
    print("  Monitoring task progress...")
    completed = 0
    while completed < len(task_ids):
        completed = 0
        for task_id in task_ids:
            task = await task_manager.get_task(task_id)
            if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
                completed += 1
            elif task.progress:
                print(f"  Task {task_id[:8]}: {task.progress.get('percentage', 0)}%")
        
        if completed < len(task_ids):
            await asyncio.sleep(1)
    
    # Check results
    for task_id in task_ids:
        task = await task_manager.get_task(task_id)
        print(f"✓ Task {task_id[:8]} - Status: {task.status.value}, Result: {task.result}")
    
    # Stop the manager
    await task_manager.stop()
    await redis.close()
    print("✓ Task manager test completed")


async def test_reconnection():
    """Test reconnection behavior"""
    print("\n=== Testing Reconnection Behavior ===")
    
    # Test with wrong port to trigger reconnection
    redis = AsyncReliableRedis(
        host='192.168.18.25',
        port=9999,  # Wrong port
        max_retries=2,
        socket_timeout=2,
        socket_connect_timeout=2
    )
    
    try:
        await redis.connect()
    except:
        print("✓ Initial connection failed as expected")
    
    # Now test with correct port
    redis = AsyncReliableRedis(
        host='192.168.18.25',
        port=6379,
        max_retries=3
    )
    
    await redis.connect()
    print("✓ Connected successfully")
    
    # Test operations
    await redis.set('test:reconnect', 'value1')
    value = await redis.get('test:reconnect')
    print(f"✓ Operations working: {value}")
    
    # Cleanup
    await redis.delete('test:reconnect')
    await redis.close()
    
    print("✓ Reconnection test completed")


async def test_concurrent_operations():
    """Test concurrent operations"""
    print("\n=== Testing Concurrent Operations ===")
    
    redis = AsyncReliableRedis(
        host='192.168.18.25',
        port=6379
    )
    await redis.connect()
    
    # Run multiple operations concurrently
    async def operation(n: int):
        key = f'test:concurrent:{n}'
        await redis.set(key, f'value{n}')
        value = await redis.get(key)
        await redis.delete(key)
        return value
    
    start = time.time()
    results = await asyncio.gather(
        *[operation(i) for i in range(20)],
        return_exceptions=True
    )
    elapsed = time.time() - start
    
    # Check results
    successful = sum(1 for r in results if isinstance(r, (str, bytes)))
    print(f"✓ Completed {successful}/20 operations in {elapsed:.2f}s")
    
    await redis.close()
    print("✓ Concurrent operations test completed")


async def main():
    """Run all tests"""
    print("Starting AsyncReliableRedis Tests")
    print("=" * 40)
    
    try:
        await test_basic_operations()
        await test_token_manager()
        await test_background_task_manager()
        await test_reconnection()
        await test_concurrent_operations()
        
        print("\n" + "=" * 40)
        print("✅ All tests completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        logger.exception("Test failed with exception")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())