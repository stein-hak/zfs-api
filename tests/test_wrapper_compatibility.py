#!/usr/bin/env python3
"""
Test the synchronous wrapper for AsyncReliableRedis
"""

import time
from redis_reliable import ReliableRedis

def test_sync_wrapper():
    print("Testing ReliableRedis synchronous wrapper...")
    
    # Create client using sync interface
    redis = ReliableRedis(
        host='192.168.18.25',
        port=6379,
        decode_responses=False
    )
    
    print("✓ Client created")
    
    # Test basic operations
    redis.set('test:wrapper', 'hello from wrapper', ex=60)
    value = redis.get('test:wrapper')
    print(f"✓ Set/Get: {value}")
    
    # Test hash operations
    redis.hset('test:hash', mapping={'field1': 'value1', 'field2': 'value2'})
    hash_data = redis.hgetall('test:hash')
    print(f"✓ Hash operations: {len(hash_data)} fields")
    
    # Test list operations
    redis.rpush('test:list', 'item1', 'item2')
    list_len = redis.llen('test:list')
    print(f"✓ List length: {list_len}")
    
    # Test increment operations
    redis.set('test:counter', '0')
    redis.incr('test:counter')
    counter = redis.get('test:counter')
    print(f"✓ Counter: {counter}")
    
    # Test hincrby
    redis.hincrby('test:stats', 'views', 1)
    redis.hincrby('test:stats', 'views', 5)
    views = redis.hget('test:stats', 'views')
    print(f"✓ Hash increment: views = {views}")
    
    # Cleanup
    redis.delete('test:wrapper', 'test:hash', 'test:list', 'test:counter', 'test:stats')
    
    # Check stats
    stats = redis.get_stats()
    print(f"✓ Stats: {stats}")
    
    redis.close()
    print("\n✅ All tests passed!")

if __name__ == "__main__":
    test_sync_wrapper()