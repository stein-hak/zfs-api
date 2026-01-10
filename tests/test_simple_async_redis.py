#!/usr/bin/env python3
"""
Simple test for AsyncReliableRedis
"""

import asyncio
import logging
from async_redis_reliable import AsyncReliableRedis

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def main():
    print("Testing AsyncReliableRedis connection...")
    
    redis = AsyncReliableRedis(
        host='192.168.18.25',
        port=6379,
        max_retries=3,
        socket_timeout=5
    )
    
    try:
        # Connect
        await redis.connect()
        print("✓ Connected to Redis")
        
        # Simple set/get test
        await redis.set('test:simple', 'hello world', ex=60)
        value = await redis.get('test:simple')
        print(f"✓ Set/Get test: {value}")
        
        # Clean up
        await redis.delete('test:simple')
        print("✓ Cleanup done")
        
        # Close connection
        await redis.close()
        print("✓ Connection closed")
        
        print("\n✅ Test completed successfully!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())