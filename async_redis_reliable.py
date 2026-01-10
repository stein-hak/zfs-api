#!/usr/bin/env python3
"""
Async Redis Reliable - An async version of ReliableRedis with automatic reconnection
and configurable failure handling for use in async/await contexts.

This module provides an async Redis client that automatically handles connection
errors and reconnects with exponential backoff.
"""

import redis.asyncio as redis
from redis.exceptions import ConnectionError as RedisConnectionError
import asyncio
import json
import logging
from typing import Optional, Callable, Any, Dict

class AsyncConnectionFailureError(Exception):
    """Raised when Redis connection fails after all retry attempts."""
    pass

class AsyncReliableRedis:
    """An async reliable Redis client with automatic reconnection capabilities."""
    
    def __init__(self, host='localhost', port=6379, password=None, db=0,
                 socket_timeout=5, socket_connect_timeout=5, logger=None,
                 max_retries=5, on_connection_failure=None, raise_on_failure=False,
                 decode_responses=False):
        """Initialize the async reliable Redis client.
        
        Args:
            host (str): Redis host address
            port (int): Redis port number
            password (str, optional): Redis password
            db (int): Redis database number
            socket_timeout (int): Socket timeout in seconds
            socket_connect_timeout (int): Connection timeout in seconds
            logger (logging.Logger, optional): Logger instance, creates one if not provided
            max_retries (int): Maximum number of reconnection attempts
            on_connection_failure (callable, optional): Async callback function to call on persistent failure
            raise_on_failure (bool): If True, raise AsyncConnectionFailureError on persistent failure
            decode_responses (bool): If True, decode Redis responses to strings
        """
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.socket_timeout = socket_timeout
        self.socket_connect_timeout = socket_connect_timeout
        self.max_retries = max_retries
        self.on_connection_failure = on_connection_failure
        self.raise_on_failure = raise_on_failure
        self.decode_responses = decode_responses
        
        # Track consecutive failures
        self.consecutive_failures = 0
        self.total_reconnection_attempts = 0
        
        # Setup logging
        self.logger = logger or self._setup_logger()
        
        # Redis client will be initialized asynchronously
        self.client: Optional[redis.Redis] = None
        self._lock = asyncio.Lock()  # For thread-safe reconnection
    
    def _setup_logger(self):
        """Create a default logger if none was provided."""
        logger = logging.getLogger('async_redis_reliable')
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    async def connect(self):
        """Connect to Redis. Must be called before using the client."""
        if not await self._initialize_connection():
            await self._handle_persistent_failure("Initial connection failed")
    
    async def _initialize_connection(self):
        """Initialize the Redis connection."""
        try:
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                password=self.password,
                db=self.db,
                socket_timeout=self.socket_timeout,
                socket_connect_timeout=self.socket_connect_timeout,
                decode_responses=self.decode_responses
            )
            
            # Test the connection
            await self.client.ping()
            self.logger.info("Async Redis connection initialized successfully")
            self.consecutive_failures = 0  # Reset on success
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize async Redis connection: {e}")
            return False
    
    async def _handle_persistent_failure(self, context: str):
        """Handle persistent connection failure according to configuration."""
        self.logger.critical(f"Async Redis connection permanently failed: {context}")
        
        if self.on_connection_failure:
            self.logger.info("Calling async connection failure handler")
            try:
                if asyncio.iscoroutinefunction(self.on_connection_failure):
                    await self.on_connection_failure(self, context)
                else:
                    self.on_connection_failure(self, context)
            except Exception as e:
                self.logger.error(f"Error in connection failure handler: {e}")
        
        if self.raise_on_failure:
            raise AsyncConnectionFailureError(f"Redis connection failed after {self.max_retries} attempts: {context}")
    
    async def _reconnect(self):
        """Reconnect to Redis with exponential backoff."""
        async with self._lock:  # Prevent concurrent reconnection attempts
            retry_delay = 1  # seconds
            
            self.total_reconnection_attempts += 1
            
            for attempt in range(1, self.max_retries + 1):
                try:
                    self.logger.info(f"Async Redis reconnection attempt {attempt}/{self.max_retries}")
                    
                    # Close existing connection if any
                    if self.client:
                        await self.client.close()
                    
                    success = await self._initialize_connection()
                    if success:
                        self.logger.info("Successfully reconnected to Redis")
                        return True
                except Exception as e:
                    self.logger.warning(f"Reconnection attempt {attempt} failed: {e}")
                
                if attempt < self.max_retries:
                    await asyncio.sleep(retry_delay)
                    # Exponential backoff with cap
                    retry_delay = min(retry_delay * 2, 10)
            
            self.consecutive_failures += 1
            self.logger.error(f"Failed to reconnect to Redis after {self.max_retries} attempts")
            await self._handle_persistent_failure(f"Reconnection failed after {self.max_retries} attempts")
            return False
    
    async def execute(self, operation: str, *args, **kwargs):
        """Execute a Redis operation with automatic reconnection on failure.
        
        Args:
            operation (str): Name of the Redis operation to execute
            *args: Arguments to pass to the Redis operation
            **kwargs: Keyword arguments to pass to the Redis operation
            
        Returns:
            The result of the Redis operation or None if failed
            
        Raises:
            AsyncConnectionFailureError: If raise_on_failure is True and reconnection fails
        """
        if not self.client:
            self.logger.error("Redis client not initialized. Call connect() first.")
            return None
            
        try:
            # Get the method from the Redis client
            method = getattr(self.client, operation)
            result = await method(*args, **kwargs)
            self.consecutive_failures = 0  # Reset on success
            return result
        except RedisConnectionError as e:
            self.logger.warning(f"Redis connection error during {operation}: {e}")
            if await self._reconnect():
                # Retry the operation after reconnection
                try:
                    method = getattr(self.client, operation)
                    return await method(*args, **kwargs)
                except Exception as retry_e:
                    self.logger.error(f"Operation {operation} failed after reconnection: {retry_e}")
                    return None
            else:
                # Reconnection failed - already handled by _reconnect
                return None
        except Exception as e:
            self.logger.error(f"Error during Redis operation {operation}: {e}")
            return None
    
    # Convenience methods for common operations
    
    async def get(self, key: str):
        """Get a value by key with automatic reconnection."""
        return await self.execute('get', key)
    
    async def set(self, key: str, value: Any, ex: Optional[int] = None):
        """Set a key-value pair with automatic reconnection."""
        return await self.execute('set', key, value, ex=ex)
    
    async def delete(self, *keys):
        """Delete keys with automatic reconnection."""
        return await self.execute('delete', *keys)
    
    async def exists(self, *keys):
        """Check if keys exist with automatic reconnection."""
        return await self.execute('exists', *keys)
    
    async def publish(self, channel: str, message: Any):
        """Publish a message to a channel with automatic reconnection."""
        if isinstance(message, (dict, list)):
            message = json.dumps(message)
        return await self.execute('publish', channel, message)
    
    async def ping(self):
        """Ping Redis server with automatic reconnection."""
        return await self.execute('ping')
    
    async def blpop(self, *keys, timeout: int = 0):
        """Blocking pop from list with automatic reconnection."""
        return await self.execute('blpop', *keys, timeout=timeout)
    
    async def rpush(self, key: str, *values):
        """Push values to the right of a list with automatic reconnection."""
        return await self.execute('rpush', key, *values)
    
    async def lpush(self, key: str, *values):
        """Push values to the left of a list with automatic reconnection."""
        return await self.execute('lpush', key, *values)
    
    async def llen(self, key: str):
        """Get the length of a list with automatic reconnection."""
        return await self.execute('llen', key)
    
    async def keys(self, pattern: str = '*'):
        """Get keys matching pattern with automatic reconnection."""
        return await self.execute('keys', pattern)
    
    async def hgetall(self, key: str) -> Dict[Any, Any]:
        """Get all fields and values of a hash with automatic reconnection."""
        result = await self.execute('hgetall', key)
        return result or {}
    
    async def hset(self, key: str, field: Optional[str] = None, 
                   value: Optional[Any] = None, mapping: Optional[Dict] = None):
        """Set hash fields with automatic reconnection.
        
        Can be called as:
        - hset(key, field, value) for single field
        - hset(key, mapping=dict) for multiple fields
        """
        if mapping is not None:
            return await self.execute('hset', key, mapping=mapping)
        elif field is not None and value is not None:
            return await self.execute('hset', key, field, value)
        else:
            raise ValueError("Either field/value or mapping must be provided")
    
    async def hget(self, key: str, field: str):
        """Get a hash field value with automatic reconnection."""
        return await self.execute('hget', key, field)
    
    async def expire(self, key: str, seconds: int):
        """Set key expiration with automatic reconnection."""
        return await self.execute('expire', key, seconds)
    
    async def ttl(self, key: str):
        """Get time to live for a key with automatic reconnection."""
        return await self.execute('ttl', key)
    
    async def incr(self, key: str):
        """Increment a key with automatic reconnection."""
        return await self.execute('incr', key)
    
    async def decr(self, key: str):
        """Decrement a key with automatic reconnection."""
        return await self.execute('decr', key)
    
    async def hincrby(self, key: str, field: str, increment: int):
        """Increment a hash field with automatic reconnection."""
        return await self.execute('hincrby', key, field, increment)
    
    async def sadd(self, key: str, *values):
        """Add members to a set with automatic reconnection."""
        return await self.execute('sadd', key, *values)
    
    async def srem(self, key: str, *values):
        """Remove members from a set with automatic reconnection."""
        return await self.execute('srem', key, *values)
    
    async def smembers(self, key: str):
        """Get all members of a set with automatic reconnection."""
        return await self.execute('smembers', key)
    
    async def sismember(self, key: str, value: Any):
        """Check if value is a member of set with automatic reconnection."""
        return await self.execute('sismember', key, value)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get connection statistics."""
        return {
            'consecutive_failures': self.consecutive_failures,
            'total_reconnection_attempts': self.total_reconnection_attempts,
            'connected': self.is_connected()
        }
    
    def is_connected(self) -> bool:
        """Check if currently connected to Redis."""
        if not self.client:
            return False
        try:
            # Use sync ping check to avoid async in property
            # This is safe as it's just a connection state check
            return self.client._pool is not None
        except:
            return False
    
    async def close(self):
        """Close the Redis connection."""
        if self.client:
            await self.client.close()
            self.logger.info("Async Redis connection closed")


# Example usage
async def example():
    """Example of using AsyncReliableRedis"""
    
    async def handle_redis_failure(redis_instance, context):
        """Async handler called when Redis connection permanently fails."""
        print(f"CRITICAL: Redis permanently failed - {context}")
        print(f"Stats: {redis_instance.get_stats()}")
    
    # Create async Redis client
    redis_client = AsyncReliableRedis(
        host='localhost',
        port=6379,
        max_retries=3,
        on_connection_failure=handle_redis_failure,
        raise_on_failure=False
    )
    
    # Connect to Redis
    await redis_client.connect()
    
    # Normal operations
    await redis_client.set('test_key', 'test_value', ex=3600)
    value = await redis_client.get('test_key')
    print(f"Retrieved: {value}")
    
    # Hash operations
    await redis_client.hset('test_hash', mapping={'field1': 'value1', 'field2': 'value2'})
    hash_data = await redis_client.hgetall('test_hash')
    print(f"Hash data: {hash_data}")
    
    # List operations
    await redis_client.rpush('test_list', 'item1', 'item2', 'item3')
    list_len = await redis_client.llen('test_list')
    print(f"List length: {list_len}")
    
    # Clean up
    await redis_client.close()


if __name__ == "__main__":
    asyncio.run(example())