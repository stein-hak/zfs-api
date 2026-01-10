#!/usr/bin/env python3
"""
Redis Reliable Enhanced - An enhanced version with configurable failure handling
including options to raise exceptions or call exit handlers on persistent failures.

This module provides classes for robust Redis operations that automatically
handle connection errors and reconnect with exponential backoff, with options
for handling persistent connection failures.
"""

import redis
import time
import json
import logging
import sys
from typing import Optional, Callable, Any

class ConnectionFailureError(Exception):
    """Raised when Redis connection fails after all retry attempts."""
    pass

class ReliableRedis:
    """A reliable Redis client with automatic reconnection capabilities."""
    
    def __init__(self, host='localhost', port=6379, password=None, db=0, 
                 socket_timeout=5, socket_connect_timeout=5, logger=None,
                 max_retries=5, on_connection_failure=None, raise_on_failure=False):
        """Initialize the reliable Redis client.
        
        Args:
            host (str): Redis host address
            port (int): Redis port number
            password (str, optional): Redis password
            db (int): Redis database number
            socket_timeout (int): Socket timeout in seconds
            socket_connect_timeout (int): Connection timeout in seconds
            logger (logging.Logger, optional): Logger instance, creates one if not provided
            max_retries (int): Maximum number of reconnection attempts
            on_connection_failure (callable, optional): Callback function to call on persistent failure
            raise_on_failure (bool): If True, raise ConnectionFailureError on persistent failure
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
        
        # Track consecutive failures
        self.consecutive_failures = 0
        self.total_reconnection_attempts = 0
        
        # Setup logging
        self.logger = logger or self._setup_logger()
        
        # Initialize connection pool and client
        if not self._initialize_connection():
            self._handle_persistent_failure("Initial connection failed")
    
    def _setup_logger(self):
        """Create a default logger if none was provided."""
        logger = logging.getLogger('redis_reliable')
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def _initialize_connection(self):
        """Initialize the Redis connection pool and client."""
        try:
            self.pool = redis.ConnectionPool(
                host=self.host,
                port=self.port,
                password=self.password,
                db=self.db,
                socket_timeout=self.socket_timeout,
                socket_connect_timeout=self.socket_connect_timeout
            )
            self.client = redis.Redis(connection_pool=self.pool)
            # Test the connection
            self.client.ping()
            self.logger.info("Redis connection initialized successfully")
            self.consecutive_failures = 0  # Reset on success
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize Redis connection: {e}")
            return False
    
    def _handle_persistent_failure(self, context: str):
        """Handle persistent connection failure according to configuration."""
        self.logger.critical(f"Redis connection permanently failed: {context}")
        
        if self.on_connection_failure:
            self.logger.info("Calling connection failure handler")
            try:
                self.on_connection_failure(self, context)
            except Exception as e:
                self.logger.error(f"Error in connection failure handler: {e}")
        
        if self.raise_on_failure:
            raise ConnectionFailureError(f"Redis connection failed after {self.max_retries} attempts: {context}")
    
    def _reconnect(self):
        """Reconnect to Redis with exponential backoff."""
        retry_delay = 1  # seconds
        
        self.total_reconnection_attempts += 1
        
        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.info(f"Redis reconnection attempt {attempt}/{self.max_retries}")
                success = self._initialize_connection()
                if success:
                    self.logger.info("Successfully reconnected to Redis")
                    return True
            except Exception as e:
                self.logger.warning(f"Reconnection attempt {attempt} failed: {e}")
            
            if attempt < self.max_retries:
                time.sleep(retry_delay)
                # Exponential backoff with cap
                retry_delay = min(retry_delay * 2, 10)
        
        self.consecutive_failures += 1
        self.logger.error(f"Failed to reconnect to Redis after {self.max_retries} attempts")
        self._handle_persistent_failure(f"Reconnection failed after {self.max_retries} attempts")
        return False
    
    def execute(self, operation, *args, **kwargs):
        """Execute a Redis operation with automatic reconnection on failure.
        
        Args:
            operation (str): Name of the Redis operation to execute
            *args: Arguments to pass to the Redis operation
            **kwargs: Keyword arguments to pass to the Redis operation
            
        Returns:
            The result of the Redis operation or None if failed
        
        Raises:
            ConnectionFailureError: If raise_on_failure is True and reconnection fails
        """
        try:
            # Get the method from the Redis client
            method = getattr(self.client, operation)
            result = method(*args, **kwargs)
            self.consecutive_failures = 0  # Reset on success
            return result
        except redis.exceptions.ConnectionError as e:
            self.logger.warning(f"Redis connection error during {operation}: {e}")
            if self._reconnect():
                # Retry the operation after reconnection
                try:
                    method = getattr(self.client, operation)
                    return method(*args, **kwargs)
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
    
    def get(self, key):
        """Get a value by key with automatic reconnection."""
        return self.execute('get', key)
    
    def set(self, key, value, ex=None):
        """Set a key-value pair with automatic reconnection."""
        return self.execute('set', key, value, ex=ex)
    
    def delete(self, key):
        """Delete a key with automatic reconnection."""
        return self.execute('delete', key)
    
    def exists(self, key):
        """Check if a key exists with automatic reconnection."""
        return self.execute('exists', key)
    
    def publish(self, channel, message):
        """Publish a message to a channel with automatic reconnection."""
        if isinstance(message, (dict, list)):
            message = json.dumps(message)
        return self.execute('publish', channel, message)
    
    def ping(self):
        """Ping Redis server with automatic reconnection."""
        return self.execute('ping')
    
    def blpop(self, *keys, timeout=0):
        """Blocking pop from list with automatic reconnection."""
        # Add timeout as the last argument for blpop
        args = list(keys) + [timeout]
        return self.execute('blpop', *args)
    
    def rpush(self, key, *values):
        """Push values to the right of a list with automatic reconnection."""
        return self.execute('rpush', key, *values)
    
    def keys(self, pattern='*'):
        """Get keys matching pattern with automatic reconnection."""
        return self.execute('keys', pattern)
    
    def hgetall(self, key):
        """Get all fields and values of a hash with automatic reconnection."""
        return self.execute('hgetall', key)
    
    def hset(self, key, field=None, value=None, mapping=None):
        """Set hash fields with automatic reconnection.
        
        Can be called as:
        - hset(key, field, value) for single field
        - hset(key, mapping=dict) for multiple fields
        """
        try:
            if mapping is not None:
                # Modern Redis-py accepts mapping parameter
                method = getattr(self.client, 'hset')
                result = method(key, mapping=mapping)
                self.consecutive_failures = 0
                return result
            elif field is not None and value is not None:
                return self.execute('hset', key, field, value)
            else:
                raise ValueError("Either field/value or mapping must be provided")
        except redis.exceptions.ConnectionError as e:
            self.logger.warning(f"Redis connection error during hset: {e}")
            if self._reconnect():
                # Retry after reconnection
                try:
                    if mapping is not None:
                        method = getattr(self.client, 'hset')
                        return method(key, mapping=mapping)
                    else:
                        return self.execute('hset', key, field, value)
                except Exception as retry_e:
                    self.logger.error(f"Operation hset failed after reconnection: {retry_e}")
                    return None
            return None
        except Exception as e:
            self.logger.error(f"Error during Redis operation hset: {e}")
            return None
    
    def expire(self, key, seconds):
        """Set key expiration with automatic reconnection."""
        return self.execute('expire', key, seconds)
    
    def get_stats(self):
        """Get connection statistics."""
        return {
            'consecutive_failures': self.consecutive_failures,
            'total_reconnection_attempts': self.total_reconnection_attempts,
            'connected': self.is_connected()
        }
    
    def is_connected(self):
        """Check if currently connected to Redis."""
        try:
            self.client.ping()
            return True
        except:
            return False


class ReliablePubSub:
    """A reliable Redis PubSub client with automatic reconnection capabilities."""
    
    def __init__(self, host='localhost', port=6379, password=None, db=0,
                 socket_timeout=5, socket_connect_timeout=5, logger=None,
                 max_retries=5, on_connection_failure=None, raise_on_failure=False):
        """Initialize the reliable Redis PubSub client.
        
        Args:
            host (str): Redis host address
            port (int): Redis port number
            password (str, optional): Redis password
            db (int): Redis database number
            socket_timeout (int): Socket timeout in seconds
            socket_connect_timeout (int): Connection timeout in seconds
            logger (logging.Logger, optional): Logger instance, creates one if not provided
            max_retries (int): Maximum number of reconnection attempts
            on_connection_failure (callable, optional): Callback function to call on persistent failure
            raise_on_failure (bool): If True, raise ConnectionFailureError on persistent failure
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
        
        # Store subscriptions to restore after reconnection
        self.subscriptions = set()
        self.psubscriptions = set()
        
        # Track failures
        self.consecutive_failures = 0
        self.total_reconnection_attempts = 0
        
        # Setup logging
        self.logger = logger or self._setup_logger()
        
        # Initialize connection
        if not self._initialize_connection():
            self._handle_persistent_failure("Initial PubSub connection failed")
    
    def _setup_logger(self):
        """Create a default logger if none was provided."""
        logger = logging.getLogger('redis_reliable_pubsub')
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def _handle_persistent_failure(self, context: str):
        """Handle persistent connection failure according to configuration."""
        self.logger.critical(f"Redis PubSub connection permanently failed: {context}")
        
        if self.on_connection_failure:
            self.logger.info("Calling connection failure handler")
            try:
                self.on_connection_failure(self, context)
            except Exception as e:
                self.logger.error(f"Error in connection failure handler: {e}")
        
        if self.raise_on_failure:
            raise ConnectionFailureError(f"Redis PubSub connection failed after {self.max_retries} attempts: {context}")
    
    def _initialize_connection(self):
        """Initialize the Redis PubSub connection."""
        try:
            # Create a standalone Redis connection for pubsub
            self.redis_client = redis.Redis(
                host=self.host,
                port=self.port,
                password=self.password,
                db=self.db,
                socket_timeout=self.socket_timeout,
                socket_connect_timeout=self.socket_connect_timeout
            )
            
            # Test the connection
            self.redis_client.ping()
            
            # Create pubsub object
            self.pubsub = self.redis_client.pubsub(ignore_subscribe_messages=True)
            
            # Restore subscriptions if any
            if self.subscriptions:
                self.pubsub.subscribe(*self.subscriptions)
                self.logger.info(f"Restored {len(self.subscriptions)} channel subscriptions")
            
            if self.psubscriptions:
                self.pubsub.psubscribe(*self.psubscriptions)
                self.logger.info(f"Restored {len(self.psubscriptions)} pattern subscriptions")
            
            self.logger.info("Redis PubSub connection initialized successfully")
            self.consecutive_failures = 0
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Redis PubSub connection: {e}")
            return False
    
    def _reconnect(self):
        """Reconnect to Redis PubSub with exponential backoff."""
        retry_delay = 1  # seconds
        
        self.total_reconnection_attempts += 1
        
        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.info(f"Redis PubSub reconnection attempt {attempt}/{self.max_retries}")
                success = self._initialize_connection()
                if success:
                    self.logger.info("Successfully reconnected to Redis PubSub")
                    return True
            except Exception as e:
                self.logger.warning(f"PubSub reconnection attempt {attempt} failed: {e}")
            
            if attempt < self.max_retries:
                time.sleep(retry_delay)
                # Exponential backoff with cap
                retry_delay = min(retry_delay * 2, 10)
        
        self.consecutive_failures += 1
        self.logger.error(f"Failed to reconnect to Redis PubSub after {self.max_retries} attempts")
        self._handle_persistent_failure(f"PubSub reconnection failed after {self.max_retries} attempts")
        return False
    
    def subscribe(self, *channels):
        """Subscribe to channels with automatic reconnection."""
        self.subscriptions.update(channels)
        try:
            self.pubsub.subscribe(*channels)
            self.logger.info(f"Subscribed to channels: {channels}")
            return True
        except redis.exceptions.ConnectionError as e:
            self.logger.warning(f"Connection error during subscribe: {e}")
            return self._reconnect()
        except Exception as e:
            self.logger.error(f"Error during subscribe: {e}")
            return False
    
    def psubscribe(self, *patterns):
        """Subscribe to patterns with automatic reconnection."""
        self.psubscriptions.update(patterns)
        try:
            self.pubsub.psubscribe(*patterns)
            self.logger.info(f"Pattern subscribed to: {patterns}")
            return True
        except redis.exceptions.ConnectionError as e:
            self.logger.warning(f"Connection error during psubscribe: {e}")
            return self._reconnect()
        except Exception as e:
            self.logger.error(f"Error during psubscribe: {e}")
            return False
    
    def get_message(self, timeout=1.0):
        """Get a message with automatic reconnection on failure."""
        try:
            message = self.pubsub.get_message(timeout=timeout)
            if message and message['type'] in ('message', 'pmessage'):
                # Try to parse JSON if possible
                try:
                    message['data'] = json.loads(message['data'])
                except (json.JSONDecodeError, TypeError):
                    pass  # Keep original data if not JSON
            self.consecutive_failures = 0
            return message
        except redis.exceptions.ConnectionError as e:
            self.logger.warning(f"Connection error during get_message: {e}")
            if self._reconnect():
                # Try once more after reconnection
                try:
                    return self.pubsub.get_message(timeout=timeout)
                except Exception:
                    return None
            return None
        except Exception as e:
            self.logger.error(f"Error during get_message: {e}")
            return None
    
    def listen(self):
        """Listen for messages with automatic reconnection."""
        while True:
            try:
                for message in self.pubsub.listen():
                    if message['type'] in ('message', 'pmessage'):
                        # Try to parse JSON if possible
                        try:
                            message['data'] = json.loads(message['data'])
                        except (json.JSONDecodeError, TypeError):
                            pass  # Keep original data if not JSON
                    self.consecutive_failures = 0
                    yield message
            except redis.exceptions.ConnectionError as e:
                self.logger.warning(f"Connection error during listen: {e}")
                if not self._reconnect():
                    # Reconnection failed - exit the generator
                    return
            except Exception as e:
                self.logger.error(f"Error during listen: {e}")
                return
    
    def close(self):
        """Close the PubSub connection."""
        try:
            self.pubsub.close()
            self.redis_client.close()
            self.logger.info("PubSub connection closed")
        except Exception as e:
            self.logger.error(f"Error closing PubSub connection: {e}")
    
    def get_stats(self):
        """Get connection statistics."""
        return {
            'consecutive_failures': self.consecutive_failures,
            'total_reconnection_attempts': self.total_reconnection_attempts,
            'subscriptions': len(self.subscriptions),
            'pattern_subscriptions': len(self.psubscriptions)
        }


# Example usage with clean exit handling
if __name__ == "__main__":
    import signal
    
    def handle_redis_failure(redis_instance, context):
        """Handler called when Redis connection permanently fails."""
        print(f"CRITICAL: Redis permanently failed - {context}")
        print(f"Stats: {redis_instance.get_stats()}")
        # Clean shutdown
        sys.exit(1)
    
    # Example with clean exit on failure
    try:
        # Create Redis client that will exit cleanly on persistent failure
        redis_client = ReliableRedis(
            host='localhost',
            port=6379,
            max_retries=3,
            on_connection_failure=handle_redis_failure,
            raise_on_failure=False  # Use callback instead of exception
        )
        
        # Normal operations
        redis_client.set('test_key', 'test_value')
        value = redis_client.get('test_key')
        print(f"Retrieved: {value}")
        
    except ConnectionFailureError as e:
        print(f"Connection failed: {e}")
        sys.exit(1)