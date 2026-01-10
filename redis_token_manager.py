#!/usr/bin/env python3
"""
Redis-based token manager for secure ZFS send/receive operations
Uses redis_reliable for robust Redis operations with automatic reconnection
"""

import json
import uuid
import time
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List
import logging

# Import the reliable Redis implementation
from redis_reliable_sync import ReliableRedis, ConnectionFailureError

logger = logging.getLogger(__name__)

class TokenManager:
    def __init__(self, redis_host: str = "192.168.10.152", redis_port: int = 6379, 
                 redis_db: int = 0, password: Optional[str] = None,
                 max_retries: int = 5, raise_on_failure: bool = True):
        """Initialize token manager with reliable Redis connection
        
        Args:
            redis_host: Redis server hostname/IP
            redis_port: Redis port
            redis_db: Redis database number
            password: Redis password if required
            max_retries: Maximum reconnection attempts
            raise_on_failure: Raise exception on connection failure
        """
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.token_prefix = "zfs:token:"
        self.user_tokens_prefix = "zfs:user:tokens:"
        self.stats_prefix = "zfs:stats:"
        
        # Initialize reliable Redis connection
        self.redis = ReliableRedis(
            host=redis_host,
            port=redis_port,
            password=password,
            db=redis_db,
            max_retries=max_retries,
            raise_on_failure=raise_on_failure,
            on_connection_failure=self._handle_redis_failure
        )
        
        logger.info(f"Token manager initialized with Redis at {redis_host}:{redis_port}")
    
    def _handle_redis_failure(self, redis_instance, context):
        """Handle Redis connection failure"""
        logger.critical(f"Redis connection permanently failed: {context}")
        logger.critical(f"Connection stats: {redis_instance.get_stats()}")
        # In production, you might want to alert monitoring systems here
    
    def create_token(self,
                    operation: str,  # "send" or "receive"
                    dataset: str,
                    user_id: str,
                    client_ip: str,
                    snapshot: Optional[str] = None,
                    parameters: Optional[Dict[str, Any]] = None,
                    ttl: int = 300) -> Optional[Dict[str, Any]]:
        """Create a new secure token for ZFS operations
        
        Returns:
            Token info dict or None if Redis operation fails
        """
        # Generate unique token ID
        token_id = str(uuid.uuid4())
        
        # Create token data
        token_data = {
            'token_id': token_id,
            'operation': operation,
            'dataset': dataset,
            'snapshot': snapshot,
            'user_id': user_id,
            'client_ip': client_ip,
            'created_at': datetime.now(timezone.utc).isoformat(),
            'expires_at': (datetime.now(timezone.utc) + timedelta(seconds=ttl)).isoformat(),
            'parameters': parameters or {},
            'used': False,
            'use_count': 0,
            'checksum': None
        }
        
        # Generate checksum for integrity
        checksum_data = f"{token_id}:{operation}:{dataset}:{user_id}"
        token_data['checksum'] = hashlib.sha256(checksum_data.encode()).hexdigest()[:16]
        
        # Store token in Redis with TTL using reliable connection
        token_key = f"{self.token_prefix}{token_id}"
        success = self.redis.set(token_key, json.dumps(token_data), ex=ttl)
        
        if not success:
            logger.error(f"Failed to create token in Redis")
            return None
        
        # Track token for user (for listing/revocation)
        user_tokens_key = f"{self.user_tokens_prefix}{user_id}"
        self.redis.execute('sadd', user_tokens_key, token_id)
        self.redis.execute('expire', user_tokens_key, ttl + 60)  # Slightly longer TTL
        
        # Update statistics
        self.redis.execute('hincrby', f"{self.stats_prefix}created", operation, 1)
        self.redis.execute('hincrby', f"{self.stats_prefix}created", "total", 1)
        
        logger.info(f"Created {operation} token {token_id} for user {user_id}, dataset {dataset}")
        
        return {
            'token': token_id,
            'operation': operation,
            'dataset': dataset,
            'snapshot': snapshot,
            'expires_in': ttl,
            'checksum': token_data['checksum']
        }
    
    def validate_token(self, token_id: str, client_ip: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Validate a token and return its data if valid
        
        Returns:
            Token data dict or None if invalid/not found
        """
        token_key = f"{self.token_prefix}{token_id}"
        
        # Get token data using reliable connection
        token_json = self.redis.get(token_key)
        if not token_json:
            logger.warning(f"Token {token_id} not found")
            self.redis.execute('hincrby', f"{self.stats_prefix}validation", "not_found", 1)
            return None
        
        try:
            token_data = json.loads(token_json)
        except json.JSONDecodeError:
            logger.error(f"Invalid token data for {token_id}")
            self.redis.execute('hincrby', f"{self.stats_prefix}validation", "invalid_data", 1)
            return None
        
        # Check expiration
        expires_at = datetime.fromisoformat(token_data['expires_at'])
        if datetime.now(timezone.utc) > expires_at:
            logger.warning(f"Token {token_id} has expired")
            self.redis.delete(token_key)
            self.redis.execute('hincrby', f"{self.stats_prefix}validation", "expired", 1)
            return None
        
        # Verify checksum
        checksum_data = f"{token_id}:{token_data['operation']}:{token_data['dataset']}:{token_data['user_id']}"
        expected_checksum = hashlib.sha256(checksum_data.encode()).hexdigest()[:16]
        if token_data.get('checksum') != expected_checksum:
            logger.error(f"Token {token_id} checksum mismatch")
            self.redis.execute('hincrby', f"{self.stats_prefix}validation", "checksum_fail", 1)
            return None
        
        # Optional: Verify client IP (can be disabled for NAT environments)
        if client_ip and token_data.get('client_ip') != client_ip:
            logger.warning(f"Token {token_id} used from different IP: {client_ip} != {token_data.get('client_ip')}")
            # For now, just log but don't reject
        
        # Check if already used (for single-use tokens)
        if token_data.get('used') and token_data.get('use_count', 0) > 0:
            logger.warning(f"Token {token_id} has already been used")
            self.redis.execute('hincrby', f"{self.stats_prefix}validation", "already_used", 1)
            # You might want to allow this for resumable operations
            # return None
        
        self.redis.execute('hincrby', f"{self.stats_prefix}validation", "success", 1)
        return token_data
    
    def mark_token_used(self, token_id: str, client_ip: Optional[str] = None) -> bool:
        """Mark a token as used
        
        Returns:
            True if successful, False otherwise
        """
        token_key = f"{self.token_prefix}{token_id}"
        
        # Get current token data
        token_json = self.redis.get(token_key)
        if not token_json:
            return False
        
        try:
            token_data = json.loads(token_json)
        except json.JSONDecodeError:
            return False
        
        # Update token data
        token_data['used'] = True
        token_data['use_count'] = token_data.get('use_count', 0) + 1
        token_data['last_used_at'] = datetime.now(timezone.utc).isoformat()
        if client_ip:
            token_data['last_used_ip'] = client_ip
        
        # Get remaining TTL and update token
        ttl = self.redis.execute('ttl', token_key)
        if ttl and ttl > 0:
            success = self.redis.set(token_key, json.dumps(token_data), ex=ttl)
            if success:
                # Update statistics
                self.redis.execute('hincrby', f"{self.stats_prefix}used", token_data['operation'], 1)
                logger.info(f"Marked token {token_id} as used")
                return True
        
        return False
    
    def revoke_token(self, token_id: str) -> bool:
        """Revoke a token immediately
        
        Returns:
            True if token was revoked, False if not found
        """
        token_key = f"{self.token_prefix}{token_id}"
        
        # Get token data first (for cleanup)
        token_json = self.redis.get(token_key)
        if token_json:
            try:
                token_data = json.loads(token_json)
                user_id = token_data.get('user_id')
                
                # Remove from user's token set
                if user_id:
                    self.redis.execute('srem', f"{self.user_tokens_prefix}{user_id}", token_id)
            except json.JSONDecodeError:
                pass
        
        # Delete the token
        result = self.redis.delete(token_key)
        
        if result:
            logger.info(f"Revoked token {token_id}")
            self.redis.execute('hincrby', f"{self.stats_prefix}revoked", "total", 1)
            return True
        
        return False
    
    def list_user_tokens(self, user_id: str) -> List[Dict[str, Any]]:
        """List all active tokens for a user
        
        Returns:
            List of token data dicts
        """
        user_tokens_key = f"{self.user_tokens_prefix}{user_id}"
        token_ids = self.redis.execute('smembers', user_tokens_key)
        
        if not token_ids:
            return []
        
        tokens = []
        for token_id in token_ids:
            token_json = self.redis.get(f"{self.token_prefix}{token_id}")
            if token_json:
                try:
                    token_data = json.loads(token_json)
                    tokens.append(token_data)
                except json.JSONDecodeError:
                    continue
        
        return tokens
    
    def get_stats(self) -> Dict[str, Any]:
        """Get token statistics
        
        Returns:
            Statistics dictionary
        """
        stats = {}
        
        # Get all stats from Redis
        created = self.redis.execute('hgetall', f"{self.stats_prefix}created")
        used = self.redis.execute('hgetall', f"{self.stats_prefix}used")
        validation = self.redis.execute('hgetall', f"{self.stats_prefix}validation")
        revoked = self.redis.execute('hgetall', f"{self.stats_prefix}revoked")
        
        # Convert to integers and ensure keys are strings
        stats['created'] = {k.decode('utf-8') if isinstance(k, bytes) else k: int(v) for k, v in (created or {}).items()}
        stats['used'] = {k.decode('utf-8') if isinstance(k, bytes) else k: int(v) for k, v in (used or {}).items()}
        stats['validation'] = {k.decode('utf-8') if isinstance(k, bytes) else k: int(v) for k, v in (validation or {}).items()}
        stats['revoked'] = {k.decode('utf-8') if isinstance(k, bytes) else k: int(v) for k, v in (revoked or {}).items()}
        
        # Count active tokens (approximate due to Redis scan limitations)
        active_count = 0
        pattern = f"{self.token_prefix}*"
        keys = self.redis.keys(pattern)
        if keys:
            active_count = len(keys)
        
        stats['active_tokens'] = active_count
        stats['redis_connected'] = self.redis.is_connected()
        stats['redis_stats'] = self.redis.get_stats()
        
        return stats
    
    def cleanup_expired(self) -> int:
        """Clean up expired tokens (Redis does this automatically with TTL, but this is for stats)
        
        Returns:
            Number of tokens cleaned up
        """
        cleaned = 0
        pattern = f"{self.token_prefix}*"
        keys = self.redis.keys(pattern)
        
        if not keys:
            return 0
        
        for key in keys:
            token_json = self.redis.get(key)
            if token_json:
                try:
                    token_data = json.loads(token_json)
                    expires_at = datetime.fromisoformat(token_data['expires_at'])
                    if datetime.now(timezone.utc) > expires_at:
                        self.redis.delete(key)
                        cleaned += 1
                except (json.JSONDecodeError, KeyError, ValueError):
                    # Invalid token data, remove it
                    self.redis.delete(key)
                    cleaned += 1
        
        if cleaned > 0:
            logger.info(f"Cleaned up {cleaned} expired tokens")
        
        return cleaned