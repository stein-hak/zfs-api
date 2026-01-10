#!/usr/bin/env python3
"""
Async Redis Token Manager - An async version of TokenManager using AsyncReliableRedis
for non-blocking Redis operations in async contexts.
"""

import json
import time
import hashlib
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List
from async_redis_reliable import AsyncReliableRedis


class AsyncTokenManager:
    """Async token management using Redis with automatic reconnection."""
    
    def __init__(self, redis_client: AsyncReliableRedis):
        """Initialize with an AsyncReliableRedis instance."""
        self.redis = redis_client
        self.token_prefix = "auth:token:"
        self.user_tokens_prefix = "auth:user:tokens:"
        self.stats_prefix = "auth:stats:"
        self.token_stats_prefix = "auth:token:stats:"
    
    async def create_token(self,
                          operation: str = None,  # "send" or "receive" - for ZFS operations
                          dataset: str = None,
                          user_id: str = None,
                          client_ip: str = None,
                          snapshot: Optional[str] = None,
                          parameters: Optional[Dict[str, Any]] = None,
                          username: str = None,  # For auth tokens
                          ttl: int = 86400) -> Any:
        """Create a new token - either for ZFS operations or authentication.
        
        For ZFS operations: provide operation, dataset, user_id, client_ip
        For auth tokens: provide username
        
        Args:
            operation: "send" or "receive" for ZFS operations
            dataset: ZFS dataset name
            user_id: User ID for ZFS operations
            client_ip: Client IP address
            snapshot: Snapshot name (for send operations)
            parameters: Additional parameters
            username: Username for auth tokens
            ttl: Time to live in seconds
            
        Returns:
            Token info dict for ZFS ops or token string for auth
        """
        # Check if this is a ZFS operation token
        if operation and dataset and user_id:
            return await self._create_zfs_token(
                operation, dataset, user_id, client_ip,
                snapshot, parameters, ttl
            )
        # Otherwise, create auth token
        elif username:
            return await self._create_auth_token(username, ttl)
        else:
            raise ValueError("Either provide ZFS operation params or username")
    
    async def _create_zfs_token(self, operation: str, dataset: str, user_id: str,
                               client_ip: str, snapshot: Optional[str],
                               parameters: Optional[Dict[str, Any]], ttl: int) -> Dict[str, Any]:
        """Create a ZFS operation token (send/receive)"""
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
        
        # Store token in Redis with TTL
        token_key = f"zfs:token:{token_id}"
        await self.redis.set(token_key, json.dumps(token_data), ex=ttl)
        
        # Track token for user (for listing/revocation)
        user_key = f"zfs:user:tokens:{user_id}"
        await self.redis.sadd(user_key, token_id)
        await self.redis.expire(user_key, ttl)
        
        # Update stats
        await self.redis.execute('hincrby', f"zfs:stats:tokens_created", operation, 1)
        await self.redis.execute('hincrby', f"zfs:stats:users", user_id, 1)
        
        return {
            'token': token_id,
            'token_id': token_id,
            'operation': operation,
            'dataset': dataset,
            'snapshot': snapshot,
            'expires_in': ttl,
            'created_at': token_data['created_at']
        }
    
    async def _create_auth_token(self, username: str, ttl: int) -> str:
        """Create authentication token (original implementation)"""
        # Generate a secure random token
        token = secrets.token_urlsafe(32)
        
        # Create token data
        token_data = {
            'username': username,
            'created_at': int(time.time()),
            'ttl': ttl
        }
        
        # Store token in Redis
        key = f"{self.token_prefix}{token}"
        await self.redis.set(key, json.dumps(token_data), ex=ttl)
        
        # Add token to user's token set
        user_key = f"{self.user_tokens_prefix}{username}"
        await self.redis.sadd(user_key, token)
        await self.redis.expire(user_key, ttl)
        
        # Update stats
        await self.redis.execute('hincrby', f"{self.stats_prefix}tokens_created", username, 1)
        await self.redis.execute('hincrby', f"{self.stats_prefix}tokens_created_total", 'count', 1)
        
        return token
    
    async def verify_zfs_token(self, token_id: str, operation: str = None) -> Optional[Dict[str, Any]]:
        """Verify a ZFS operation token and return token data if valid."""
        key = f"zfs:token:{token_id}"
        data = await self.redis.get(key)
        
        if not data:
            return None
        
        try:
            token_data = json.loads(data)
            
            # Check if operation matches (if specified)
            if operation and token_data.get('operation') != operation:
                return None
            
            # Check if expired
            expires_at = datetime.fromisoformat(token_data['expires_at'])
            if datetime.now(timezone.utc) > expires_at:
                return None
            
            # Verify checksum
            expected_checksum = f"{token_id}:{token_data['operation']}:{token_data['dataset']}:{token_data['user_id']}"
            if hashlib.sha256(expected_checksum.encode()).hexdigest()[:16] != token_data['checksum']:
                return None
            
            # Update usage stats
            await self.redis.execute('hincrby', f"zfs:token:stats:{token_id}", 'verifications', 1)
            
            return token_data
        except (json.JSONDecodeError, KeyError, ValueError):
            return None
    
    async def mark_token_used(self, token_id: str) -> bool:
        """Mark a ZFS token as used."""
        key = f"zfs:token:{token_id}"
        data = await self.redis.get(key)
        
        if not data:
            return False
            
        try:
            token_data = json.loads(data)
            token_data['used'] = True
            token_data['use_count'] = token_data.get('use_count', 0) + 1
            token_data['last_used'] = datetime.now(timezone.utc).isoformat()
            
            # Update in Redis
            ttl = await self.redis.ttl(key)
            if ttl > 0:
                await self.redis.set(key, json.dumps(token_data), ex=ttl)
                
                # Update stats
                await self.redis.execute('hincrby', f"zfs:token:stats:{token_id}", 'uses', 1)
                return True
        except:
            pass
            
        return False
    
    async def list_zfs_tokens(self, user_id: str = None, operation: str = None) -> List[Dict[str, Any]]:
        """List ZFS tokens, optionally filtered by user or operation."""
        tokens = []
        
        if user_id:
            # Get tokens for specific user
            user_key = f"zfs:user:tokens:{user_id}"
            token_ids = await self.redis.smembers(user_key)
            keys = [f"zfs:token:{tid.decode() if isinstance(tid, bytes) else tid}" for tid in token_ids]
        else:
            # Get all ZFS tokens
            keys = await self.redis.keys('zfs:token:*')
        
        for key in keys:
            if isinstance(key, bytes):
                key = key.decode()
                
            data = await self.redis.get(key)
            if data:
                try:
                    token_data = json.loads(data)
                    
                    # Filter by operation if specified
                    if operation and token_data.get('operation') != operation:
                        continue
                        
                    # Add TTL info
                    ttl = await self.redis.ttl(key)
                    token_data['expires_in'] = ttl
                    
                    tokens.append(token_data)
                except:
                    pass
                    
        return tokens
    
    async def revoke_zfs_token(self, token_id: str) -> bool:
        """Revoke a ZFS token."""
        key = f"zfs:token:{token_id}"
        data = await self.redis.get(key)
        
        if not data:
            return False
            
        try:
            token_data = json.loads(data)
            user_id = token_data.get('user_id')
            
            # Delete the token
            await self.redis.delete(key)
            
            # Remove from user's token set
            if user_id:
                user_key = f"zfs:user:tokens:{user_id}"
                await self.redis.srem(user_key, token_id)
                
            # Update stats
            await self.redis.execute('hincrby', f"zfs:stats:tokens_revoked", token_data.get('operation', 'unknown'), 1)
            
            # Delete token stats
            await self.redis.delete(f"zfs:token:stats:{token_id}")
            
            return True
        except:
            return False

    async def verify_token(self, token: str) -> Optional[str]:
        """Verify a token and return the username if valid.
        
        Args:
            token: The token to verify
            
        Returns:
            Username if token is valid, None otherwise
        """
        key = f"{self.token_prefix}{token}"
        data = await self.redis.get(key)
        
        if not data:
            await self.redis.execute('hincrby', f"{self.stats_prefix}token_verifications_failed", 'count', 1)
            return None
        
        try:
            token_data = json.loads(data)
            username = token_data.get('username')
            
            # Update token stats
            stats_key = f"{self.token_stats_prefix}{token}"
            await self.redis.execute('hincrby', stats_key, 'verifications', 1)
            await self.redis.execute('hincrby', stats_key, 'last_verified', int(time.time()))
            
            # Update global stats
            await self.redis.execute('hincrby', f"{self.stats_prefix}token_verifications_success", username, 1)
            
            return username
        except (json.JSONDecodeError, KeyError):
            await self.redis.execute('hincrby', f"{self.stats_prefix}token_verifications_failed", 'count', 1)
            return None
    
    async def revoke_token(self, token: str) -> bool:
        """Revoke a token.
        
        Args:
            token: The token to revoke
            
        Returns:
            True if token was revoked, False if token didn't exist
        """
        key = f"{self.token_prefix}{token}"
        
        # Get token data first to find the username
        data = await self.redis.get(key)
        if not data:
            return False
        
        try:
            token_data = json.loads(data)
            username = token_data.get('username')
            
            # Delete the token
            await self.redis.delete(key)
            
            # Remove from user's token set
            if username:
                user_key = f"{self.user_tokens_prefix}{username}"
                await self.redis.srem(user_key, token)
                
                # Update stats
                await self.redis.execute('hincrby', f"{self.stats_prefix}tokens_revoked", username, 1)
            
            # Delete token stats
            stats_key = f"{self.token_stats_prefix}{token}"
            await self.redis.delete(stats_key)
            
            return True
        except (json.JSONDecodeError, KeyError):
            return False
    
    async def list_user_tokens(self, username: str) -> List[str]:
        """List all active tokens for a user.
        
        Args:
            username: The username to list tokens for
            
        Returns:
            List of active token strings
        """
        user_key = f"{self.user_tokens_prefix}{username}"
        tokens = await self.redis.smembers(user_key)
        
        if not tokens:
            return []
        
        # Filter out expired tokens
        active_tokens = []
        for token in tokens:
            if isinstance(token, bytes):
                token = token.decode('utf-8')
            
            key = f"{self.token_prefix}{token}"
            if await self.redis.exists(key):
                active_tokens.append(token)
            else:
                # Clean up expired token from set
                await self.redis.srem(user_key, token)
        
        return active_tokens
    
    async def get_token_info(self, token: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a token.
        
        Args:
            token: The token to get info for
            
        Returns:
            Token information dict or None if token doesn't exist
        """
        key = f"{self.token_prefix}{token}"
        data = await self.redis.get(key)
        
        if not data:
            return None
        
        try:
            token_data = json.loads(data)
            
            # Get TTL
            ttl = await self.redis.ttl(key)
            token_data['expires_in'] = ttl
            
            # Get stats
            stats_key = f"{self.token_stats_prefix}{token}"
            stats = await self.redis.hgetall(stats_key)
            
            if stats:
                # Convert bytes keys/values to strings if needed
                token_data['stats'] = {
                    k.decode() if isinstance(k, bytes) else k: 
                    v.decode() if isinstance(v, bytes) else v 
                    for k, v in stats.items()
                }
            
            return token_data
        except (json.JSONDecodeError, KeyError):
            return None
    
    async def cleanup_expired_tokens(self, username: str) -> int:
        """Clean up expired tokens for a user.
        
        Args:
            username: The username to clean up tokens for
            
        Returns:
            Number of expired tokens removed
        """
        user_key = f"{self.user_tokens_prefix}{username}"
        tokens = await self.redis.smembers(user_key)
        
        if not tokens:
            return 0
        
        removed = 0
        for token in tokens:
            if isinstance(token, bytes):
                token = token.decode('utf-8')
                
            key = f"{self.token_prefix}{token}"
            if not await self.redis.exists(key):
                await self.redis.srem(user_key, token)
                removed += 1
        
        return removed
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get authentication statistics.
        
        Returns:
            Dictionary of statistics
        """
        stats = {}
        
        # Get all stats keys
        stat_keys = [
            f"{self.stats_prefix}tokens_created",
            f"{self.stats_prefix}tokens_created_total",
            f"{self.stats_prefix}token_verifications_success",
            f"{self.stats_prefix}token_verifications_failed",
            f"{self.stats_prefix}tokens_revoked"
        ]
        
        for key in stat_keys:
            data = await self.redis.hgetall(key)
            if data:
                # Convert bytes to strings and values to ints
                stats[key.split(':')[-1]] = {
                    k.decode() if isinstance(k, bytes) else k: 
                    int(v) if isinstance(v, (bytes, str)) else v 
                    for k, v in data.items()
                }
        
        # Add Redis connection stats
        stats['redis_connection'] = self.redis.get_stats()
        
        return stats
    
    async def create_api_key(self, name: str, permissions: List[str], 
                            ttl: Optional[int] = None) -> str:
        """Create an API key with specific permissions.
        
        Args:
            name: Name/description for the API key
            permissions: List of permission strings
            ttl: Optional TTL in seconds (None for no expiration)
            
        Returns:
            The generated API key
        """
        # Generate API key
        api_key = f"sk_{secrets.token_urlsafe(32)}"
        
        # Create key data
        key_data = {
            'name': name,
            'permissions': permissions,
            'created_at': int(time.time()),
            'type': 'api_key'
        }
        
        # Store in Redis
        key = f"auth:apikey:{api_key}"
        if ttl:
            await self.redis.set(key, json.dumps(key_data), ex=ttl)
        else:
            await self.redis.set(key, json.dumps(key_data))
        
        # Update stats
        await self.redis.execute('hincrby', f"{self.stats_prefix}api_keys_created", 'count', 1)
        
        return api_key
    
    async def verify_api_key(self, api_key: str) -> Optional[Dict[str, Any]]:
        """Verify an API key and return its data.
        
        Args:
            api_key: The API key to verify
            
        Returns:
            API key data dict or None if invalid
        """
        key = f"auth:apikey:{api_key}"
        data = await self.redis.get(key)
        
        if not data:
            return None
        
        try:
            key_data = json.loads(data)
            
            # Update usage stats
            await self.redis.execute('hincrby', f"auth:apikey:stats:{api_key}", 'uses', 1)
            await self.redis.execute('hincrby', f"auth:apikey:stats:{api_key}", 
                                   'last_used', int(time.time()))
            
            return key_data
        except json.JSONDecodeError:
            return None
    
    async def revoke_api_key(self, api_key: str) -> bool:
        """Revoke an API key.
        
        Args:
            api_key: The API key to revoke
            
        Returns:
            True if revoked, False if didn't exist
        """
        key = f"auth:apikey:{api_key}"
        stats_key = f"auth:apikey:stats:{api_key}"
        
        result = await self.redis.delete(key, stats_key)
        
        if result:
            await self.redis.execute('hincrby', f"{self.stats_prefix}api_keys_revoked", 'count', 1)
            return True
        
        return False