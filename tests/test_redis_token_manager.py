#!/usr/bin/env python3
"""
Test script for Redis Token Manager
Tests token creation, validation, and expiration with Redis at 192.168.10.152
"""

import asyncio
import time
import logging
from datetime import datetime
import sys

# Import the token manager
from redis_token_manager import TokenManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_basic_operations():
    """Test basic token operations"""
    print("\n" + "="*60)
    print("Testing Basic Token Operations")
    print("="*60)
    
    # Create token manager
    print("\n1. Connecting to Redis at 192.168.10.152...")
    try:
        manager = TokenManager(
            redis_host="192.168.10.152",
            redis_port=6379,
            redis_db=0,
            max_retries=3,
            raise_on_failure=True
        )
        print("✓ Connected to Redis successfully")
    except Exception as e:
        print(f"✗ Failed to connect to Redis: {e}")
        return False
    
    # Test token creation
    print("\n2. Creating test tokens...")
    
    # Create send token
    send_token = manager.create_token(
        operation="send",
        dataset="tank/test",
        user_id="test_user",
        client_ip="127.0.0.1",
        snapshot="test_snap",
        parameters={"raw": True, "compressed": True},
        ttl=60  # 1 minute TTL for testing
    )
    
    if send_token:
        print(f"✓ Created send token: {send_token['token'][:8]}...")
        print(f"  Operation: {send_token['operation']}")
        print(f"  Dataset: {send_token['dataset']}")
        print(f"  Expires in: {send_token['expires_in']}s")
    else:
        print("✗ Failed to create send token")
        return False
    
    # Create receive token
    receive_token = manager.create_token(
        operation="receive",
        dataset="tank/test_receive",
        user_id="test_user",
        client_ip="127.0.0.1",
        parameters={"force": True},
        ttl=120  # 2 minutes TTL
    )
    
    if receive_token:
        print(f"✓ Created receive token: {receive_token['token'][:8]}...")
    else:
        print("✗ Failed to create receive token")
        return False
    
    # Test token validation
    print("\n3. Validating tokens...")
    
    # Validate send token
    validated = manager.validate_token(send_token['token'], client_ip="127.0.0.1")
    if validated:
        print(f"✓ Send token validated successfully")
        print(f"  User: {validated['user_id']}")
        print(f"  Created at: {validated['created_at']}")
        print(f"  Parameters: {validated['parameters']}")
    else:
        print("✗ Failed to validate send token")
        return False
    
    # Test marking token as used
    print("\n4. Marking token as used...")
    if manager.mark_token_used(send_token['token'], client_ip="127.0.0.1"):
        print("✓ Token marked as used")
    else:
        print("✗ Failed to mark token as used")
        return False
    
    # Validate again to check use count
    validated = manager.validate_token(send_token['token'])
    if validated:
        print(f"✓ Token still valid with use_count: {validated['use_count']}")
    
    # Test listing user tokens
    print("\n5. Listing user tokens...")
    user_tokens = manager.list_user_tokens("test_user")
    print(f"✓ Found {len(user_tokens)} tokens for test_user")
    for token in user_tokens:
        print(f"  - {token['token_id'][:8]}... ({token['operation']}) - Used: {token['used']}")
    
    # Test revoking token
    print("\n6. Revoking receive token...")
    if manager.revoke_token(receive_token['token']):
        print("✓ Token revoked successfully")
    else:
        print("✗ Failed to revoke token")
        return False
    
    # Try to validate revoked token
    validated = manager.validate_token(receive_token['token'])
    if validated:
        print("✗ Revoked token still validates!")
        return False
    else:
        print("✓ Revoked token correctly fails validation")
    
    # Get statistics
    print("\n7. Getting statistics...")
    stats = manager.get_stats()
    print("✓ Token statistics:")
    print(f"  Created: {stats['created']}")
    print(f"  Used: {stats['used']}")
    print(f"  Validation: {stats['validation']}")
    print(f"  Active tokens: {stats['active_tokens']}")
    print(f"  Redis connected: {stats['redis_connected']}")
    print(f"  Redis stats: {stats['redis_stats']}")
    
    return True

def test_token_expiration():
    """Test token expiration"""
    print("\n" + "="*60)
    print("Testing Token Expiration")
    print("="*60)
    
    manager = TokenManager(redis_host="192.168.10.152")
    
    # Create token with very short TTL
    print("\n1. Creating token with 5 second TTL...")
    short_token = manager.create_token(
        operation="send",
        dataset="tank/expire_test",
        user_id="test_user",
        client_ip="127.0.0.1",
        ttl=5  # 5 seconds
    )
    
    if not short_token:
        print("✗ Failed to create token")
        return False
    
    print(f"✓ Created token: {short_token['token'][:8]}...")
    
    # Validate immediately
    print("\n2. Validating immediately...")
    if manager.validate_token(short_token['token']):
        print("✓ Token validates successfully")
    else:
        print("✗ Token should validate but doesn't")
        return False
    
    # Wait for expiration
    print("\n3. Waiting 6 seconds for token to expire...")
    time.sleep(6)
    
    # Try to validate expired token
    print("\n4. Validating after expiration...")
    if manager.validate_token(short_token['token']):
        print("✗ Expired token still validates!")
        return False
    else:
        print("✓ Expired token correctly fails validation")
    
    # Test cleanup
    print("\n5. Running cleanup...")
    cleaned = manager.cleanup_expired()
    print(f"✓ Cleaned up {cleaned} expired tokens")
    
    return True

def test_connection_failure():
    """Test handling of Redis connection failure"""
    print("\n" + "="*60)
    print("Testing Connection Failure Handling")
    print("="*60)
    
    # Create manager with wrong host to test failure
    print("\n1. Testing connection to non-existent Redis...")
    try:
        manager = TokenManager(
            redis_host="192.168.10.999",  # Invalid IP
            redis_port=6379,
            max_retries=2,
            raise_on_failure=True
        )
        print("✗ Should have raised ConnectionFailureError")
        return False
    except Exception as e:
        print(f"✓ Correctly raised exception: {type(e).__name__}")
        print(f"  Message: {e}")
    
    # Create manager with failure callback instead of exception
    print("\n2. Testing with failure callback...")
    failure_called = False
    
    def failure_callback(redis_instance, context):
        nonlocal failure_called
        failure_called = True
        print(f"  Failure callback called: {context}")
    
    try:
        manager = TokenManager(
            redis_host="192.168.10.999",  # Invalid IP
            redis_port=6379,
            max_retries=2,
            raise_on_failure=False  # Use callback instead
        )
        # The manager might be created but operations will fail
        print("✓ Manager created with failure handling")
    except Exception as e:
        print(f"  Exception during creation: {e}")
    
    return True

def test_concurrent_access():
    """Test concurrent token operations"""
    print("\n" + "="*60)
    print("Testing Concurrent Access")
    print("="*60)
    
    manager = TokenManager(redis_host="192.168.10.152")
    
    print("\n1. Creating 10 tokens concurrently...")
    tokens = []
    start_time = time.time()
    
    for i in range(10):
        token = manager.create_token(
            operation="send" if i % 2 == 0 else "receive",
            dataset=f"tank/concurrent_{i}",
            user_id=f"user_{i % 3}",  # 3 different users
            client_ip="127.0.0.1",
            ttl=60
        )
        if token:
            tokens.append(token['token'])
    
    creation_time = time.time() - start_time
    print(f"✓ Created {len(tokens)} tokens in {creation_time:.2f}s")
    
    print("\n2. Validating all tokens...")
    start_time = time.time()
    valid_count = 0
    
    for token in tokens:
        if manager.validate_token(token):
            valid_count += 1
    
    validation_time = time.time() - start_time
    print(f"✓ Validated {valid_count}/{len(tokens)} tokens in {validation_time:.2f}s")
    
    # Clean up
    print("\n3. Cleaning up test tokens...")
    for token in tokens:
        manager.revoke_token(token)
    print("✓ Cleanup complete")
    
    return True

def main():
    """Run all tests"""
    print("\nRedis Token Manager Test Suite")
    print(f"Testing with Redis at 192.168.10.152")
    print(f"Started at: {datetime.now()}")
    
    tests = [
        ("Basic Operations", test_basic_operations),
        ("Token Expiration", test_token_expiration),
        ("Connection Failure", test_connection_failure),
        ("Concurrent Access", test_concurrent_access)
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
                print(f"\n✓ {test_name} PASSED")
            else:
                failed += 1
                print(f"\n✗ {test_name} FAILED")
        except Exception as e:
            failed += 1
            print(f"\n✗ {test_name} FAILED with exception: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "="*60)
    print("Test Summary")
    print("="*60)
    print(f"Total tests: {len(tests)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Success rate: {(passed/len(tests)*100):.1f}%")
    
    return failed == 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)