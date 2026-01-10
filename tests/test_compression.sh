#!/bin/bash
# Test ZFS compression options

DATASET="syspool/compression_test"
TESTFILE="/syspool/compression_test/bigfile.dat"

echo "=== ZFS Compression Test ==="
echo

# Create test dataset with compression
echo "1. Creating test dataset with lz4 compression..."
zfs create -o compression=lz4 $DATASET

# Create test data (100MB of compressible data)
echo "2. Creating 100MB of compressible test data..."
yes "This is a test line that will be repeated many times to create compressible data" | head -c 100M > $TESTFILE

# Show dataset compression ratio
echo "3. Dataset compression ratio:"
zfs get compression,compressratio,used,logicalused $DATASET

# Create snapshot
echo -e "\n4. Creating snapshot..."
zfs snapshot ${DATASET}@test_snap

# Test 1: Regular send
echo -e "\n5. Testing regular send (no -c flag)..."
zfs send ${DATASET}@test_snap > /tmp/regular_send.zfs
REGULAR_SIZE=$(ls -lh /tmp/regular_send.zfs | awk '{print $5}')
echo "   Regular send size: $REGULAR_SIZE"

# Test 2: Compressed send
echo -e "\n6. Testing compressed send (with -c flag)..."
zfs send -c ${DATASET}@test_snap > /tmp/compressed_send.zfs
COMPRESSED_SIZE=$(ls -lh /tmp/compressed_send.zfs | awk '{print $5}')
echo "   Compressed send size: $COMPRESSED_SIZE"

# Test 3: Gzip compressed regular send
echo -e "\n7. Testing gzip compression of regular send..."
gzip -c /tmp/regular_send.zfs > /tmp/regular_send.zfs.gz
GZIP_REGULAR_SIZE=$(ls -lh /tmp/regular_send.zfs.gz | awk '{print $5}')
echo "   Gzipped regular send size: $GZIP_REGULAR_SIZE"

# Test 4: Gzip compressed of compressed send
echo -e "\n8. Testing gzip compression of compressed send..."
gzip -c /tmp/compressed_send.zfs > /tmp/compressed_send.zfs.gz
GZIP_COMPRESSED_SIZE=$(ls -lh /tmp/compressed_send.zfs.gz | awk '{print $5}')
echo "   Gzipped compressed send size: $GZIP_COMPRESSED_SIZE"

# Test 5: zstd compression
echo -e "\n9. Testing zstd compression..."
zstd -c /tmp/regular_send.zfs > /tmp/regular_send.zfs.zst
ZSTD_REGULAR_SIZE=$(ls -lh /tmp/regular_send.zfs.zst | awk '{print $5}')
echo "   Zstd regular send size: $ZSTD_REGULAR_SIZE"

zstd -c /tmp/compressed_send.zfs > /tmp/compressed_send.zfs.zst
ZSTD_COMPRESSED_SIZE=$(ls -lh /tmp/compressed_send.zfs.zst | awk '{print $5}')
echo "   Zstd compressed send size: $ZSTD_COMPRESSED_SIZE"

# Summary
echo -e "\n=== SUMMARY ==="
echo "Original data: 100MB"
echo "Regular send: $REGULAR_SIZE"
echo "Compressed send (-c): $COMPRESSED_SIZE"
echo "Gzipped regular: $GZIP_REGULAR_SIZE"
echo "Gzipped compressed: $GZIP_COMPRESSED_SIZE"
echo "Zstd regular: $ZSTD_REGULAR_SIZE"
echo "Zstd compressed: $ZSTD_COMPRESSED_SIZE"

# Test receive
echo -e "\n=== Testing Receive ==="
echo "10. Testing receive from compressed send..."
zfs receive -F ${DATASET}_recv < /tmp/compressed_send.zfs
if [ $? -eq 0 ]; then
    echo "   ✓ Successfully received from compressed send"
    zfs list ${DATASET}_recv
    zfs destroy -r ${DATASET}_recv
else
    echo "   ✗ Failed to receive from compressed send"
fi

# Cleanup
echo -e "\n11. Cleaning up..."
rm -f /tmp/*.zfs /tmp/*.zfs.gz /tmp/*.zfs.zst
zfs destroy -r $DATASET

echo -e "\nTest complete!"