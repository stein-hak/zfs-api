#!/bin/bash
# Setup script for ZFS API Server

set -e  # Exit on any error

echo "ZFS API Server Setup"
echo "===================="
echo ""

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: Python 3 is not installed"
    echo "   Please install Python 3.8 or higher"
    exit 1
fi

# Get Python version
PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
echo "✓ Found Python $PYTHON_VERSION"

# Check if venv module is available
if ! python3 -c "import venv" &> /dev/null; then
    echo "❌ Error: Python venv module is not installed"
    echo "   Install it with: sudo apt-get install python3-venv (on Ubuntu/Debian)"
    echo "                   sudo yum install python3-venv (on RHEL/CentOS)"
    exit 1
fi

# Check if virtual environment already exists
if [ -d "venv" ]; then
    echo ""
    echo "⚠️  Virtual environment already exists!"
    read -p "Do you want to recreate it? (y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Removing existing virtual environment..."
        rm -rf venv
    else
        echo "Using existing virtual environment..."
    fi
fi

# Create virtual environment
if [ ! -d "venv" ]; then
    echo ""
    echo "Creating virtual environment..."
    python3 -m venv venv
    echo "✓ Virtual environment created"
fi

# Activate virtual environment
echo ""
echo "Activating virtual environment..."
source venv/bin/activate

# Verify activation
if [[ "$VIRTUAL_ENV" != "" ]]; then
    echo "✓ Virtual environment activated: $VIRTUAL_ENV"
else
    echo "❌ Failed to activate virtual environment"
    exit 1
fi

# Upgrade pip
echo ""
echo "Upgrading pip..."
pip install --quiet --upgrade pip
echo "✓ pip upgraded to $(pip --version | cut -d' ' -f2)"

# Install requirements
echo ""
echo "Installing requirements..."
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
    echo "✓ All requirements installed"
else
    echo "❌ Error: requirements.txt not found"
    exit 1
fi

# Make scripts executable
chmod +x zfs_api_server.py 2>/dev/null || true
chmod +x test_api.py 2>/dev/null || true

# Create default config if it doesn't exist
if [ ! -f "config.yaml" ]; then
    echo ""
    echo "Creating default configuration..."
    cat > config.yaml << 'EOF'
# ZFS API Server Configuration

server:
  host: 0.0.0.0
  port: 8545
  workers: 20

auth:
  enabled: true
  # Default users (password: admin)
  users:
    admin: "$2b$12$xFSRKpLYk0e.YXdMsVSWiOFjBqYn7ktPqJB3FpDmcVvUeCqQRxXKu"

rate_limit:
  enabled: true
  requests: 100
  window: 60

logging:
  level: INFO
  file: zfs_api.log
EOF
    echo "✓ Default configuration created"
fi

# Create convenience scripts
echo ""
echo "Creating convenience scripts..."

# Create start script
cat > start.sh << 'EOF'
#!/bin/bash
# Start ZFS API Server

# Activate virtual environment
source venv/bin/activate

# Start server
echo "Starting ZFS API Server..."
python zfs_api_server.py
EOF
chmod +x start.sh

# Create test script
cat > run_tests.sh << 'EOF'
#!/bin/bash
# Run API tests

# Activate virtual environment
source venv/bin/activate

# Check if server is running
if ! curl -s http://localhost:8545/health > /dev/null 2>&1; then
    echo "❌ Error: ZFS API server is not running"
    echo "   Start it with: ./start.sh"
    exit 1
fi

# Run tests
echo "Running API tests..."
python test_api.py
EOF
chmod +x run_tests.sh

echo "✓ Convenience scripts created"

# Test import of required modules
echo ""
echo "Verifying installation..."
python -c "
from zfs_commands import AsyncZFS, SyncZFS
import jsonrpcserver
import aiohttp
print('✓ All modules can be imported successfully')
" || {
    echo "❌ Error: Failed to import required modules"
    exit 1
}

# Print summary
echo ""
echo "========================================="
echo "✅ Setup completed successfully!"
echo "========================================="
echo ""
echo "Quick start guide:"
echo ""
echo "1. Start the server:"
echo "   ./start.sh"
echo ""
echo "2. In another terminal, run tests:"
echo "   ./run_tests.sh"
echo ""
echo "3. To use Python directly:"
echo "   source venv/bin/activate"
echo "   python zfs_api_server.py"
echo ""
echo "4. To deactivate virtual environment:"
echo "   deactivate"
echo ""
echo "API will be available at:"
echo "  - http://localhost:8545/ (JSON-RPC endpoint)"
echo "  - http://localhost:8545/health (Health check)"
echo "  - http://localhost:8545/metrics (Prometheus metrics)"
echo ""
echo "Default credentials (for remote access):"
echo "  Username: admin"
echo "  Password: admin"
echo ""
echo "Note: Localhost connections do not require authentication"