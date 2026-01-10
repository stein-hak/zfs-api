#!/usr/bin/env python3
"""
Token-authenticated socket server handlers for ZFS operations
Uses Redis token manager for secure authentication
"""

import socketserver
import socket
import struct
import json
import subprocess
import logging
import os
from typing import Optional, Dict, Any

# Import token manager
from redis_token_manager import TokenManager

logger = logging.getLogger(__name__)

# Constants
CHUNK_SIZE = 16 * 1024 * 1024  # 16MB chunks
OP_SEND = 1
OP_RECEIVE = 2

class TokenAuthenticatedHandler(socketserver.BaseRequestHandler):
    """Base handler with token authentication"""
    
    # Class-level token manager (shared across all handlers)
    token_manager = None
    
    @classmethod
    def set_token_manager(cls, token_manager: TokenManager):
        """Set the token manager instance"""
        cls.token_manager = token_manager
    
    def authenticate_token(self) -> Optional[Dict[str, Any]]:
        """Read and authenticate token from socket
        
        Returns:
            Token data if valid, None otherwise
        """
        try:
            # Read token length (4 bytes)
            length_data = self.request.recv(4)
            if len(length_data) < 4:
                self.send_error("Invalid token length")
                return None
            
            token_length = struct.unpack("!I", length_data)[0]
            
            # Sanity check
            if token_length > 128:
                self.send_error("Token too long")
                return None
            
            # Read token
            token_id = b""
            while len(token_id) < token_length:
                chunk = self.request.recv(min(token_length - len(token_id), 4096))
                if not chunk:
                    break
                token_id += chunk
            
            if len(token_id) != token_length:
                self.send_error("Failed to read complete token")
                return None
            
            token_id = token_id.decode('utf-8')
            
            # Get client IP
            client_ip = "unknown"
            if hasattr(self, 'client_address') and self.client_address:
                # For TCP sockets, client_address is (ip, port)
                # For Unix sockets, client_address might be empty or a string
                if isinstance(self.client_address, tuple) and len(self.client_address) > 0:
                    client_ip = self.client_address[0]
                elif isinstance(self.client_address, str):
                    client_ip = "unix-socket"
            else:
                # Unix sockets might not have client_address
                if isinstance(self.request, socket.socket) and self.request.family == socket.AF_UNIX:
                    client_ip = "unix-socket"
            
            # Validate token
            token_preview = token_id[:8] if len(token_id) >= 8 else token_id
            logger.info(f"Authenticating token {token_preview}... from {client_ip}")
            token_data = self.token_manager.validate_token(token_id, client_ip)
            
            if not token_data:
                self.send_error("Invalid or expired token")
                return None
            
            # Mark token as used
            self.token_manager.mark_token_used(token_id, client_ip)
            
            logger.info(f"Authenticated {token_data['operation']} for user={token_data['user_id']} dataset={token_data['dataset']}")
            return token_data
            
        except Exception as e:
            logger.exception(f"Authentication error: {e}")
            self.send_error(f"Authentication failed: {str(e)}")
            return None
    
    def send_response(self, data: dict):
        """Send JSON response to client"""
        json_data = json.dumps(data).encode('utf-8')
        
        # Send length header (4 bytes) + data
        self.request.sendall(struct.pack("!I", len(json_data)))
        self.request.sendall(json_data)
    
    def send_error(self, message: str):
        """Send error response"""
        self.send_response({"error": message, "status": "failed"})
    
    def handle(self):
        """Main handler - authenticate and route to operation"""
        try:
            # Authenticate first
            token_data = self.authenticate_token()
            if not token_data:
                return
            
            # Route to appropriate handler based on operation
            if token_data['operation'] == 'send':
                self.handle_send(token_data)
            elif token_data['operation'] == 'receive':
                self.handle_receive(token_data)
            else:
                self.send_error(f"Unknown operation: {token_data['operation']}")
                
        except Exception as e:
            logger.exception(f"Handler error: {e}")
            self.send_error(str(e))
    
    def handle_send(self, token_data: Dict[str, Any]):
        """Handle authenticated send operation"""
        dataset = token_data['dataset']
        snapshot = token_data.get('snapshot')
        params = token_data.get('parameters', {})
        
        if not snapshot:
            self.send_error("Snapshot name required for send")
            return
        
        # Build ZFS send command
        cmd = ['zfs', 'send']
        
        # Apply parameters from token
        if params.get('raw'):
            cmd.append('-w')
        if params.get('compressed'):
            cmd.append('-c')
        # Note: resumable flag is for receive operations, not send
        if params.get('recursive'):
            cmd.append('-R')
        
        # Add incremental if specified
        if params.get('from_snapshot'):
            cmd.extend(['-i', f"{dataset}@{params['from_snapshot']}"])
        
        cmd.append(f"{dataset}@{snapshot}")
        
        logger.info(f"Executing: {' '.join(cmd)}")
        
        # Send success response with operation details
        self.send_response({
            'status': 'started',
            'operation': 'send',
            'dataset': dataset,
            'snapshot': snapshot,
            'command': ' '.join(cmd)
        })
        
        # Execute send operation
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # Stream data to client
            total_bytes = 0
            chunk_count = 0
            
            while True:
                chunk = proc.stdout.read(CHUNK_SIZE)
                if not chunk:
                    break
                
                # Send chunk size (8 bytes) then data
                self.request.sendall(struct.pack("!Q", len(chunk)))
                self.request.sendall(chunk)
                
                total_bytes += len(chunk)
                chunk_count += 1
                
                # Log progress every 1GB
                if total_bytes % (1024 * 1024 * 1024) == 0:
                    logger.info(f"Send progress: {total_bytes / (1024*1024*1024):.1f} GB")
            
            # Send end marker
            self.request.sendall(struct.pack("!Q", 0))
            
            # Wait for process completion
            proc.stdout.close()
            returncode = proc.wait()
            
            if returncode != 0:
                stderr = proc.stderr.read().decode('utf-8', errors='ignore')
                logger.error(f"ZFS send failed with code {returncode}: {stderr}")
                # Send error to client
                error_msg = f"ZFS send failed: {stderr}"
                self.request.sendall(struct.pack("!Q", len(error_msg)))
                self.request.sendall(error_msg.encode('utf-8'))
            else:
                logger.info(f"Send completed: {total_bytes} bytes in {chunk_count} chunks")
                
        except Exception as e:
            logger.exception(f"Error during send: {e}")
    
    def handle_receive(self, token_data: Dict[str, Any]):
        """Handle authenticated receive operation"""
        dataset = token_data['dataset']
        params = token_data.get('parameters', {})
        
        # Build ZFS receive command
        cmd = ['zfs', 'receive']
        
        if params.get('force', True):
            cmd.append('-F')
        if params.get('resumable'):
            cmd.append('-s')
        
        cmd.append(dataset)
        
        logger.info(f"Executing: {' '.join(cmd)}")
        
        # Send success response
        self.send_response({
            'status': 'started',
            'operation': 'receive',
            'dataset': dataset,
            'command': ' '.join(cmd)
        })
        
        # Execute receive operation
        try:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # Read data from client and write to ZFS
            total_bytes = 0
            chunk_count = 0
            
            while True:
                # Read data directly (no chunk protocol for receive)
                data = self.request.recv(CHUNK_SIZE)
                if not data:
                    break  # Socket closed
                
                # Write to ZFS
                try:
                    proc.stdin.write(data)
                    proc.stdin.flush()
                    total_bytes += len(data)
                    chunk_count += 1
                    
                    # Log progress every 1GB
                    if total_bytes % (1024 * 1024 * 1024) < len(data):
                        logger.info(f"Receive progress: {total_bytes / (1024*1024*1024):.1f} GB")
                        
                except BrokenPipeError:
                    # ZFS process died
                    stderr_output = proc.stderr.read() if proc.stderr else b""
                    logger.error(f"ZFS receive died after {total_bytes} bytes: {stderr_output.decode('utf-8', errors='ignore')}")
                    break
            
            # Close stdin and wait for completion
            if proc.stdin:
                try:
                    proc.stdin.close()
                except:
                    pass
            
            # Wait for process completion
            returncode = proc.wait()
            
            # Read any remaining stderr
            stderr_output = b""
            if proc.stderr:
                try:
                    stderr_output = proc.stderr.read()
                except:
                    pass
            
            if returncode != 0:
                logger.error(f"ZFS receive failed with code {returncode}: {stderr_output.decode('utf-8', errors='ignore')}")
            else:
                logger.info(f"Receive completed successfully: {total_bytes} bytes")
                
        except Exception as e:
            logger.exception(f"Error during receive: {e}")

class TokenAuthenticatedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """Multi-threaded TCP server with token authentication"""
    allow_reuse_address = True
    
    def __init__(self, server_address, handler_class, token_manager):
        super().__init__(server_address, handler_class)
        handler_class.set_token_manager(token_manager)
        logger.info(f"Token-authenticated TCP server listening on {server_address}")

class TokenAuthenticatedUnixServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    """Multi-threaded Unix socket server with token authentication"""
    allow_reuse_address = True
    
    def __init__(self, server_address, handler_class, token_manager):
        # Remove existing socket file if it exists
        if os.path.exists(server_address):
            os.unlink(server_address)
        
        super().__init__(server_address, handler_class)
        handler_class.set_token_manager(token_manager)
        logger.info(f"Token-authenticated Unix socket server listening on {server_address}")

# Example standalone server
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Token-authenticated ZFS socket server")
    parser.add_argument('--type', choices=['tcp', 'unix'], default='tcp',
                       help='Server type')
    parser.add_argument('--host', default='0.0.0.0',
                       help='TCP host (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=9999,
                       help='TCP port (default: 9999)')
    parser.add_argument('--unix-socket', default='./zfs_token_socket',
                       help='Unix socket path')
    parser.add_argument('--redis-host', default='192.168.10.152',
                       help='Redis host')
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create token manager
    token_manager = TokenManager(redis_host=args.redis_host)
    
    # Start server
    if args.type == 'tcp':
        server = TokenAuthenticatedTCPServer(
            (args.host, args.port),
            TokenAuthenticatedHandler,
            token_manager
        )
    else:
        server = TokenAuthenticatedUnixServer(
            args.unix_socket,
            TokenAuthenticatedHandler,
            token_manager
        )
    
    try:
        logger.info(f"Starting {args.type} server...")
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        server.shutdown()