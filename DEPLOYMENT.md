# ZFS API Server Deployment Guide

Since ZFS operations require root privileges, here are several deployment strategies:

## Option 1: Run as Root (Not Recommended for Production)

The simplest but least secure option:

```bash
sudo ./start.sh
```

**Pros:**
- Simple to set up
- No additional configuration

**Cons:**
- Security risk - entire API server runs as root
- Not recommended for production

## Option 2: Using sudo with NOPASSWD (Recommended)

Configure sudo to allow the API server user to run only ZFS commands without password:

### Setup Steps:

1. Create a dedicated user for the API:
```bash
sudo useradd -r -s /bin/false zfsapi
sudo usermod -a -G zfsapi $USER  # Add yourself to the group
```

2. Create sudoers file:
```bash
sudo visudo -f /etc/sudoers.d/zfsapi
```

Add these lines:
```
# Allow zfsapi user to run ZFS commands without password
zfsapi ALL=(ALL) NOPASSWD: /sbin/zfs, /sbin/zpool, /sbin/zdb
%zfsapi ALL=(ALL) NOPASSWD: /sbin/zfs, /sbin/zpool, /sbin/zdb

# Restrict to specific commands if you want more security:
# zfsapi ALL=(ALL) NOPASSWD: /sbin/zfs list*, /sbin/zfs get*, /sbin/zfs set*, /sbin/zfs create*, /sbin/zfs destroy*, /sbin/zfs snapshot*, /sbin/zfs mount*, /sbin/zfs rollback*, /sbin/zfs clone*, /sbin/zfs send*, /sbin/zfs receive*
# zfsapi ALL=(ALL) NOPASSWD: /sbin/zpool list*, /sbin/zpool get*, /sbin/zpool set*, /sbin/zpool status*, /sbin/zpool scrub*
```

3. Modify the execute function to use sudo:
```bash
cat > execute_wrapper.py << 'EOF'
#!/usr/bin/env python3
import subprocess

def execute(args):
    """Execute commands with sudo if needed"""
    # Commands that need sudo
    sudo_commands = ['zfs', 'zpool', 'zdb']
    
    # Check if command needs sudo
    if args[0] in sudo_commands:
        # Check if we're already root
        import os
        if os.geteuid() != 0:
            args = ['sudo', '-n'] + args
    
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, err = p.communicate()
    rc = p.returncode
    return output.decode('utf-8'), err.decode('utf-8'), rc
EOF
```

## Option 3: Systemd Service with Capabilities

Create a systemd service that runs with minimal required capabilities:

```bash
sudo cat > /etc/systemd/system/zfs-api.service << 'EOF'
[Unit]
Description=ZFS API Server
After=network.target zfs.target

[Service]
Type=simple
User=zfsapi
Group=zfsapi
WorkingDirectory=/home/stein/python/zfs-api
Environment="PATH=/home/stein/python/zfs-api/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ExecStart=/home/stein/python/zfs-api/venv/bin/python /home/stein/python/zfs-api/zfs_api_server.py

# Security settings
PrivateTmp=true
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/log

# Required for ZFS operations
AmbientCapabilities=CAP_SYS_ADMIN
CapabilityBoundingSet=CAP_SYS_ADMIN

Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable zfs-api
sudo systemctl start zfs-api
```

## Option 4: Docker Container with Privileges

Create a Docker deployment:

```dockerfile
# Dockerfile
FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv zfsutils-linux && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app/

RUN python3 -m venv venv && \
    . venv/bin/activate && \
    pip install -r requirements.txt

EXPOSE 8545

CMD ["venv/bin/python", "zfs_api_server.py"]
```

Run with privileges:
```bash
docker build -t zfs-api .
docker run -d \
  --name zfs-api \
  --privileged \
  -v /dev:/dev \
  -v /proc:/proc:ro \
  -p 8545:8545 \
  zfs-api
```

## Option 5: Using Polkit (Modern Systems)

For systems using Polkit, create a policy:

```bash
sudo cat > /usr/share/polkit-1/actions/org.zfs.api.policy << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE policyconfig PUBLIC
 "-//freedesktop//DTD PolicyKit Policy Configuration 1.0//EN"
 "http://www.freedesktop.org/standards/PolicyKit/1.0/policyconfig.dtd">
<policyconfig>
  <action id="org.zfs.api.manage">
    <description>Manage ZFS filesystems</description>
    <message>Authentication is required to manage ZFS</message>
    <defaults>
      <allow_any>no</allow_any>
      <allow_inactive>no</allow_inactive>
      <allow_active>auth_admin</allow_active>
    </defaults>
    <annotate key="org.freedesktop.policykit.exec.path">/sbin/zfs</annotate>
  </action>
</policyconfig>
EOF
```

## Option 6: Capability-Aware Wrapper

Create a small setuid wrapper in C:

```c
// zfs-wrapper.c
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/capability.h>

int main(int argc, char *argv[]) {
    // Drop all capabilities except CAP_SYS_ADMIN
    cap_t caps = cap_init();
    cap_value_t cap_list[1] = {CAP_SYS_ADMIN};
    cap_set_flag(caps, CAP_PERMITTED, 1, cap_list, CAP_SET);
    cap_set_flag(caps, CAP_EFFECTIVE, 1, cap_list, CAP_SET);
    cap_set_proc(caps);
    cap_free(caps);
    
    // Execute ZFS command
    execvp("/sbin/zfs", argv);
    perror("execvp");
    return 1;
}
```

Compile and install:
```bash
gcc -o zfs-wrapper zfs-wrapper.c -lcap
sudo chown root:zfsapi zfs-wrapper
sudo chmod 4750 zfs-wrapper
```

## Recommended Production Setup

For production, we recommend **Option 2 (sudo with NOPASSWD)** combined with:

1. **Reverse Proxy** (nginx/Apache) for SSL termination
2. **Firewall** rules to restrict access
3. **SELinux/AppArmor** profiles for additional security
4. **Resource limits** in systemd
5. **Audit logging** for all ZFS operations

### Complete Production Setup Script:

```bash
#!/bin/bash
# production-setup.sh

# Create user
sudo useradd -r -m -s /bin/bash zfsapi

# Setup sudoers
sudo tee /etc/sudoers.d/zfsapi << 'EOF'
zfsapi ALL=(ALL) NOPASSWD: /sbin/zfs, /sbin/zpool, /sbin/zdb
EOF

# Copy application
sudo cp -r /home/stein/python/zfs-api /opt/zfs-api
sudo chown -R zfsapi:zfsapi /opt/zfs-api

# Setup as systemd service
sudo tee /etc/systemd/system/zfs-api.service << 'EOF'
[Unit]
Description=ZFS API Server
After=network.target

[Service]
Type=simple
User=zfsapi
Group=zfsapi
WorkingDirectory=/opt/zfs-api
ExecStart=/opt/zfs-api/venv/bin/python /opt/zfs-api/zfs_api_server.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Setup nginx reverse proxy
sudo tee /etc/nginx/sites-available/zfs-api << 'EOF'
server {
    listen 443 ssl http2;
    server_name zfs-api.example.com;
    
    ssl_certificate /etc/ssl/certs/zfs-api.crt;
    ssl_certificate_key /etc/ssl/private/zfs-api.key;
    
    location / {
        proxy_pass http://localhost:8545;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
EOF

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable zfs-api
sudo systemctl start zfs-api
```

## Security Considerations

1. **Limit ZFS commands**: Only allow necessary ZFS subcommands in sudoers
2. **Network isolation**: Bind to localhost and use reverse proxy
3. **API rate limiting**: Already implemented in the server
4. **Audit logging**: Log all ZFS operations
5. **Input validation**: Validate all dataset names and parameters
6. **Least privilege**: Only grant minimum required permissions

## Monitoring

1. Check service status:
```bash
sudo systemctl status zfs-api
```

2. View logs:
```bash
sudo journalctl -u zfs-api -f
```

3. Monitor metrics:
```bash
curl http://localhost:8545/metrics
```