#!/usr/bin/env python3
"""
Generate bcrypt password hash for ZFS API Server configuration.

Usage:
    python3 generate_password_hash.py

This will prompt for a password and output the bcrypt hash
to use in config.yaml under auth.users.admin
"""

import sys
import getpass

try:
    from passlib.hash import bcrypt
except ImportError:
    print("Error: passlib is not installed")
    print("Install it with: pip install passlib")
    sys.exit(1)


def main():
    print("ZFS API Server - Password Hash Generator")
    print("=" * 50)
    print()

    # Get password from user
    while True:
        password = getpass.getpass("Enter password: ")
        if len(password) < 8:
            print("Error: Password must be at least 8 characters long")
            continue

        password_confirm = getpass.getpass("Confirm password: ")

        if password != password_confirm:
            print("Error: Passwords do not match. Try again.")
            print()
            continue

        break

    # Generate hash
    print()
    print("Generating bcrypt hash...")
    password_hash = bcrypt.hash(password)

    print()
    print("=" * 50)
    print("Password hash generated successfully!")
    print("=" * 50)
    print()
    print("Add this to your config.yaml:")
    print()
    print("auth:")
    print("  enabled: true")
    print("  users:")
    print(f'    admin: "{password_hash}"')
    print()
    print("=" * 50)


if __name__ == "__main__":
    main()
