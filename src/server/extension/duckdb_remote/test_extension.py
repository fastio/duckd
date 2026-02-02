#!/usr/bin/env python3
"""
Test script for duckdb_remote extension

Usage:
1. Start the DuckDB server:
   ./build_server/duckdb-server --port 9999

2. Run this test script:
   python3 extension/duckdb_remote/test_extension.py
"""

import subprocess
import sys
import os
import time

def main():
    # Paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    build_dir = os.path.join(script_dir, '..', '..', 'build_server')
    extension_path = os.path.join(build_dir, 'extension', 'duckdb_remote', 'duckdb_remote.duckdb_extension')
    duckdb_cli = os.path.join(build_dir, 'duckdb')

    print("=== DuckDB Remote Extension Test ===")
    print(f"Extension path: {extension_path}")

    # Check if extension exists
    if not os.path.exists(extension_path):
        print(f"ERROR: Extension not found at {extension_path}")
        print("Please build the extension first:")
        print("  cd src/server && ./build.sh")
        sys.exit(1)

    print(f"\nExtension file exists: {os.path.getsize(extension_path)} bytes")

    # Test SQL to run
    test_sql = f"""
-- Load the extension
LOAD '{extension_path}';

-- Test ping function (will fail if server not running)
SELECT duckdb_remote_ping('localhost', 9999) AS server_reachable;

-- Test remote_query (requires server running)
SELECT * FROM remote_query('localhost', 9999, 'SELECT 1 + 1 AS result');

-- More complex test
SELECT * FROM remote_query('localhost', 9999, 'SELECT 42 AS answer, ''hello'' AS greeting');
"""

    print("\n=== Test SQL ===")
    print(test_sql)

    print("\n=== Instructions ===")
    print("1. First, start the DuckDB server in another terminal:")
    print(f"   cd {build_dir} && ./duckdb-server --port 9999")
    print("\n2. Then run these SQL commands in DuckDB CLI:")
    print(f"   {duckdb_cli if os.path.exists(duckdb_cli) else 'duckdb'}")
    print("\nOr copy the SQL above.")

    # If DuckDB CLI is available, offer to run interactively
    if os.path.exists(duckdb_cli):
        print(f"\n=== Found DuckDB CLI at {duckdb_cli} ===")
        print("You can run: " + duckdb_cli)

if __name__ == '__main__':
    main()
