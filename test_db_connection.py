"""
Database connection diagnostic script.
Run this to test and diagnose database connectivity issues.
"""
import os
import sys
import socket
from urllib.parse import urlparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_dns_resolution(hostname):
    """Test if hostname can be resolved."""
    try:
        ip = socket.gethostbyname(hostname)
        print(f"✓ DNS resolution successful: {hostname} -> {ip}")
        return True, ip
    except socket.gaierror as e:
        print(f"✗ DNS resolution failed: {hostname}")
        print(f"  Error: {e}")
        return False, None

def test_port_connectivity(hostname, port, timeout=5):
    """Test if port is accessible."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((hostname, port))
        sock.close()
        if result == 0:
            print(f"✓ Port {port} is accessible on {hostname}")
            return True
        else:
            print(f"✗ Port {port} is not accessible on {hostname}")
            return False
    except Exception as e:
        print(f"✗ Port connectivity test failed: {e}")
        return False

def parse_database_url(url):
    """Parse database URL and extract components."""
    try:
        parsed = urlparse(url)
        return {
            'hostname': parsed.hostname,
            'port': parsed.port or 5432,
            'database': parsed.path.lstrip('/'),
            'username': parsed.username,
            'password': '***' if parsed.password else None,
            'scheme': parsed.scheme
        }
    except Exception as e:
        print(f"✗ Failed to parse DATABASE_URL: {e}")
        return None

def test_psycopg2_connection(url):
    """Test actual psycopg2 connection."""
    try:
        import psycopg2
        conn = psycopg2.connect(url, connect_timeout=10)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        print(f"✓ PostgreSQL connection successful!")
        print(f"  Database version: {version[:50]}...")
        return True
    except psycopg2.OperationalError as e:
        print(f"✗ PostgreSQL connection failed:")
        print(f"  Error: {e}")
        return False
    except ImportError:
        print("✗ psycopg2 not installed. Install it with: pip install psycopg2-binary")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False

def main():
    print("=" * 60)
    print("Database Connection Diagnostic Tool")
    print("=" * 60)
    print()
    
    # Get DATABASE_URL
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("✗ DATABASE_URL environment variable not found!")
        print("  Please set it in your .env file or environment.")
        return 1
    
    print(f"✓ DATABASE_URL found")
    print(f"  URL: {db_url[:50]}..." if len(db_url) > 50 else f"  URL: {db_url}")
    print()
    
    # Parse URL
    components = parse_database_url(db_url)
    if not components:
        return 1
    
    print("Parsed connection details:")
    print(f"  Hostname: {components['hostname']}")
    print(f"  Port: {components['port']}")
    print(f"  Database: {components['database']}")
    print(f"  Username: {components['username']}")
    print(f"  Scheme: {components['scheme']}")
    print()
    
    # Test DNS resolution
    print("Step 1: Testing DNS resolution...")
    dns_ok, ip = test_dns_resolution(components['hostname'])
    print()
    
    if not dns_ok:
        print("=" * 60)
        print("DIAGNOSIS: DNS resolution failed")
        print("=" * 60)
        print()
        print("Possible solutions:")
        print("1. Check your internet connection")
        print("2. Verify DNS settings:")
        print("   - Try using a different DNS server (e.g., 8.8.8.8)")
        print("   - Check if DNS is blocked by firewall")
        print("3. Check if VPN is required:")
        print("   - Some databases require VPN connection")
        print("   - Connect to VPN and try again")
        print("4. Verify the hostname is correct:")
        print(f"   - Current hostname: {components['hostname']}")
        print("   - Check with your database administrator")
        print("5. Try pinging the hostname:")
        print(f"   ping {components['hostname']}")
        return 1
    
    # Test port connectivity
    print("Step 2: Testing port connectivity...")
    port_ok = test_port_connectivity(components['hostname'], components['port'])
    print()
    
    if not port_ok:
        print("=" * 60)
        print("DIAGNOSIS: Port is not accessible")
        print("=" * 60)
        print()
        print("Possible solutions:")
        print("1. Check if database server is running")
        print("2. Verify firewall settings:")
        print("   - Check if port is blocked")
        print("   - Check if IP is whitelisted")
        print("3. Check if VPN is required")
        print("4. Verify port number is correct")
        return 1
    
    # Test actual PostgreSQL connection
    print("Step 3: Testing PostgreSQL connection...")
    conn_ok = test_psycopg2_connection(db_url)
    print()
    
    if conn_ok:
        print("=" * 60)
        print("SUCCESS: All tests passed!")
        print("=" * 60)
        return 0
    else:
        print("=" * 60)
        print("DIAGNOSIS: PostgreSQL connection failed")
        print("=" * 60)
        print()
        print("Possible solutions:")
        print("1. Verify database credentials:")
        print("   - Check username and password")
        print("   - Ensure user has proper permissions")
        print("2. Check database name is correct")
        print("3. Verify SSL settings if required")
        print("4. Check database server logs for errors")
        return 1

if __name__ == "__main__":
    sys.exit(main())

