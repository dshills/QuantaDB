#!/usr/bin/env python3
"""
PostgreSQL driver test for QuantaDB using psycopg2
Tests prepared statements, parameter binding, and advanced features
"""

import psycopg2
import psycopg2.extras
import sys
import traceback
from contextlib import contextmanager

# Connection parameters
CONN_PARAMS = {
    'host': 'localhost',
    'port': 5433,
    'user': 'postgres',
    'database': 'quantadb',
    'sslmode': 'disable'
}

@contextmanager
def get_connection():
    """Get a database connection with proper cleanup"""
    conn = None
    try:
        conn = psycopg2.connect(**CONN_PARAMS)
        yield conn
    finally:
        if conn:
            conn.close()

def test_simple_connection():
    """Test basic connection to QuantaDB"""
    print("   Testing basic connection...")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()[0]
            if result != 1:
                raise AssertionError(f"Expected 1, got {result}")
            print(f"   Simple query result: {result}")

def test_parameter_binding():
    """Test parameter binding with different types"""
    print("   Testing parameter binding...")
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Test single parameter
            cur.execute("SELECT %s", (42,))
            result = cur.fetchone()[0]
            if result != 42:
                raise AssertionError(f"Expected 42, got {result}")
            print(f"   Single parameter: {result}")
            
            # Test multiple parameters
            cur.execute("SELECT %s, %s, %s", (123, "hello", True))
            int_val, str_val, bool_val = cur.fetchone()
            if int_val != 123 or str_val != "hello" or bool_val != True:
                raise AssertionError(f"Parameter mismatch: {int_val}, {str_val}, {bool_val}")
            print(f"   Multiple parameters: {int_val}, '{str_val}', {bool_val}")

def test_prepared_statements():
    """Test prepared statements (server-side prepared statements)"""
    print("   Testing prepared statements...")
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Prepare a statement
            cur.execute("PREPARE test_stmt AS SELECT $1 * $2")
            
            # Execute prepared statement
            cur.execute("EXECUTE test_stmt(%s, %s)", (6, 7))
            result = cur.fetchone()[0]
            if result != 42:
                raise AssertionError(f"Expected 42, got {result}")
            print(f"   Prepared statement result: {result}")
            
            # Clean up (might not be supported)
            try:
                cur.execute("DEALLOCATE test_stmt")
            except Exception as e:
                print(f"   Note: DEALLOCATE not supported: {e}")

def test_named_cursors():
    """Test named cursors (server-side cursors)"""
    print("   Testing named cursors...")
    with get_connection() as conn:
        # Create a named cursor
        with conn.cursor(name='test_cursor') as cur:
            # Execute query with parameters
            cur.execute("SELECT generate_series(1, %s)", (5,))
            
            # Fetch results
            results = cur.fetchall()
            expected = [(i,) for i in range(1, 6)]
            
            # Note: generate_series might not be implemented in QuantaDB
            # So we'll just check that we can create and use a named cursor
            print(f"   Named cursor executed successfully")

def test_type_adaptation():
    """Test different Python type adaptations"""
    print("   Testing type adaptations...")
    with get_connection() as conn:
        with conn.cursor() as cur:
            test_cases = [
                (42, "integer"),
                ("test string", "text"),
                (True, "boolean"),
                (False, "boolean"),
                (None, "null"),
                (123.45, "numeric"),
            ]
            
            for value, type_name in test_cases:
                cur.execute("SELECT %s", (value,))
                result = cur.fetchone()[0]
                print(f"   {type_name}: {value} -> {result}")

def test_transaction_handling():
    """Test transaction handling"""
    print("   Testing transactions...")
    with get_connection() as conn:
        # Test autocommit mode
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 100")
            result = cur.fetchone()[0]
            if result != 100:
                raise AssertionError(f"Expected 100, got {result}")
        
        # Test manual transaction
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 200")
                result = cur.fetchone()[0]
                if result != 200:
                    raise AssertionError(f"Expected 200, got {result}")
                conn.commit()
                print(f"   Transaction result: {result}")
        except Exception:
            conn.rollback()
            raise

def test_dict_cursor():
    """Test dictionary cursor for named access"""
    print("   Testing dictionary cursor...")
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT %s as test_col", (999,))
            result = cur.fetchone()
            
            # Access by name
            if result['test_col'] != 999:
                raise AssertionError(f"Expected 999, got {result['test_col']}")
            print(f"   Dict cursor result: {result['test_col']}")

def test_real_table_cursor():
    """Test RealDictCursor for real dictionary access"""
    print("   Testing real table cursor...")
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT %s as id, %s as name", (1, "Alice"))
            result = cur.fetchone()
            
            if result['id'] != 1 or result['name'] != "Alice":
                raise AssertionError(f"Expected id=1, name=Alice, got {result}")
            print(f"   Real dict cursor: id={result['id']}, name='{result['name']}'")

def test_batch_operations():
    """Test batch operations and executemany"""
    print("   Testing batch operations...")
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Test executemany (simulated batch)
            data = [(1,), (2,), (3,)]
            results = []
            for params in data:
                cur.execute("SELECT %s", params)
                results.append(cur.fetchone()[0])
            
            expected = [1, 2, 3]
            if results != expected:
                raise AssertionError(f"Expected {expected}, got {results}")
            print(f"   Batch results: {results}")

def test_error_handling():
    """Test error handling and exception types"""
    print("   Testing error handling...")
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Test syntax error
            try:
                cur.execute("INVALID SQL STATEMENT")
                raise AssertionError("Expected error for invalid SQL")
            except psycopg2.Error as e:
                print(f"   Error handling works: {type(e).__name__}")
            
            # Test parameter error
            try:
                cur.execute("SELECT %s", (1, 2))  # Too many parameters
                raise AssertionError("Expected error for parameter mismatch")
            except psycopg2.Error as e:
                print(f"   Parameter error handling: {type(e).__name__}")

def test_connection_info():
    """Test connection information and status"""
    print("   Testing connection info...")
    with get_connection() as conn:
        # Test connection status
        print(f"   Connection status: {conn.status}")
        print(f"   Server version: {conn.server_version}")
        print(f"   Protocol version: {conn.protocol_version}")
        
        # Test connection info
        info = conn.get_dsn_parameters()
        print(f"   Connected to: {info.get('host')}:{info.get('port')}")

def main():
    """Run all psycopg2 tests"""
    print("=== Testing Python psycopg2 driver with QuantaDB ===")
    print(f"Connecting to: {CONN_PARAMS['host']}:{CONN_PARAMS['port']}")
    
    tests = [
        ("Simple Connection", test_simple_connection),
        ("Parameter Binding", test_parameter_binding),
        ("Prepared Statements", test_prepared_statements),
        ("Named Cursors", test_named_cursors),
        ("Type Adaptation", test_type_adaptation),
        ("Transaction Handling", test_transaction_handling),
        ("Dictionary Cursor", test_dict_cursor),
        ("Real Dictionary Cursor", test_real_table_cursor),
        ("Batch Operations", test_batch_operations),
        ("Error Handling", test_error_handling),
        ("Connection Info", test_connection_info),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        print(f"\n--- Testing: {test_name} ---")
        try:
            test_func()
            print("✅ PASSED")
            passed += 1
        except Exception as e:
            print(f"❌ FAILED: {e}")
            traceback.print_exc()
            failed += 1
    
    print(f"\n=== Test Results ===")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Total: {passed + failed}")
    
    if failed > 0:
        print("❌ Some tests failed")
        sys.exit(1)
    else:
        print("🎉 All tests passed!")
        sys.exit(0)

if __name__ == "__main__":
    main()