import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    END = '\033[0m'
    BOLD = '\033[1m'


def print_step(msg):
    print(f"{Colors.BLUE}{msg}{Colors.END}")


def print_success(msg):
    print(f"{Colors.GREEN}✓ {msg}{Colors.END}")


def print_error(msg):
    print(f"{Colors.RED}✗ {msg}{Colors.END}")


def get_connection():
    config = {
        'user': os.getenv('ECOMMERCE_USER'),
        'password': os.getenv('ECOMMERCE_PASSWORD'),
        'host': os.getenv('ECOMMERCE_HOST'),
        'port': os.getenv('ECOMMERCE_PORT'),
        'database': os.getenv('ECOMMERCE_DATABASE'),
    }
    return psycopg2.connect(**config)


def test_connection():
    print_step("Testing database connection...")
    try:
        conn = get_connection()
        conn.close()
        print_success("Connection successful")
        return True
    except Exception as e:
        print_error(f"Connection failed: {e}")
        return False


def test_tables():
    print_step("Checking tables...")
    
    expected_tables = [
        'customers', 'addresses', 'categories', 
        'products', 'orders', 'order_items', 'reviews'
    ]
    
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public'
            ORDER BY tablename
        """)
        
        existing_tables = [row[0] for row in cur.fetchall()]
        
        all_exist = True
        for table in expected_tables:
            if table in existing_tables:
                print_success(f"Table '{table}' exists")
            else:
                print_error(f"Table '{table}' missing")
                all_exist = False
        
        cur.close()
        conn.close()
        return all_exist
        
    except Exception as e:
        print_error(f"Error checking tables: {e}")
        return False


def test_foreign_keys():
    print_step("Checking foreign key constraints...")
    
    expected_fks = [
        ('addresses', 'addresses_customer_id_fkey'),
        ('products', 'products_category_id_fkey'),
        ('orders', 'orders_customer_id_fkey'),
        ('order_items', 'order_items_order_id_fkey'),
        ('order_items', 'order_items_product_id_fkey'),
        ('reviews', 'reviews_product_id_fkey'),
        ('reviews', 'reviews_customer_id_fkey'),
    ]
    
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        all_exist = True
        for table, constraint in expected_fks:
            cur.execute("""
                SELECT 1 FROM information_schema.table_constraints
                WHERE table_name = %s AND constraint_name = %s
            """, (table, constraint))
            
            if cur.fetchone():
                print_success(f"FK '{constraint}' exists")
            else:
                print_error(f"FK '{constraint}' missing")
                all_exist = False
        
        cur.close()
        conn.close()
        return all_exist
        
    except Exception as e:
        print_error(f"Error checking foreign keys: {e}")
        return False


def test_data_population():
    print_step("Checking data population...")
    
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        tables = ['customers', 'products', 'orders', 'order_items', 'reviews']
        all_populated = True
        
        for table in tables:
            cur.execute(f"SELECT COUNT(*) as count FROM {table}")
            count = cur.fetchone()['count']
            
            if count > 0:
                print_success(f"Table '{table}' has {count} rows")
            else:
                print_error(f"Table '{table}' is empty")
                all_populated = False
        
        cur.close()
        conn.close()
        return all_populated
        
    except Exception as e:
        print_error(f"Error checking data: {e}")
        return False


def test_sample_queries():
    print_step("Testing sample queries...")
    
    queries = [
        ("Simple SELECT", "SELECT COUNT(*) as count FROM customers"),
        ("JOIN query", """
            SELECT COUNT(*) as count 
            FROM orders o 
            JOIN customers c ON o.customer_id = c.customer_id
        """),
        ("Aggregation", """
            SELECT c.category_name, COUNT(p.product_id) as product_count
            FROM categories c
            LEFT JOIN products p ON c.category_id = p.category_id
            GROUP BY c.category_name
            LIMIT 5
        """),
        ("Complex join", """
            SELECT p.product_name, COUNT(DISTINCT o.order_id) as order_count
            FROM products p
            LEFT JOIN order_items oi ON p.product_id = oi.product_id
            LEFT JOIN orders o ON oi.order_id = o.order_id
            GROUP BY p.product_id
            LIMIT 5
        """),
    ]
    
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        all_passed = True
        for name, query in queries:
            try:
                cur.execute(query)
                cur.fetchall()
                print_success(f"Query '{name}' executed successfully")
            except Exception as e:
                print_error(f"Query '{name}' failed: {e}")
                all_passed = False
        
        cur.close()
        conn.close()
        return all_passed
        
    except Exception as e:
        print_error(f"Error running queries: {e}")
        return False


def test_indexes():
    print_step("Checking indexes...")
    
    expected_indexes = [
        'idx_orders_customer_id',
        'idx_orders_order_date',
        'idx_order_items_order_id',
        'idx_products_category_id',
    ]
    
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        all_exist = True
        for index in expected_indexes:
            cur.execute("""
                SELECT 1 FROM pg_indexes
                WHERE indexname = %s
            """, (index,))
            
            if cur.fetchone():
                print_success(f"Index '{index}' exists")
            else:
                print_error(f"Index '{index}' missing")
                all_exist = False
        
        cur.close()
        conn.close()
        return all_exist
        
    except Exception as e:
        print_error(f"Error checking indexes: {e}")
        return False


def main():
    print(f"\n{Colors.BOLD}E-commerce Database Verification{Colors.END}\n")
    
    tests = []
    
    # Run all tests
    tests.append(("Connection", test_connection()))
    
    if not tests[-1][1]:
        print(f"\n{Colors.RED}Cannot continue without database connection{Colors.END}")
        sys.exit(1)
    
    tests.append(("Tables exist", test_tables()))
    tests.append(("Foreign keys", test_foreign_keys()))
    tests.append(("Data populated", test_data_population()))
    tests.append(("Sample queries", test_sample_queries()))
    tests.append(("Indexes", test_indexes()))
    
    # Summary
    print(f"\n{Colors.BOLD}{'=' * 60}{Colors.END}")
    print(f"{Colors.BOLD}Test Summary{Colors.END}")
    print(f"{Colors.BOLD}{'=' * 60}{Colors.END}\n")
    
    passed = sum(1 for _, result in tests if result)
    total = len(tests)
    
    for name, result in tests:
        status = f"{Colors.GREEN}✓ PASS" if result else f"{Colors.RED}✗ FAIL"
        print(f"{status}{Colors.END} - {name}")
    
    print(f"\n{Colors.BOLD}Results: {passed}/{total} tests passed{Colors.END}\n")
    
    if passed == total:
        print(f"{Colors.GREEN}{Colors.BOLD}✓ All tests passed! Database is ready.{Colors.END}")
        print(f"\n{Colors.BOLD}Next steps:{Colors.END}")
        print("  1. Build knowledge graph: python scripts/build_kg.py")
        print("  2. Test SQL queries with natural language\n")
        sys.exit(0)
    else:
        print(f"{Colors.RED}{Colors.BOLD}✗ Some tests failed.{Colors.END}\n")
        sys.exit(1)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{Colors.BOLD}Verification cancelled{Colors.END}")
        sys.exit(1)
    except Exception as e:
        print_error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)