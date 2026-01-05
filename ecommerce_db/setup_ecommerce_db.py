import os
import sys
from datetime import datetime, timedelta
import random
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv()


class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    END = '\033[0m'
    BOLD = '\033[1m'


def print_step(message: str):
    print(f"{Colors.BLUE}{Colors.BOLD}► {message}{Colors.END}")


def print_success(message: str):
    print(f"{Colors.GREEN}✓ {message}{Colors.END}")


def print_error(message: str):
    print(f"{Colors.RED}✗ {message}{Colors.END}")


def get_config():
    """Get database configuration from environment."""
    return {
        'user': os.getenv('ECOMMERCE_USER'),
        'password': os.getenv('ECOMMERCE_PASSWORD'),
        'host': os.getenv('ECOMMERCE_HOST'),
        'port': os.getenv('ECOMMERCE_PORT'),
        'database': os.getenv('ECOMMERCE_DATABASE'),
    }


def create_database(config):
    """Create the ecommerce_db database if it doesn't exist."""
    print_step(f"Creating database '{config['database']}'...")
    
    try:
        conn = psycopg2.connect(
            user=config['user'],
            password=config['password'],
            host=config['host'],
            port=config['port'],
            database='postgres'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        # Check if database exists
        cur.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (config['database'],)
        )
        exists = cur.fetchone()
        
        if not exists:
            cur.execute(f"CREATE DATABASE {config['database']}")
            print_success(f"Database '{config['database']}' created")
        else:
            print_success(f"Database '{config['database']}' already exists")
        
        cur.close()
        conn.close()
        return True
        
    except Exception as e:
        print_error(f"Error creating database: {e}")
        return False


def create_schema(config):
    """Create all tables for the e-commerce database."""
    print_step("Creating database schema...")
    
    conn = psycopg2.connect(
        user=config['user'],
        password=config['password'],
        host=config['host'],
        port=config['port'],
        database=config['database']
    )
    cur = conn.cursor()
    
    # Drop existing tables (in reverse order of dependencies)
    print_step("Dropping existing tables if any...")
    cur.execute("""
        DROP TABLE IF EXISTS order_items CASCADE;
        DROP TABLE IF EXISTS orders CASCADE;
        DROP TABLE IF EXISTS reviews CASCADE;
        DROP TABLE IF EXISTS products CASCADE;
        DROP TABLE IF EXISTS categories CASCADE;
        DROP TABLE IF EXISTS addresses CASCADE;
        DROP TABLE IF EXISTS customers CASCADE;
    """)
    
    # Create customers table
    cur.execute("""
        CREATE TABLE customers (
            customer_id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            first_name VARCHAR(100) NOT NULL,
            last_name VARCHAR(100) NOT NULL,
            phone VARCHAR(20),
            date_joined DATE NOT NULL DEFAULT CURRENT_DATE,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            loyalty_points INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print_success("Created customers table")
    
    # Create addresses table
    cur.execute("""
        CREATE TABLE addresses (
            address_id SERIAL PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
            address_type VARCHAR(20) NOT NULL CHECK (address_type IN ('billing', 'shipping')),
            street_address VARCHAR(255) NOT NULL,
            city VARCHAR(100) NOT NULL,
            state VARCHAR(50) NOT NULL,
            postal_code VARCHAR(20) NOT NULL,
            country VARCHAR(100) NOT NULL DEFAULT 'USA',
            is_default BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print_success("Created addresses table")
    
    # Create categories table
    cur.execute("""
        CREATE TABLE categories (
            category_id SERIAL PRIMARY KEY,
            category_name VARCHAR(100) UNIQUE NOT NULL,
            description TEXT,
            parent_category_id INTEGER REFERENCES categories(category_id) ON DELETE SET NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print_success("Created categories table")
    
    # Create products table
    cur.execute("""
        CREATE TABLE products (
            product_id SERIAL PRIMARY KEY,
            product_name VARCHAR(255) NOT NULL,
            category_id INTEGER REFERENCES categories(category_id) ON DELETE SET NULL,
            description TEXT,
            price DECIMAL(10, 2) NOT NULL,
            cost DECIMAL(10, 2),
            stock_quantity INTEGER NOT NULL DEFAULT 0,
            sku VARCHAR(50) UNIQUE NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            weight_kg DECIMAL(8, 2),
            dimensions VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print_success("Created products table")
    
    # Create orders table
    cur.execute("""
        CREATE TABLE orders (
            order_id SERIAL PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
            shipping_address_id INTEGER REFERENCES addresses(address_id) ON DELETE SET NULL,
            order_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(20) NOT NULL DEFAULT 'pending' 
                CHECK (status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled')),
            total_amount DECIMAL(10, 2) NOT NULL,
            shipping_cost DECIMAL(10, 2) DEFAULT 0.00,
            tax_amount DECIMAL(10, 2) DEFAULT 0.00,
            discount_amount DECIMAL(10, 2) DEFAULT 0.00,
            payment_method VARCHAR(50),
            shipped_date TIMESTAMP,
            delivered_date TIMESTAMP,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print_success("Created orders table")
    
    # Create order_items table
    cur.execute("""
        CREATE TABLE order_items (
            order_item_id SERIAL PRIMARY KEY,
            order_id INTEGER NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
            product_id INTEGER NOT NULL REFERENCES products(product_id) ON DELETE RESTRICT,
            quantity INTEGER NOT NULL CHECK (quantity > 0),
            unit_price DECIMAL(10, 2) NOT NULL,
            discount_percent DECIMAL(5, 2) DEFAULT 0.00,
            subtotal DECIMAL(10, 2) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print_success("Created order_items table")
    
    # Create reviews table
    cur.execute("""
        CREATE TABLE reviews (
            review_id SERIAL PRIMARY KEY,
            product_id INTEGER NOT NULL REFERENCES products(product_id) ON DELETE CASCADE,
            customer_id INTEGER NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
            rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
            title VARCHAR(200),
            comment TEXT,
            is_verified_purchase BOOLEAN DEFAULT FALSE,
            helpful_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(product_id, customer_id)
        )
    """)
    print_success("Created reviews table")
    
    # Create indexes for better query performance
    print_step("Creating indexes...")
    cur.execute("CREATE INDEX idx_orders_customer_id ON orders(customer_id)")
    cur.execute("CREATE INDEX idx_orders_order_date ON orders(order_date)")
    cur.execute("CREATE INDEX idx_orders_status ON orders(status)")
    cur.execute("CREATE INDEX idx_order_items_order_id ON order_items(order_id)")
    cur.execute("CREATE INDEX idx_order_items_product_id ON order_items(product_id)")
    cur.execute("CREATE INDEX idx_products_category_id ON products(category_id)")
    cur.execute("CREATE INDEX idx_reviews_product_id ON reviews(product_id)")
    cur.execute("CREATE INDEX idx_reviews_customer_id ON reviews(customer_id)")
    cur.execute("CREATE INDEX idx_addresses_customer_id ON addresses(customer_id)")
    print_success("Created indexes")
    
    conn.commit()
    cur.close()
    conn.close()
    
    print_success("Schema created successfully")
    return True


def populate_sample_data(config):
    """Populate tables with realistic sample data."""
    print_step("Populating sample data...")
    
    conn = psycopg2.connect(
        user=config['user'],
        password=config['password'],
        host=config['host'],
        port=config['port'],
        database=config['database']
    )
    cur = conn.cursor()
    
    # Sample data
    first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'Robert', 'Lisa', 
                   'James', 'Mary', 'William', 'Patricia', 'Richard', 'Jennifer', 'Thomas']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 
                  'Davis', 'Rodriguez', 'Martinez', 'Wilson', 'Anderson', 'Taylor']
    
    # Insert customers
    print_step("Inserting customers...")
    customers = []
    for i in range(50):
        first = random.choice(first_names)
        last = random.choice(last_names)
        email = f"{first.lower()}.{last.lower()}{i}@email.com"
        phone = f"+1-555-{random.randint(1000, 9999)}"
        days_ago = random.randint(1, 730)  # Up to 2 years ago
        date_joined = datetime.now().date() - timedelta(days=days_ago)
        loyalty_points = random.randint(0, 5000)
        
        cur.execute("""
            INSERT INTO customers (email, first_name, last_name, phone, date_joined, loyalty_points)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING customer_id
        """, (email, first, last, phone, date_joined, loyalty_points))
        
        customers.append(cur.fetchone()[0])
    
    print_success(f"Inserted {len(customers)} customers")
    
    # Insert addresses
    print_step("Inserting addresses...")
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia']
    states = ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA']
    
    for customer_id in customers:
        # Each customer gets 1-2 addresses
        num_addresses = random.randint(1, 2)
        for i in range(num_addresses):
            city_idx = random.randint(0, len(cities) - 1)
            cur.execute("""
                INSERT INTO addresses (customer_id, address_type, street_address, city, state, postal_code, is_default)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                customer_id,
                'shipping' if i == 0 else 'billing',
                f"{random.randint(100, 9999)} Main St",
                cities[city_idx],
                states[city_idx],
                f"{random.randint(10000, 99999)}",
                i == 0
            ))
    
    print_success("Inserted addresses")
    
    # Insert categories
    print_step("Inserting categories...")
    categories_data = [
        ('Electronics', 'Electronic devices and accessories', None),
        ('Laptops', 'Portable computers', 1),
        ('Smartphones', 'Mobile phones', 1),
        ('Accessories', 'Electronic accessories', 1),
        ('Clothing', 'Apparel and fashion', None),
        ('Men', 'Men\'s clothing', 5),
        ('Women', 'Women\'s clothing', 5),
        ('Home & Garden', 'Home and garden items', None),
        ('Furniture', 'Home furniture', 8),
        ('Kitchen', 'Kitchen appliances and tools', 8),
        ('Books', 'Books and literature', None),
        ('Sports', 'Sports equipment and gear', None),
    ]
    
    category_ids = {}
    for name, desc, parent_id in categories_data:
        cur.execute("""
            INSERT INTO categories (category_name, description, parent_category_id)
            VALUES (%s, %s, %s)
            RETURNING category_id
        """, (name, desc, parent_id))
        category_ids[name] = cur.fetchone()[0]
    
    print_success(f"Inserted {len(categories_data)} categories")
    
    # Insert products
    print_step("Inserting products...")
    products_data = [
        # Electronics - Laptops
        ('MacBook Pro 16"', category_ids['Laptops'], 'High-performance laptop', 2499.99, 1800.00, 25, 'LAPTOP-MBP16'),
        ('Dell XPS 15', category_ids['Laptops'], 'Professional laptop', 1899.99, 1400.00, 30, 'LAPTOP-XPS15'),
        ('ThinkPad X1 Carbon', category_ids['Laptops'], 'Business laptop', 1699.99, 1200.00, 20, 'LAPTOP-X1C'),
        
        # Electronics - Smartphones
        ('iPhone 15 Pro', category_ids['Smartphones'], 'Latest iPhone', 999.99, 700.00, 50, 'PHONE-IP15P'),
        ('Samsung Galaxy S24', category_ids['Smartphones'], 'Android flagship', 899.99, 650.00, 45, 'PHONE-SGS24'),
        ('Google Pixel 8', category_ids['Smartphones'], 'Pure Android experience', 699.99, 500.00, 35, 'PHONE-PIX8'),
        
        # Electronics - Accessories
        ('Wireless Mouse', category_ids['Accessories'], 'Ergonomic wireless mouse', 29.99, 15.00, 100, 'ACC-MOUSE-W'),
        ('USB-C Hub', category_ids['Accessories'], '7-in-1 USB-C adapter', 49.99, 25.00, 80, 'ACC-USBC-HUB'),
        ('Laptop Stand', category_ids['Accessories'], 'Aluminum laptop stand', 39.99, 20.00, 60, 'ACC-STAND'),
        
        # Clothing - Men
        ('Men\'s T-Shirt', category_ids['Men'], 'Cotton t-shirt', 19.99, 8.00, 200, 'CLOTH-M-TSHIRT'),
        ('Men\'s Jeans', category_ids['Men'], 'Denim jeans', 59.99, 30.00, 150, 'CLOTH-M-JEANS'),
        
        # Clothing - Women
        ('Women\'s Dress', category_ids['Women'], 'Summer dress', 49.99, 25.00, 100, 'CLOTH-W-DRESS'),
        ('Women\'s Blouse', category_ids['Women'], 'Silk blouse', 39.99, 20.00, 120, 'CLOTH-W-BLOUSE'),
        
        # Home & Garden - Furniture
        ('Office Chair', category_ids['Furniture'], 'Ergonomic office chair', 299.99, 150.00, 40, 'FURN-CHAIR-OFF'),
        ('Standing Desk', category_ids['Furniture'], 'Adjustable standing desk', 499.99, 250.00, 25, 'FURN-DESK-STD'),
        
        # Home & Garden - Kitchen
        ('Coffee Maker', category_ids['Kitchen'], 'Programmable coffee maker', 89.99, 45.00, 60, 'KITCH-COFFEE'),
        ('Blender', category_ids['Kitchen'], 'High-speed blender', 129.99, 65.00, 50, 'KITCH-BLEND'),
        
        # Books
        ('Python Programming', category_ids['Books'], 'Learn Python programming', 39.99, 20.00, 75, 'BOOK-PYTHON'),
        ('Data Science Handbook', category_ids['Books'], 'Data science guide', 49.99, 25.00, 60, 'BOOK-DS'),
        
        # Sports
        ('Yoga Mat', category_ids['Sports'], 'Non-slip yoga mat', 24.99, 12.00, 100, 'SPORT-YOGAMAT'),
        ('Dumbbells Set', category_ids['Sports'], 'Adjustable dumbbells', 199.99, 100.00, 30, 'SPORT-DUMBELLS'),
    ]
    
    product_ids = []
    for name, cat_id, desc, price, cost, stock, sku in products_data:
        cur.execute("""
            INSERT INTO products (product_name, category_id, description, price, cost, stock_quantity, sku)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING product_id
        """, (name, cat_id, desc, price, cost, stock, sku))
        product_ids.append(cur.fetchone()[0])
    
    print_success(f"Inserted {len(products_data)} products")
    
    # Insert orders and order_items
    print_step("Inserting orders and order items...")
    statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
    payment_methods = ['credit_card', 'debit_card', 'paypal', 'apple_pay']
    
    total_orders = 0
    total_items = 0
    
    for customer_id in customers:
        # Each customer has 0-5 orders
        num_orders = random.randint(0, 5)
        
        for _ in range(num_orders):
            # Order date within last year
            days_ago = random.randint(1, 365)
            order_date = datetime.now() - timedelta(days=days_ago)
            
            status = random.choice(statuses)
            payment_method = random.choice(payment_methods)
            
            # Get shipping address
            cur.execute("""
                SELECT address_id FROM addresses 
                WHERE customer_id = %s AND address_type = 'shipping' 
                LIMIT 1
            """, (customer_id,))
            
            result = cur.fetchone()
            shipping_address_id = result[0] if result else None
            
            # Calculate shipped/delivered dates based on status
            shipped_date = None
            delivered_date = None
            
            if status in ['shipped', 'delivered']:
                shipped_date = order_date + timedelta(days=random.randint(1, 3))
            
            if status == 'delivered':
                delivered_date = shipped_date + timedelta(days=random.randint(2, 7))
            
            # Create order items
            num_items = random.randint(1, 5)
            selected_products = random.sample(product_ids, min(num_items, len(product_ids)))
            
            items_total = Decimal('0')
            order_items_data = []
            
            for product_id in selected_products:
                # Get product price
                cur.execute("SELECT price FROM products WHERE product_id = %s", (product_id,))
                unit_price = cur.fetchone()[0]
                
                quantity = random.randint(1, 3)
                discount_percent = random.choice([0, 0, 0, 5, 10, 15])  # Most items no discount
                discount_multiplier = Decimal('1') - (Decimal(str(discount_percent)) / Decimal('100'))
                subtotal = (unit_price * quantity) * discount_multiplier
                items_total += subtotal
                
                order_items_data.append((product_id, quantity, unit_price, discount_percent, subtotal))
            
            # Calculate order totals
            shipping_cost = Decimal('0') if items_total > 50 else Decimal('9.99')
            tax_amount = items_total * Decimal('0.08')  # 8% tax
            discount_amount = Decimal('0')
            total_amount = items_total + shipping_cost + tax_amount - discount_amount
            
            # Insert order
            cur.execute("""
                INSERT INTO orders (
                    customer_id, shipping_address_id, order_date, status,
                    total_amount, shipping_cost, tax_amount, discount_amount,
                    payment_method, shipped_date, delivered_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING order_id
            """, (
                customer_id, shipping_address_id, order_date, status,
                total_amount, shipping_cost, tax_amount, discount_amount,
                payment_method, shipped_date, delivered_date
            ))
            
            order_id = cur.fetchone()[0]
            total_orders += 1
            
            # Insert order items
            for product_id, quantity, unit_price, discount_percent, subtotal in order_items_data:
                cur.execute("""
                    INSERT INTO order_items (
                        order_id, product_id, quantity, unit_price, discount_percent, subtotal
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (order_id, product_id, quantity, unit_price, discount_percent, subtotal))
                total_items += 1
    
    print_success(f"Inserted {total_orders} orders with {total_items} order items")
    
    # Insert reviews
    print_step("Inserting reviews...")
    review_titles = [
        'Great product!', 'Excellent quality', 'Worth the price', 'Highly recommend',
        'Not bad', 'Could be better', 'Disappointed', 'Fantastic!', 'Good value',
        'Amazing', 'Perfect', 'Love it', 'Okay product', 'Not as expected'
    ]
    
    review_comments = [
        'Really happy with this purchase. Exceeded my expectations!',
        'Good quality for the price. Would buy again.',
        'Works as advertised. No complaints.',
        'Not quite what I was looking for, but it\'s okay.',
        'Excellent product! Highly recommended to everyone.',
        'Had some issues but customer service was helpful.',
        'Perfect for my needs. Very satisfied.',
        'Quality could be better for this price point.',
    ]
    
    total_reviews = 0
    
    # Add reviews for random products from random customers
    for _ in range(100):
        customer_id = random.choice(customers)
        product_id = random.choice(product_ids)
        
        # Check if review already exists
        cur.execute("""
            SELECT 1 FROM reviews WHERE customer_id = %s AND product_id = %s
        """, (customer_id, product_id))
        
        if cur.fetchone() is None:
            rating = random.randint(1, 5)
            # Higher ratings more likely
            if random.random() < 0.6:
                rating = random.randint(4, 5)
            
            title = random.choice(review_titles)
            comment = random.choice(review_comments)
            is_verified = random.choice([True, True, True, False])  # 75% verified
            helpful_count = random.randint(0, 50) if rating >= 4 else random.randint(0, 10)
            
            cur.execute("""
                INSERT INTO reviews (
                    product_id, customer_id, rating, title, comment,
                    is_verified_purchase, helpful_count
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (product_id, customer_id, rating, title, comment, is_verified, helpful_count))
            
            total_reviews += 1
    
    print_success(f"Inserted {total_reviews} reviews")
    
    conn.commit()
    cur.close()
    conn.close()
    
    print_success("Sample data populated successfully")
    return True


def print_summary(config):
    """Print database summary."""
    conn = psycopg2.connect(
        user=config['user'],
        password=config['password'],
        host=config['host'],
        port=config['port'],
        database=config['database']
    )
    cur = conn.cursor()
    
    # Get row counts
    tables = ['customers', 'addresses', 'categories', 'products', 'orders', 'order_items', 'reviews']
    counts = {}
    
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        counts[table] = cur.fetchone()[0]
    
    cur.close()
    conn.close()
    
    print(f"\n{Colors.BOLD}{'=' * 60}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.GREEN}✓ E-commerce Database Ready!{Colors.END}")
    print(f"{Colors.BOLD}{'=' * 60}{Colors.END}\n")
    
    print(f"{Colors.BOLD}Database: {config['database']}{Colors.END}")
    print(f"Host: {config['host']}:{config['port']}\n")
    
    print(f"{Colors.BOLD}Table Summary:{Colors.END}")
    for table in tables:
        print(f"  {table:20s} {counts[table]:6d} rows")
    
    print(f"\n{Colors.BOLD}Sample Queries to Try:{Colors.END}")
    print("  1. 'Show me all orders from last month'")
    print("  2. 'Which products have the highest ratings?'")
    print("  3. 'List customers with more than 5 orders'")
    print("  4. 'What are the top selling products by revenue?'")
    print("  5. 'Show me pending orders with their customer names'")
    
    print(f"\n{Colors.BOLD}Connection String:{Colors.END}")
    print(f"  postgresql://{config['user']}:****@{config['host']}:{config['port']}/{config['database']}\n")


def main():
    """Main setup function."""
    print(f"\n{Colors.BOLD}E-commerce Database Setup{Colors.END}\n")
    
    config = get_config()
    
    # Step 1: Create database
    if not create_database(config):
        sys.exit(1)
    
    # Step 2: Create schema
    if not create_schema(config):
        sys.exit(1)
    
    # Step 3: Populate data
    if not populate_sample_data(config):
        sys.exit(1)
    
    # Step 4: Print summary
    print_summary(config)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Setup cancelled{Colors.END}")
        sys.exit(1)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
