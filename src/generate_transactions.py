"""
Enterprise Retail Data Generator
Simulates realistic e-commerce transactions for Azure SQL Database.
Supports:
  - Initial Seed Mode: Generates baseline customers, products, orders, and line items.
  - Incremental Mode: Generates new orders, updates order statuses, and triggers
                      customer profile mutations (to test SCD Type 2 in downstream Silver layer).
"""

import os
import sys
import random
import argparse
from datetime import datetime, timedelta, timezone
from faker import Faker

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

fake = Faker()

# Database connection configuration
DB_SERVER = os.getenv("DB_SERVER", "")
DB_DATABASE = os.getenv("DB_DATABASE", "sqldb-retail-source")
DB_USERNAME = os.getenv("DB_USERNAME", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_PORT = int(os.getenv("DB_PORT", "1433"))


def get_db_connection():
    """Establish database connection using pymssql or pyodbc based on availability."""
    if not DB_SERVER or not DB_USERNAME or not DB_PASSWORD:
        raise ValueError(
            "Database credentials missing! Please configure DB_SERVER, DB_USERNAME, "
            "and DB_PASSWORD in your .env file."
        )

    # Clean server name if user included protocol
    server_clean = DB_SERVER.replace("https://", "").replace("http://", "").split(":")[0]

    # Try pymssql first
    try:
        import pymssql
        conn = pymssql.connect(
            server=server_clean,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=DB_DATABASE,
            port=DB_PORT,
            autocommit=False
        )
        return conn, "pymssql"
    except ImportError:
        pass

    # Try pyodbc fallback
    try:
        import pyodbc
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server_clean},{DB_PORT};"
            f"DATABASE={DB_DATABASE};"
            f"UID={DB_USERNAME};"
            f"PWD={DB_PASSWORD};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
        conn = pyodbc.connect(conn_str, autocommit=False)
        return conn, "pyodbc"
    except ImportError:
        pass

    print("\n[ERROR] Neither 'pymssql' nor 'pyodbc' is installed in this Python environment.")
    print("Please install via: pip install pymssql OR pip install pyodbc")
    sys.exit(1)


PRODUCT_CATALOG = [
    # Category, SubCategory, Name, Cost, Price
    ("Electronics", "Smartphones", "Apex Pro 5G Smartphone", 450.00, 799.99),
    ("Electronics", "Laptops", "Horizon Ultrabook 14-inch", 650.00, 1199.00),
    ("Electronics", "Accessories", "Noise-Cancelling Wireless Headphones", 60.00, 149.99),
    ("Electronics", "Wearables", "PulseFit Smartwatch V3", 45.00, 129.99),
    ("Electronics", "Storage", "ThunderBolt 1TB External SSD", 40.00, 89.99),
    ("Home & Kitchen", "Appliances", "Smart Bean Espresso Machine", 120.00, 249.50),
    ("Home & Kitchen", "Cookware", "Cast Iron Dutch Oven 6-Qt", 35.00, 79.99),
    ("Home & Kitchen", "Cleaning", "Robot Vacuum with Lidar", 180.00, 399.00),
    ("Home & Kitchen", "Furniture", "Ergonomic Mesh Office Chair", 95.00, 220.00),
    ("Apparel", "Footwear", "CloudStrider Running Shoes", 38.00, 110.00),
    ("Apparel", "Outerwear", "All-Weather Technical Parka", 55.00, 145.00),
    ("Apparel", "Activewear", "Breathable Performance Tee", 8.00, 28.00),
    ("Fitness", "Equipment", "Adjustable Quick-Select Dumbbell Set", 110.00, 275.00),
    ("Fitness", "Yoga", "Eco-Grip High Density Yoga Mat", 14.00, 42.00),
    ("Office", "Supplies", "Motorized Standing Desk 55x28", 160.00, 380.00)
]

STORE_LOCATIONS = [
    "Store-Dallas-01", "Store-Chicago-02", "Store-Atlanta-03",
    "Store-Seattle-04", "Store-NewYork-05", "Online-WebStore",
    "Online-MobileApp"
]

CUSTOMER_SEGMENTS = ["Standard", "Silver", "Gold", "Platinum"]
ORDER_STATUSES = ["Pending", "Processing", "Shipped", "Delivered", "Cancelled"]


def seed_products(cursor, conn):
    """Seed initial product catalog if not already populated."""
    cursor.execute("SELECT COUNT(*) FROM dbo.Products")
    count = cursor.fetchone()[0]
    if count > 0:
        print(f"[*] Products table already contains {count} items. Skipping product seeding.")
        return

    print(f"[*] Seeding {len(PRODUCT_CATALOG)} products...")
    insert_sql = """
        INSERT INTO dbo.Products 
        (ProductSKU, ProductName, Category, SubCategory, UnitCost, UnitPrice, IsActive, CreatedDate, ModifiedDate)
        VALUES (%s, %s, %s, %s, %s, %s, 1, %s, %s)
    """
    now = datetime.now(timezone.utc)
    for idx, (cat, subcat, name, cost, price) in enumerate(PRODUCT_CATALOG, start=1):
        sku = f"SKU-{cat[:3].upper()}-{idx:04d}"
        cursor.execute(insert_sql, (sku, name, cat, subcat, cost, price, now, now))
    conn.commit()
    print("[+] Products successfully seeded.")


def seed_customers(cursor, conn, num_customers=300):
    """Seed baseline customers."""
    cursor.execute("SELECT COUNT(*) FROM dbo.Customers")
    count = cursor.fetchone()[0]
    if count >= num_customers:
        print(f"[*] Customers table already contains {count} records. Skipping customer seeding.")
        return

    to_create = num_customers - count
    print(f"[*] Seeding {to_create} new customer profiles...")
    insert_sql = """
        INSERT INTO dbo.Customers
        (CustomerCode, FirstName, LastName, Email, PhoneNumber, CustomerSegment, City, State, PostalCode, Country, CreatedDate, ModifiedDate)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'United States', %s, %s)
    """
    now = datetime.now(timezone.utc)
    batch = []
    for i in range(to_create):
        first = fake.first_name()
        last = fake.last_name()
        code = f"CUST-{random.randint(10000, 99999)}-{i}"
        email = f"{first.lower()}.{last.lower()}.{random.randint(10, 999)}@{fake.free_email_domain()}"
        phone = fake.phone_number()[:20]
        seg = random.choices(CUSTOMER_SEGMENTS, weights=[60, 25, 10, 5])[0]
        city = fake.city()
        state = fake.state_abbr()
        zip_code = fake.zipcode()
        created = now - timedelta(days=random.randint(10, 90))
        batch.append((code, first, last, email, phone, seg, city, state, zip_code, created, created))

    for row in batch:
        cursor.execute(insert_sql, row)
    conn.commit()
    print(f"[+] Successfully seeded {to_create} customers.")


def create_orders(cursor, conn, num_orders=1000, is_historical=False):
    """Generate orders and associated line items."""
    # Get available customer IDs and products
    cursor.execute("SELECT CustomerID FROM dbo.Customers")
    customer_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT ProductID, UnitPrice FROM dbo.Products WHERE IsActive = 1")
    products = cursor.fetchall()

    if not customer_ids or not products:
        print("[!] Cannot generate orders: Customers or Products tables are empty.")
        return

    print(f"[*] Generating {num_orders} orders...")
    now = datetime.now(timezone.utc)

    order_insert_sql = """
        INSERT INTO dbo.SalesOrders 
        (OrderNumber, CustomerID, StoreLocation, OrderStatus, PaymentMethod, TaxAmount, ShippingCost, TotalAmount, OrderDate, ModifiedDate)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    line_item_insert_sql = """
        INSERT INTO dbo.OrderLineItems
        (OrderID, ProductID, Quantity, UnitPrice, DiscountAmount, CreatedDate, ModifiedDate)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    for i in range(num_orders):
        customer_id = random.choice(customer_ids)
        store = random.choice(STORE_LOCATIONS)
        pay_method = random.choice(["Credit Card", "Debit Card", "PayPal", "Apple Pay"])
        status = random.choices(ORDER_STATUSES, weights=[20, 25, 30, 20, 5])[0]

        if is_historical:
            order_date = now - timedelta(days=random.randint(1, 45), minutes=random.randint(0, 1440))
            modified_date = order_date + timedelta(hours=random.randint(1, 24))
        else:
            order_date = now - timedelta(minutes=random.randint(1, 60))
            modified_date = order_date

        order_num = f"ORD-{int(order_date.timestamp())}-{random.randint(1000, 9999)}-{i}"

        # Preliminary insert with zero total to obtain OrderID
        cursor.execute(
            order_insert_sql,
            (order_num, customer_id, store, status, pay_method, 0.0, 0.0, 0.0, order_date, modified_date)
        )

        # Retrieve generated OrderID
        cursor.execute("SELECT @@IDENTITY")
        order_id = int(cursor.fetchone()[0])

        # Generate 1 to 4 line items
        num_items = random.choices([1, 2, 3, 4], weights=[50, 30, 15, 5])[0]
        selected_products = random.sample(products, k=min(num_items, len(products)))

        subtotal = 0.0
        for prod_id, unit_price in selected_products:
            qty = random.choices([1, 2, 3, 5], weights=[70, 20, 7, 3])[0]
            discount = round(float(unit_price) * qty * 0.1, 2) if random.random() < 0.2 else 0.0
            cursor.execute(
                line_item_insert_sql,
                (order_id, prod_id, qty, float(unit_price), discount, order_date, modified_date)
            )
            subtotal += (float(unit_price) * qty) - discount

        tax = round(subtotal * 0.0825, 2)
        shipping = 9.99 if subtotal < 100 and "Online" in store else 0.00
        total = round(subtotal + tax + shipping, 2)

        # Update order totals
        cursor.execute(
            "UPDATE dbo.SalesOrders SET TaxAmount = %s, ShippingCost = %s, TotalAmount = %s WHERE OrderID = %s",
            (tax, shipping, total, order_id)
        )

        if (i + 1) % 250 == 0 or (i + 1) == num_orders:
            conn.commit()
            print(f"    - Processed {i + 1}/{num_orders} orders...")

    print(f"[+] Finished generating {num_orders} orders.")


def mutate_incremental_updates(cursor, conn, num_order_updates=30, num_customer_updates=15):
    """
    Simulates real-world operational changes:
      1. Order status changes (e.g., Pending -> Processing -> Shipped).
      2. Customer attribute updates (e.g., tier upgrade or address move),
         which downstream Spark jobs will capture as SCD Type 2!
    """
    now = datetime.now(timezone.utc)
    print(f"[*] Simulating {num_order_updates} order status transitions...")

    # Fetch eligible pending/processing orders
    cursor.execute("""
        SELECT TOP (%s) OrderID, OrderStatus 
        FROM dbo.SalesOrders 
        WHERE OrderStatus IN ('Pending', 'Processing')
        ORDER BY ModifiedDate ASC
    """, (num_order_updates,))
    orders_to_update = cursor.fetchall()

    for order_id, current_status in orders_to_update:
        new_status = "Processing" if current_status == "Pending" else "Shipped"
        cursor.execute("""
            UPDATE dbo.SalesOrders 
            SET OrderStatus = %s, ModifiedDate = %s 
            WHERE OrderID = %s
        """, (new_status, now, order_id))

    print(f"[*] Simulating {num_customer_updates} customer mutations (triggers SCD Type 2 downstream)...")
    cursor.execute("SELECT TOP (%s) CustomerID, CustomerSegment, City FROM dbo.Customers ORDER BY NEWID()", (num_customer_updates,))
    customers_to_mutate = cursor.fetchall()

    for cust_id, old_seg, old_city in customers_to_mutate:
        new_city = fake.city()
        # Upgrade tier or rotate segment
        new_seg = "Platinum" if old_seg == "Gold" else ("Gold" if old_seg == "Silver" else "Silver")
        cursor.execute("""
            UPDATE dbo.Customers 
            SET CustomerSegment = %s, City = %s, ModifiedDate = %s 
            WHERE CustomerID = %s
        """, (new_seg, new_city, now, cust_id))

    conn.commit()
    print(f"[+] Successfully mutated {len(orders_to_update)} orders and {len(customers_to_mutate)} customer records.")


def main():
    parser = argparse.ArgumentParser(description="Retail Transaction Data Generator for Azure SQL DB")
    parser.add_argument(
        "--mode",
        choices=["initial", "incremental"],
        default="initial",
        help="Run initial baseline seed or incremental daily batch"
    )
    parser.add_argument("--orders", type=int, default=1000, help="Number of orders to generate")
    args = parser.parse_args()

    print("=" * 65)
    print(f"  Enterprise Retail Data Generator -- Mode: {args.mode.upper()}")
    print("=" * 65)

    conn, driver_used = get_db_connection()
    print(f"[*] Connected to Azure SQL DB using [{driver_used}] driver.")

    cursor = conn.cursor()

    try:
        if args.mode == "initial":
            seed_products(cursor, conn)
            seed_customers(cursor, conn, num_customers=300)
            create_orders(cursor, conn, num_orders=args.orders, is_historical=True)
            print("\n[SUCCESS] Baseline historical data generated successfully!")
        else:
            # Incremental daily run
            create_orders(cursor, conn, num_orders=args.orders, is_historical=False)
            mutate_incremental_updates(cursor, conn, num_order_updates=25, num_customer_updates=10)
            print("\n[SUCCESS] Incremental transaction batch generated successfully!")

        # Print current watermark status
        cursor.execute("SELECT TableName, LastProcessedWatermark FROM dbo.WatermarkControl")
        print("\nCurrent Watermark Control Status:")
        for row in cursor.fetchall():
            print(f"  - {row[0]}: {row[1]}")

    except Exception as e:
        conn.rollback()
        print(f"\n[ERROR] Transaction aborted due to error: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
        print("[*] Database connection closed.")


if __name__ == "__main__":
    main()
