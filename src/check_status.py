import os
from dotenv import load_dotenv
import pymssql

load_dotenv()

server = os.getenv("DB_SERVER", "").replace("https://", "").replace("http://", "").split(":")[0]
user = os.getenv("DB_USERNAME")
pwd = os.getenv("DB_PASSWORD")
db = os.getenv("DB_DATABASE")
port = int(os.getenv("DB_PORT", "1433"))

try:
    conn = pymssql.connect(server=server, user=user, password=pwd, database=db, port=port)
    cursor = conn.cursor()
    
    for tbl in ["Customers", "Products", "SalesOrders", "OrderLineItems", "WatermarkControl"]:
        cursor.execute(f"SELECT COUNT(*) FROM dbo.{tbl}")
        cnt = cursor.fetchone()[0]
        print(f"Table dbo.{tbl}: {cnt} rows")
        
    cursor.execute("SELECT * FROM dbo.WatermarkControl")
    print("\nWatermark Control records:")
    for row in cursor.fetchall():
        print(f"  {row}")
        
    cursor.execute("SELECT MAX(ModifiedDate) FROM dbo.SalesOrders")
    max_mod = cursor.fetchone()[0]
    print(f"\nMax ModifiedDate in SalesOrders: {max_mod}")

    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error connecting/querying: {e}")
