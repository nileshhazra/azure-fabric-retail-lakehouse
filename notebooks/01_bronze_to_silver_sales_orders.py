# ======================================================================================
# MICROSOFT FABRIC NOTEBOOK: 01_bronze_to_silver_sales_orders
# Description: Ingests raw Parquet from OneLake Bronze Shortcut, performs schema
#              validation, quality filtering, deduplication, and merges into the
#              Silver Delta table with V-Order optimization.
# ======================================================================================

# Cell 1: Imports and Lakehouse Context Setup
import os
from pyspark.sql.functions import (
    col, current_timestamp, to_date, row_number, when, lit
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DecimalType, TimestampType
)
from delta.tables import DeltaTable

print("PySpark and Delta libraries loaded successfully.")

# --------------------------------------------------------------------------------------
# Cell 2: Define Explicit Schema for Ingestion (Schema Enforcement)
# --------------------------------------------------------------------------------------
sales_orders_schema = StructType([
    StructField("OrderID", IntegerType(), False),
    StructField("OrderNumber", StringType(), False),
    StructField("CustomerID", IntegerType(), False),
    StructField("StoreLocation", StringType(), True),
    StructField("OrderStatus", StringType(), True),
    StructField("PaymentMethod", StringType(), True),
    StructField("TaxAmount", DecimalType(18, 2), True),
    StructField("ShippingCost", DecimalType(18, 2), True),
    StructField("TotalAmount", DecimalType(18, 2), True),
    StructField("OrderDate", TimestampType(), True),
    StructField("ModifiedDate", TimestampType(), True)
])

# Path to OneLake Bronze Shortcut (adjust shortcut folder name if different)
BRONZE_SHORTCUT_PATH = "Files/bronze_adls/sales_orders"
SILVER_TABLE_NAME = "silver_sales_orders"
QUARANTINE_TABLE_NAME = "silver_quarantined_orders"

# --------------------------------------------------------------------------------------
# Cell 3: Read Raw Bronze Parquet Files
# --------------------------------------------------------------------------------------
print(f"Reading raw Parquet data from OneLake shortcut: {BRONZE_SHORTCUT_PATH}...")
df_raw = spark.read \
    .schema(sales_orders_schema) \
    .parquet(BRONZE_SHORTCUT_PATH)

total_raw_count = df_raw.count()
print(f"Total raw records loaded: {total_raw_count}")
display(df_raw.limit(5))

# --------------------------------------------------------------------------------------
# Cell 4: Data Quality Gating (Circuit Breaker)
# --------------------------------------------------------------------------------------
# Business Rules:
#  1. OrderID and CustomerID must NOT be NULL.
#  2. TotalAmount must be >= 0.
#  3. OrderDate must NOT be NULL.
df_valid = df_raw.filter(
    col("OrderID").isNotNull() &
    col("CustomerID").isNotNull() &
    (col("TotalAmount") >= 0) &
    col("OrderDate").isNotNull()
)

df_quarantine = df_raw.filter(
    col("OrderID").isNull() |
    col("CustomerID").isNull() |
    (col("TotalAmount") < 0) |
    col("OrderDate").isNull()
).withColumn("_rejection_reason", 
    when(col("OrderID").isNull(), "Null OrderID")
    .when(col("CustomerID").isNull(), "Null CustomerID")
    .when(col("TotalAmount") < 0, "Negative TotalAmount")
    .otherwise("Invalid OrderDate")
).withColumn("_quarantined_at", current_timestamp())

valid_count = df_valid.count()
quarantine_count = df_quarantine.count()
print(f"Valid records: {valid_count} | Quarantined records: {quarantine_count}")

# Persist quarantined records for data governance audit
if quarantine_count > 0:
    df_quarantine.write.format("delta").mode("append").saveAsTable(QUARANTINE_TABLE_NAME)
    print(f"Appended {quarantine_count} bad records to audit table [{QUARANTINE_TABLE_NAME}].")

# --------------------------------------------------------------------------------------
# Cell 5: Deduplication (Window Function over Primary Key)
# Handles situations where multiple updates to the same order exist in the batch
# --------------------------------------------------------------------------------------
window_dedup = Window.partitionBy("OrderID").orderBy(col("ModifiedDate").desc())

df_deduped = df_valid \
    .withColumn("_row_num", row_number().over(window_dedup)) \
    .filter(col("_row_num") == 1) \
    .drop("_row_num") \
    .withColumn("_ingested_to_silver_at", current_timestamp()) \
    .withColumn("OrderDateOnly", to_date(col("OrderDate")))

print(f"Record count after deduplication: {df_deduped.count()}")

# --------------------------------------------------------------------------------------
# Cell 6: Delta Lake MERGE (Idempotent Upsert into Silver)
# --------------------------------------------------------------------------------------
# If the target table does not exist, create it. Otherwise, perform an ACID MERGE.
if not spark.catalog.tableExists(SILVER_TABLE_NAME):
    print(f"Target table [{SILVER_TABLE_NAME}] does not exist. Initializing Delta table...")
    df_deduped.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(SILVER_TABLE_NAME)
    print(f"[+] Silver table [{SILVER_TABLE_NAME}] created successfully.")
else:
    print(f"Merging delta updates into existing table [{SILVER_TABLE_NAME}]...")
    silver_target = DeltaTable.forName(spark, SILVER_TABLE_NAME)
    
    silver_target.alias("target").merge(
        source=df_deduped.alias("source"),
        condition="target.OrderID = source.OrderID"
    ).whenMatchedUpdate(
        condition="source.ModifiedDate > target.ModifiedDate",
        set={
            "OrderNumber": "source.OrderNumber",
            "CustomerID": "source.CustomerID",
            "StoreLocation": "source.StoreLocation",
            "OrderStatus": "source.OrderStatus",
            "PaymentMethod": "source.PaymentMethod",
            "TaxAmount": "source.TaxAmount",
            "ShippingCost": "source.ShippingCost",
            "TotalAmount": "source.TotalAmount",
            "ModifiedDate": "source.ModifiedDate",
            "_ingested_to_silver_at": "source._ingested_to_silver_at"
        }
    ).whenNotMatchedInsertAll().execute()
    print("[+] Delta Lake merge completed successfully.")

# --------------------------------------------------------------------------------------
# Cell 7: Table Compaction & Z-Order Clustering (FinOps / Performance)
# --------------------------------------------------------------------------------------
# Optimizes Parquet layout and co-locates rows by OrderDate and CustomerID
spark.sql(f"OPTIMIZE {SILVER_TABLE_NAME} ZORDER BY (OrderDateOnly, CustomerID)")
print("[+] Table optimization and Z-Ordering complete.")

# --------------------------------------------------------------------------------------
# Cell 8: Final Validation & Sanity Inspection
# --------------------------------------------------------------------------------------
df_final = spark.table(SILVER_TABLE_NAME)
print(f"Final total count in [{SILVER_TABLE_NAME}]: {df_final.count()}")
display(df_final.groupBy("OrderStatus").count())
