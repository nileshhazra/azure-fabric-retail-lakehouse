# ======================================================================================
# MICROSOFT FABRIC NOTEBOOK: 02_silver_to_gold_star_schema
# Description: Transforms Silver Delta tables into a Kimball Star Schema Gold Layer:
#              - dim_date (Full date dimension with temporal hierarchies)
#              - dim_store (Conformed store & channel dimension)
#              - fact_sales (High-performance transactional sales fact)
#              - gold_daily_store_sales (Pre-aggregated business reporting table)
# ======================================================================================

# Cell 1: Imports and Environment Setup
from pyspark.sql.functions import (
    col, to_date, date_format, year, quarter, month, dayofmonth,
    dayofweek, when, lit, monotonically_increasing_id, round as spark_round,
    sum as spark_sum, count as spark_count, avg as spark_avg, explode, sequence
)
from pyspark.sql.types import IntegerType

print("PySpark initialized for Gold Layer (Kimball Star Schema) modeling.")

# --------------------------------------------------------------------------------------
# Cell 2: Build Conformed Date Dimension (dim_date)
# Generates a standard Kimball calendar dimension spanning all order dates
# --------------------------------------------------------------------------------------
print("Building [dim_date] dimension...")

# Determine min and max order dates from Silver
silver_orders = spark.table("silver_sales_orders")
min_max_dates = silver_orders.selectExpr("min(OrderDateOnly) as min_dt", "max(OrderDateOnly) as max_dt").collect()[0]

min_date = min_max_dates["min_dt"] or "2026-01-01"
max_date = min_max_dates["max_dt"] or "2026-12-31"

# Generate continuous daily date range
df_dates = spark.sql(f"SELECT sequence(to_date('{min_date}'), to_date('{max_date}'), interval 1 day) as date_seq") \
    .withColumn("FullDate", explode(col("date_seq"))) \
    .select("FullDate")

df_dim_date = df_dates.select(
    date_format(col("FullDate"), "yyyyMMdd").cast(IntegerType()).alias("DateKey"),
    col("FullDate").alias("Date"),
    year(col("FullDate")).alias("Year"),
    quarter(col("FullDate")).alias("Quarter"),
    month(col("FullDate")).alias("Month"),
    date_format(col("FullDate"), "MMMM").alias("MonthName"),
    dayofmonth(col("FullDate")).alias("DayOfMonth"),
    dayofweek(col("FullDate")).alias("DayOfWeek"),
    date_format(col("FullDate"), "EEEE").alias("DayName"),
    when(dayofweek(col("FullDate")).isin(1, 7), lit(1)).otherwise(lit(0)).alias("IsWeekend")
)

# Write dim_date as Gold Delta Table
df_dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_date")
print(f"[+] dim_date table created with {df_dim_date.count()} date records.")
display(df_dim_date.limit(5))

# --------------------------------------------------------------------------------------
# Cell 3: Build Store Dimension (dim_store)
# Extracts distinct retail store and digital channel footprints
# --------------------------------------------------------------------------------------
print("Building [dim_store] dimension...")

df_stores = silver_orders.select("StoreLocation").distinct()

df_dim_store = df_stores.select(
    (monotonically_increasing_id() + 101).cast(IntegerType()).alias("StoreKey"),
    col("StoreLocation").alias("StoreName"),
    when(col("StoreLocation").like("Online%"), "E-Commerce").otherwise("Brick & Mortar").alias("SalesChannel"),
    when(col("StoreLocation").like("%Dallas%"), "South")
        .when(col("StoreLocation").like("%Chicago%"), "Midwest")
        .when(col("StoreLocation").like("%Atlanta%"), "Southeast")
        .when(col("StoreLocation").like("%Seattle%"), "Northwest")
        .when(col("StoreLocation").like("%NewYork%"), "Northeast")
        .otherwise("Digital / Global").alias("Region")
)

df_dim_store.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_store")
print(f"[+] dim_store table created with {df_dim_store.count()} store locations.")
display(df_dim_store)

# --------------------------------------------------------------------------------------
# Cell 4: Build Sales Fact Table (fact_sales)
# Enriches transactions with Surrogate Keys and calculates analytical metrics
# --------------------------------------------------------------------------------------
print("Building [fact_sales] Star Schema Fact Table...")

df_stores_lookup = spark.table("dim_store")

df_fact_sales = silver_orders \
    .join(df_stores_lookup, silver_orders.StoreLocation == df_stores_lookup.StoreName, "left") \
    .select(
        col("OrderID").alias("SalesOrderID"),
        col("OrderNumber"),
        date_format(col("OrderDateOnly"), "yyyyMMdd").cast(IntegerType()).alias("DateKey"),
        col("StoreKey"),
        col("CustomerID"),
        col("OrderStatus"),
        col("PaymentMethod"),
        spark_round(col("TotalAmount"), 2).alias("GrossSalesAmount"),
        spark_round(col("TaxAmount"), 2).alias("TaxAmount"),
        spark_round(col("ShippingCost"), 2).alias("ShippingCost"),
        spark_round(col("TotalAmount") - col("TaxAmount") - col("ShippingCost"), 2).alias("NetSalesAmount"),
        col("ModifiedDate").alias("LastModifiedTimestamp")
    )

df_fact_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fact_sales")
print(f"[+] fact_sales table created with {df_fact_sales.count()} transactions.")
display(df_fact_sales.limit(5))

# --------------------------------------------------------------------------------------
# Cell 5: Build Business Aggregation Table (gold_daily_store_sales)
# High-performance summary table for C-Suite dashboards and Direct Lake Power BI
# --------------------------------------------------------------------------------------
print("Building [gold_daily_store_sales] aggregated analytics table...")

df_gold_agg = df_fact_sales.groupBy("DateKey", "StoreKey") \
    .agg(
        spark_count("SalesOrderID").alias("DailyOrderVolume"),
        spark_round(spark_sum("GrossSalesAmount"), 2).alias("DailyGrossRevenue"),
        spark_round(spark_sum("NetSalesAmount"), 2).alias("DailyNetRevenue"),
        spark_round(spark_avg("GrossSalesAmount"), 2).alias("AverageOrderValue")
    )

df_gold_agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_daily_store_sales")
print(f"[+] gold_daily_store_sales created with {df_gold_agg.count()} aggregated rows.")

# --------------------------------------------------------------------------------------
# Cell 6: Table Compaction & Z-Order Clustering (FinOps / Query Optimization)
# --------------------------------------------------------------------------------------
print("Applying Delta Lake Z-Ordering for maximum Direct Lake query performance...")
spark.sql("OPTIMIZE fact_sales ZORDER BY (DateKey, StoreKey)")
spark.sql("OPTIMIZE dim_date ZORDER BY (DateKey)")

print("[SUCCESS] Gold Layer Kimball Star Schema generated and optimized successfully!")
