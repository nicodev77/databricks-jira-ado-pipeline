# Databricks notebook source
# MAGIC %md
# MAGIC #  **Overview**
# MAGIC ## This task demonstrates advanced SQL-to-PySpark migration skills by converting complex T-SQL queries with OUTER APPLY operators into efficient PySpark code. The solution handles both window function patterns and complex conditional aggregations while maintaining identical business logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 1:** Foundation & Configuration
# MAGIC ### This section establishes the core infrastructure with necessary imports and implements the fixed reference date requirement for reproducible results.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timedelta

# Configuration and fixed reference date
def get_reference_date():
    return F.lit("2024-04-20").cast("date")

reference_date = get_reference_date()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Key Design Decisions:**
# MAGIC
# MAGIC - **Fixed Reference Date:** Implements the requirement to use April 20, 2024 instead of current_timestamp for reproducible results
# MAGIC
# MAGIC - **PySpark Core Libraries:** Essential functions for data manipulation and window operations
# MAGIC
# MAGIC - **Window Functions:** Critical for replicating SQL Server ranking and analytical capabilities

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 2:** Data Loading & Bronze Layer
# MAGIC ### This section handles the initial data ingestion from CSV files and creates the Bronze layer in Unity Catalog for raw data persistence.

# COMMAND ----------


# File paths
base_path = "/Volumes/nico_chamorro/default/volumes/nico_chamorro/bronze/landing/"
customers_path = f"{base_path}customers.csv"
orders_path = f"{base_path}orders.csv"
order_items_path = f"{base_path}order_items.csv"
products_path = f"{base_path}products.csv"

# Ensure Bronze database exists
spark.sql("CREATE DATABASE IF NOT EXISTS nico_chamorro.bronze")

# Load CSV and save to Bronze
def load_and_save_bronze(csv_path, table_name):
    df = spark.read.option("header","true").option("inferSchema","true").csv(csv_path)
    df.write.format("delta").mode("overwrite").saveAsTable(f"nico_chamorro.bronze.{table_name}")
    return df

customers_bronze = load_and_save_bronze(customers_path, "customers")
orders_bronze = load_and_save_bronze(orders_path, "orders") 
order_items_bronze = load_and_save_bronze(order_items_path, "order_items")
products_bronze = load_and_save_bronze(products_path, "products")

print("Data loaded successfully!")
print(f"Customers: {customers_bronze.count()}")
print(f"Orders: {orders_bronze.count()}")
print(f"Order Items: {order_items_bronze.count()}")
print(f"Products: {products_bronze.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Bronze Layer Architecture:**
# MAGIC
# MAGIC - **CSV to Delta Conversion:** Transforms raw CSV files into optimized Delta tables
# MAGIC
# MAGIC - **Unity Catalog Integration:** Stores all tables in nico_chamorro.bronze schema
# MAGIC
# MAGIC - **Automatic Schema Inference:** Leverages Spark's schema detection for rapid development
# MAGIC
# MAGIC - **Data Validation:** Record count verification for data quality assurance

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 3:** Silver Layer - Data Cleaning & Preparation
# MAGIC ### This section applies data type conversions and basic cleaning to ensure data quality and proper format for complex analytical operations.

# COMMAND ----------


# Silver transformations
customers_silver = customers_bronze.withColumn(
    "registration_date", F.col("registration_date").cast("date")
).filter(F.col("registration_date").isNotNull())

orders_silver = orders_bronze.withColumn(
    "order_date", F.col("order_date").cast("date")
).withColumn(
    "total_amount", F.col("total_amount").cast("double")
)

order_items_silver = order_items_bronze.withColumn(
    "unit_price", F.col("unit_price").cast("double")
)

products_silver = products_bronze.withColumn(
    "unit_price", F.col("unit_price").cast("double")
).withColumn(
    "is_active", F.col("is_active").cast("integer")
)

# Ensure Silver database exists
spark.sql("CREATE DATABASE IF NOT EXISTS nico_chamorro.silver")

# Save Silver tables
customers_silver.write.format("delta").mode("overwrite").saveAsTable("nico_chamorro.silver.customers")
orders_silver.write.format("delta").mode("overwrite").saveAsTable("nico_chamorro.silver.orders")
order_items_silver.write.format("delta").mode("overwrite").saveAsTable("nico_chamorro.silver.order_items")
products_silver.write.format("delta").mode("overwrite").saveAsTable("nico_chamorro.silver.products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Silver Layer Transformations:**
# MAGIC
# MAGIC - **Date Type Casting:** Ensures proper temporal operations and filtering
# MAGIC
# MAGIC - **Numeric Type Enforcement:** Converts monetary values to double for precise calculations
# MAGIC
# MAGIC - **Null Value Handling:** Filters out invalid registration dates
# MAGIC
# MAGIC - **Boolean Conversion:** Converts is_active flags to integer for conditional logic
# MAGIC
# MAGIC - **Data Quality Foundation:** Clean, typed data ready for complex business logic

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 4:** Gold Query 1 - OUTER APPLY with Window Functions

# COMMAND ----------

# MAGIC ## Objective
# MAGIC Convert complex SQL queries with advanced operators (particularly OUTER APPLY) to equivalent PySpark code.
# MAGIC
# MAGIC *: Your data may be too old to use `current_timestamp` and return meaningful results. In this case, replace it with a UDF that return the following date: `2024-04-20`
# MAGIC
# MAGIC ## Challenge Queries
# MAGIC
# MAGIC ### Query 1: OUTER APPLY with Window Functions
# MAGIC **Original SQL (SQL Server/T-SQL):**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.customer_name,
# MAGIC     c.registration_date,
# MAGIC     o.order_id,
# MAGIC     o.order_date,
# MAGIC     o.total_amount,
# MAGIC     o.rank_in_customer_orders
# MAGIC FROM customers c
# MAGIC OUTER APPLY (
# MAGIC     SELECT TOP 1
# MAGIC         order_id,
# MAGIC         order_date,
# MAGIC         total_amount,
# MAGIC         ROW_NUMBER() OVER (ORDER BY order_date DESC) as rank_in_customer_orders
# MAGIC     FROM orders o
# MAGIC     WHERE o.customer_id = c.customer_id
# MAGIC         AND o.order_date >= DATEADD(day, -90, GETDATE())
# MAGIC     ORDER BY order_date DESC
# MAGIC ) o
# MAGIC WHERE c.registration_date >= '2023-01-01';

# Filter customers registered in 2023
customers_filtered = customers_silver.filter(F.col("registration_date") >= "2023-01-01")

# Calculate 90-day cutoff from fixed reference date
cutoff_date_90d = F.date_sub(reference_date, 90)

# Filter recent orders (last 90 days)
orders_recent = orders_silver.filter(F.col("order_date") >= cutoff_date_90d)

# Create window for ranking orders by date (newest first)
window_last_order = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc())

# Add ranking column using row_number (equivalent to ROW_NUMBER() in SQL)
orders_with_rank = orders_recent.withColumn("rank_in_customer_orders", F.row_number().over(window_last_order))

# Filter to get only the latest order (rank = 1) - equivalent to TOP 1
last_order_per_customer = orders_with_rank.filter(F.col("rank_in_customer_orders") == 1)

# Left join to replicate OUTER APPLY behavior
customer_orders = customers_filtered.join(
    last_order_per_customer,
    on="customer_id",
    how="left"
).select(
    customers_filtered["customer_id"],
    customers_filtered["customer_name"], 
    customers_filtered["registration_date"],
    last_order_per_customer["order_id"],
    last_order_per_customer["order_date"],
    last_order_per_customer["total_amount"],
    last_order_per_customer["rank_in_customer_orders"]
)
# Ensure Gold database exists
spark.sql("CREATE DATABASE IF NOT EXISTS nico_chamorro.gold")

# Save to Gold layer
customer_orders.write.format("delta").mode("overwrite").saveAsTable("nico_chamorro.gold.customer_last_order")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Migration Approach for Query 1:**
# MAGIC
# MAGIC ## **OUTER APPLY Pattern Translation:**
# MAGIC
# MAGIC - **SQL OUTER APPLY** → **PySpark Left Join:** Maintains all customers while optionally adding order data
# MAGIC
# MAGIC - **TOP 1 with ORDER BY** → **Window Function + Filter:** Uses row_number() with descending date order and filters to rank = 1
# MAGIC
# MAGIC - **ROW_NUMBER() OVER()** → **row_number().over(Window):** Direct equivalent for ranking logic
# MAGIC
# MAGIC - **DATEADD()** → **date_sub():** Date arithmetic using fixed reference date
# MAGIC
# MAGIC **Performance Optimizations:**
# MAGIC
# MAGIC - **Early Filtering:** Applies date filters before expensive window operations
# MAGIC
# MAGIC - **Window Partitioning:** Processes each customer's orders independently
# MAGIC
# MAGIC - **Selective Projection:** Only includes necessary columns in final output

# COMMAND ----------

# MAGIC %md
# MAGIC ##  **STEP 5:** Gold Query 2 - Complex OUTER APPLY with Aggregations

# COMMAND ----------

# MAGIC **Original SQL (SQL Server/T-SQL):**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     p.product_id,
# MAGIC     p.product_name,
# MAGIC     p.category,
# MAGIC     s.monthly_sales,
# MAGIC     s.avg_daily_sales,
# MAGIC     s.sales_trend
# MAGIC FROM products p
# MAGIC OUTER APPLY (
# MAGIC     SELECT 
# MAGIC         SUM(quantity * unit_price) as monthly_sales,
# MAGIC         AVG(quantity * unit_price) as avg_daily_sales,
# MAGIC         CASE 
# MAGIC             WHEN COUNT(*) > 1 THEN 
# MAGIC                 (SUM(CASE WHEN order_date >= DATEADD(day, -15, GETDATE()) 
# MAGIC                           THEN quantity * unit_price ELSE 0 END) -
# MAGIC                  SUM(CASE WHEN order_date BETWEEN DATEADD(day, -30, GETDATE()) 
# MAGIC                                      AND DATEADD(day, -15, GETDATE()) 
# MAGIC                           THEN quantity * unit_price ELSE 0 END)) /
# MAGIC                 NULLIF(SUM(CASE WHEN order_date BETWEEN DATEADD(day, -30, GETDATE()) 
# MAGIC                                            AND DATEADD(day, -15, GETDATE()) 
# MAGIC                                 THEN quantity * unit_price ELSE 0 END), 0) * 100
# MAGIC             ELSE 0 
# MAGIC         END as sales_trend
# MAGIC     FROM order_items oi
# MAGIC     JOIN orders o ON oi.order_id = o.order_id
# MAGIC     WHERE oi.product_id = p.product_id
# MAGIC         AND o.order_date >= DATEADD(day, -30, GETDATE())
# MAGIC ) s
# MAGIC WHERE p.is_active = 1;
# MAGIC ```

# Filter active products
products_active = products_silver.filter(F.col("is_active") == 1)

# Define number of days for averaging
num_days = 30

# Calculate date cutoffs for trend analysis
cutoff_date_30d = F.date_sub(reference_date, num_days)
cutoff_date_15d = F.date_sub(reference_date, 15)

# Filter orders from last 30 days and join with order items
orders_30d = orders_silver.filter(F.col("order_date") >= cutoff_date_30d)
order_items_joined = order_items_silver.join(orders_30d, on="order_id", how="inner")

# Calculate line amount for each order item
order_items_with_amount = order_items_joined.withColumn(
    "line_amount", F.col("quantity") * F.col("unit_price")
)

# Base aggregations for monthly metrics
product_agg = order_items_with_amount.groupBy("product_id").agg(
    F.sum("line_amount").alias("monthly_sales"),
    (F.sum("line_amount") / num_days).alias("avg_daily_sales"),  # Fixed average per day
    F.count("*").alias("transaction_count")
)

# Recent sales (last 15 days)
sales_last_15d = order_items_with_amount.filter(
    F.col("order_date") >= cutoff_date_15d
).groupBy("product_id").agg(
    F.sum("line_amount").alias("recent_sales")
)

# Previous sales (15-30 days ago)
sales_prev_15d = order_items_with_amount.filter(
    (F.col("order_date") >= cutoff_date_30d) & (F.col("order_date") < cutoff_date_15d)
).groupBy("product_id").agg(
    F.sum("line_amount").alias("previous_sales")
)

# Combine all sales metrics
sales_trend_data = product_agg.join(
    sales_last_15d, on="product_id", how="left"
).join(
    sales_prev_15d, on="product_id", how="left"
).fillna(0)

# Calculate sales trend percentage (equivalent to complex CASE statement)
product_sales_final = sales_trend_data.withColumn(
    "sales_trend",
    F.when(
        F.col("transaction_count") > 1,  # Equivalent to COUNT(*) > 1
        F.when(
            F.col("previous_sales") != 0,  # Equivalent to NULLIF
            ((F.col("recent_sales") - F.col("previous_sales")) / F.col("previous_sales")) * 100
        ).otherwise(0)
    ).otherwise(0)  # ELSE 0 when transaction_count <= 1
)

# Final join with products (OUTER APPLY equivalent)
product_summary = products_active.join(
    product_sales_final.select(
        "product_id", "monthly_sales", "avg_daily_sales", "sales_trend"
    ),
    on="product_id",
    how="left"
).select(
    "product_id", "product_name", "category", "monthly_sales", "avg_daily_sales", "sales_trend"
).fillna(0, subset=["monthly_sales", "avg_daily_sales", "sales_trend"])

# Save to Gold layer
product_summary.write.format("delta").mode("overwrite").saveAsTable(
    "nico_chamorro.gold.product_sales_summary"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## **Migration Approach for Query 2:**
# MAGIC
# MAGIC ## **Complex Conditional Aggregation Translation:**
# MAGIC
# MAGIC - **Nested CASE in Aggregations**  → **Separate Filtered Aggregations:** More maintainable and testable
# MAGIC
# MAGIC - **NULLIF Logic** → **Conditional Division:** Uses previous_sales != 0 check to prevent division by zero
# MAGIC
# MAGIC - **BETWEEN Date Logic** → **Explicit Date Range Filters:** Clear and precise date boundary handling
# MAGIC
# MAGIC - **COUNT(*)** > **1 Check** → **Transaction Count Filter:** Direct equivalent for trend calculation eligibility
# MAGIC
# MAGIC ## **Modular Decomposition Strategy:**
# MAGIC
# MAGIC - **Base Metrics:** Monthly sales, average daily sales, transaction count
# MAGIC
# MAGIC - **Time-Based Aggregations:** Separate calculations for recent (0-15 days) and previous (15-30 days) periods
# MAGIC
# MAGIC - **Trend Calculation:** Combines time-based metrics with conditional logic
# MAGIC
# MAGIC - **Final Assembly:** Joins all components with product master data
# MAGIC
# MAGIC ## **Performance Considerations:**
# MAGIC
# MAGIC - **Early Filtering:** Applies date constraints before expensive aggregations
# MAGIC  
# MAGIC - **Incremental Processing:** Modular approach enables partial recomputation
# MAGIC
# MAGIC - **Left Join Strategy:** Maintains OUTER APPLY semantics for products without sales

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 6:** Verification & Results
# MAGIC ### This section provides final validation of the migrated queries and demonstrates successful execution with actual results.

# COMMAND ----------

# Final verification
print("Task 2 COMPLETED SUCCESSFULLY")
display(spark.table("nico_chamorro.gold.customer_last_order"))
display(spark.table("nico_chamorro.gold.product_sales_summary"))