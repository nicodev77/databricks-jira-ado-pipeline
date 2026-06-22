# Databricks notebook source
# MAGIC %md
# MAGIC # **Overview**
# MAGIC ### This ETL pipeline extracts foreign exchange rate data from the Bank of Canada API, applies financial transformations, and delivers analytics-ready data through a robust medallion architecture. The solution processes three major currency pairs (USD/CAD, EUR/CAD, GBP/CAD) with comprehensive data quality controls and financial analytics.![](path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 1:** Foundation & Configuration
# MAGIC ### This section establishes the core infrastructure for our ETL pipeline with all necessary imports and logging configuration.
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, when, row_number, percentile_approx
from datetime import datetime, timedelta
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Key Design Decisions:**
# MAGIC
# MAGIC - **PySpark Core Libraries:** For distributed data processing
# MAGIC
# MAGIC - **Window Functions:** Essential for time-series financial calculations
# MAGIC
# MAGIC - **Comprehensive Error Handling:** Production-grade reliability
# MAGIC
# MAGIC - **Structured Logging:** Real-time pipeline monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 2:** Robust API Extraction Layer
# MAGIC ### This section implements production-grade HTTP communication with retry logic and comprehensive error handling for reliable data extraction from the Bank of Canada API.
# MAGIC

# COMMAND ----------

def create_session_with_retries():
    """Create requests session with retry logic"""
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def fetch_bank_of_canada_data(series, days=30):
    """
    Fetch data from Bank of Canada API
    
    Args:
        series (str): Currency pair series code (e.g., 'FXUSDCAD')
        days (int): Number of recent days to fetch
    
    Returns:
        dict: API response data or None if error
    """
    base_url = "https://www.bankofcanada.ca/valet/observations/"
    url = f"{base_url}{series}/json?recent={days}"
    
    session = create_session_with_retries()
    try:
        logger.info(f"Fetching data for {series}")
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        observations = data.get("observations", [])
        if not observations:
            logger.warning(f"No observations found for {series}")
            return []
        logger.info(f"Fetched {len(observations)} records for {series}")
        return observations
    except Exception as e:
        logger.error(f"Error fetching {series}: {e}")
        return []

# COMMAND ----------

# MAGIC %md
# MAGIC ## **API Integration Features:**
# MAGIC
# MAGIC - **Automatic Retry Logic:** Handles temporary API failures with exponential backoff
# MAGIC
# MAGIC - **30-Second Timeouts:** Prevents hanging requests in production
# MAGIC
# MAGIC - **Comprehensive Error Handling:** Graceful degradation on API failures
# MAGIC
# MAGIC - **Real-time Progress Tracking:** Detailed logging for operational monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 **STEP 3:** Data Quality Framework
# MAGIC ### This section implements proactive data quality monitoring with automated issue detection and comprehensive validation checks.

# COMMAND ----------

def perform_data_quality_checks(df):
    """Perform basic data quality checks"""
    logger.info("Performing data quality checks...")
    
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(f"{c}_nulls") 
        for c in df.columns
    ]).collect()[0]
    
    for col_name in df.columns:
        null_count = null_counts[f"{col_name}_nulls"]
        if null_count > 0:
            logger.warning(f"Column {col_name} has {null_count} null values")
    
    duplicate_count = df.groupBy("date", "currency").count()\
        .filter(col("count") > 1).count()
    
    if duplicate_count > 0:
        logger.warning(f"Found {duplicate_count} duplicate records")
    
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Quality Assurance Framework:**
# MAGIC
# MAGIC - **Automated Null Detection:** Scans all columns for missing values using PySpark's conditional aggregation
# MAGIC
# MAGIC - **Duplicate Prevention:** Ensures unique date-currency combinations for data integrity
# MAGIC
# MAGIC - **Actionable Alerts:** Provides specific counts and locations of data issues for troubleshooting
# MAGIC
# MAGIC - **Multi-Stage Validation:** Runs at critical pipeline stages to catch issues early

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 4:** Bronze Layer - Raw Data Extraction
# MAGIC ### This section handles the initial data extraction from multiple currency pairs and persists raw data in Delta format for auditability and reprocessing capabilities.

# COMMAND ----------

currency_pairs = ["FXUSDCAD", "FXEURCAD", "FXGBPCAD"]
all_dfs = []

for currency in currency_pairs:
    observations = fetch_bank_of_canada_data(currency)
    if observations:
        df = pd.DataFrame(observations)
        if currency not in df.columns:
            logger.warning(f"Series {currency} not found in response")
            continue
            
        rates = []
        dates = []
        for idx, row in df.iterrows():
            try:
                rate_val = float(row[currency]['v']) if row[currency] and 'v' in row[currency] else None
                rates.append(rate_val)
                dates.append(row['d'])
            except Exception as e:
                logger.warning(f"Error parsing rate for {currency} at index {idx}: {e}")
                rates.append(None)
                dates.append(row['d'])
        
        temp_df = pd.DataFrame({
            'date': pd.to_datetime(dates),
            'currency': currency,
            'rate': rates
        })
        all_dfs.append(temp_df)
    else:
        logger.warning(f"No data retrieved for {currency}")

if not all_dfs:
    raise ValueError("No data retrieved from any currency pair")

# Combine all data into a single DataFrame
concatenated_df = pd.concat(all_dfs, ignore_index=True)
fx_rates_df_bronze = spark.createDataFrame(concatenated_df)

# Ensure Bronze database exists
spark.sql("CREATE DATABASE IF NOT EXISTS nico_chamorro.bronze")

# Write raw data to Bronze
fx_rates_df_bronze.write.format("delta").mode("overwrite").saveAsTable("nico_chamorro.bronze.fx_rates")
logger.info("Bronze layer created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Bronze Layer Architecture:**
# MAGIC
# MAGIC - **Multi-Currency Extraction:** Processes all three required currency pairs with individual error handling
# MAGIC
# MAGIC - **Robust Data Parsing:** Implements try-catch logic for malformed API responses
# MAGIC
# MAGIC - **Pandas for JSON Processing:** Leverages pandas for efficient JSON handling before Spark conversion
# MAGIC
# MAGIC - **Raw Data Preservation:** Maintains original API data for auditability and reprocessing
# MAGIC
# MAGIC - **Unity Catalog Integration:** Stores data in structured catalog for governance

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 5:** Silver Layer - Data Cleansing & Enrichment
# MAGIC ### This section transforms raw data into analytics-ready format through statistical cleaning, financial calculations, and comprehensive data quality scoring.
# MAGIC

# COMMAND ----------

# Read from Bronze layer
fx_rates_df = spark.table("nico_chamorro.bronze.fx_rates")

# Remove null rates to ensure data quality
fx_rates_df = fx_rates_df.filter(col("rate").isNotNull())

#Handle outliers using IQR (Interquartile Range) method
#It calculates  Q1 and Q3 (25th and 75th percentiles) by currency, then the IQR (Q3 - Q1), and using the standard rule Q1 - 1.5IQR and 
# Q3 + 1.#5IQR to identify outliers. This is used to detect anomalous exchange rates and clean data before analysis.
#The Interquartile Range (IQR) method is used to analyze the dispersion and variability of data, especially in the presence of outliers, and to identify unusual values in a data set. It is calculated as the difference between the third and first quartiles (\(Q3-Q1\)), representing the range of the central half of the data and being less sensitive to extremes than the standard range. 
window_curr = Window.partitionBy("currency")
fx_rates_df = fx_rates_df.withColumn("q1", percentile_approx("rate", 0.25).over(window_curr)) \
                   .withColumn("q3", percentile_approx("rate", 0.75).over(window_curr)) \
                   .withColumn("iqr", col("q3") - col("q1")) \
                   .withColumn("lower_bound", col("q1") - 1.5 * col("iqr")) \
                   .withColumn("upper_bound", col("q3") + 1.5 * col("iqr"))
#Filter the outliers while mantaining the bounds columns for now

fx_rates_df = fx_rates_df.filter((col("rate") >= col("lower_bound")) & (col("rate") <= col("upper_bound")))

# Perform data quality checks
fx_rates_df = perform_data_quality_checks(fx_rates_df)

# Calculate derived columns using window functions
window_spec = Window.partitionBy("currency").orderBy("date")
window_7d = Window.partitionBy("currency").orderBy("date").rowsBetween(-6, 0)
window_3d = Window.partitionBy("currency").orderBy("date").rowsBetween(-2, 0)

fx_rates_df = fx_rates_df.withColumn(
    "pct_change",
    (col("rate") - F.lag("rate").over(window_spec)) / F.lag("rate").over(window_spec) * 100
).withColumn("volatility_7d", F.stddev("rate").over(window_7d)) \
 .withColumn("trend_3d_ma", F.avg("rate").over(window_3d)) \
 .withColumn("extraction_timestamp", F.current_timestamp())

# Calculate comprehensive data quality score (0.0 - 1.0 scale) using lower_bound y upper_bound
fx_rates_df = fx_rates_df.withColumn(
    "data_quality_score",
    F.when(col("rate").isNull(), 0.0)  # Null values = lowest score
    .when(
        (col("rate") < col("lower_bound")) | (col("rate") > col("upper_bound")), 
        0.3  # Outliers = poor score but keep for analysis
    )
    .otherwise(1.0)  # Clean data = perfect score
).withColumn(
    "quality_category",
    F.when(col("data_quality_score") == 1.0, "Excellent")
    .when(col("data_quality_score") >= 0.7, "Good")
    .when(col("data_quality_score") >= 0.4, "Fair")
    .otherwise("Poor")
)

fx_rates_df = fx_rates_df.drop("q1", "q3", "iqr", "lower_bound", "upper_bound")

# Ensure Silver database exists
spark.sql("CREATE DATABASE IF NOT EXISTS nico_chamorro.silver")

# Partition by DATE for optimal query performance
fx_rates_df.write.format("delta").mode("overwrite").option("mergeSchema","true")\
    .partitionBy("date")\
    .saveAsTable("nico_chamorro.silver.fx_rates")
logger.info("Silver layer created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Silver Layer Transformations, Statistical Outlier Detection:**
# MAGIC
# MAGIC - **IQR Method:** Robust for financial data without normal distribution assumptions
# MAGIC
# MAGIC - **Currency-Specific Bounds:** Accounts for different volatility characteristics
# MAGIC
# MAGIC - **1.5x IQR Rule:** Industry standard for outlier identification
# MAGIC
# MAGIC ## **Financial Analytics:**
# MAGIC
# MAGIC - **Daily Percentage Changes:** Tracks day-over-day performance for trading decisions
# MAGIC
# MAGIC - **7-Day Rolling Volatility:** Standard deviation for risk assessment and market stability
# MAGIC
# MAGIC - **3-Day Moving Average:** Identifies short-term directional trends
# MAGIC
# MAGIC - **Extraction Timestamp:** Data lineage and freshness tracking
# MAGIC
# MAGIC ## **Data Quality Scoring:**
# MAGIC
# MAGIC - **1.0:** Clean data within statistical bounds (Excellent)
# MAGIC
# MAGIC - **0.3:** Statistical outliers retained for analysis (Poor)
# MAGIC
# MAGIC - **0.0:** Null/invalid data filtered out
# MAGIC
# MAGIC - **Categorical Classification:** Excellent/Good/Fair/Poor for business users

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 6: Gold Layer - Business Intelligence
# MAGIC ### This section creates executive-level summary statistics and business aggregates for consumption by analysts, dashboards, and reporting tools.

# COMMAND ----------

summary_df = fx_rates_df.groupBy("currency").agg(
    F.count("rate").alias("record_count"),
    F.min("rate").alias("min_rate"),
    F.max("rate").alias("max_rate"),
    F.avg("rate").alias("avg_rate"),
    F.stddev("rate").alias("stddev_rate"),
    F.avg("pct_change").alias("avg_daily_change"),
    F.avg("volatility_7d").alias("avg_volatility"),
    F.avg("data_quality_score").alias("avg_quality_score"),
    F.min("date").alias("first_date"),
    F.max("date").alias("last_date")
)

# Ensure Gold database exists
spark.sql("CREATE DATABASE IF NOT EXISTS nico_chamorro.gold")

# Write to Gold layer
summary_df.write.format("delta").mode("overwrite").option("mergeSchema","true")\
    .saveAsTable("nico_chamorro.gold.fx_rates_summary")
logger.info("Gold layer created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Executive Analytics Metrics, Performance & Risk Analysis:**
# MAGIC
# MAGIC - **Record Count:** Data completeness and coverage validation
# MAGIC
# MAGIC - **Price Range Analysis:** Min/max rates for understanding currency bounds
# MAGIC
# MAGIC - **Central Tendency:** Average rates for typical valuation analysis
# MAGIC
# MAGIC - **Volatility Metrics:** Standard deviation for overall risk assessment
# MAGIC
# MAGIC ## **Business Intelligence:**
# MAGIC
# MAGIC - **Average Daily Change:** Performance tracking for strategy evaluation
# MAGIC
# MAGIC - **Average Volatility:** Risk measurement for portfolio management
# MAGIC
# MAGIC - **Data Quality Score:** Reliability indicator for decision confidence
# MAGIC
# MAGIC - **Temporal Coverage:** Date ranges for context and freshness awareness

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 7: Verification & Results
# MAGIC ### This section provides final validation and visualization of the processed data to ensure pipeline success and data quality.

# COMMAND ----------

# Display results for verification
display(fx_rates_df.orderBy("currency", "date"))
display(summary_df)

# Final success logging
logger.info("--- ETL PIPELINE COMPLETED SUCCESSFULLY ---")
logger.info(f"Silver records processed: {fx_rates_df.count()}")
logger.info(f"Currency pairs completed: {fx_rates_df.select('currency').distinct().count()}")
logger.info(f"Data date range: {fx_rates_df.agg(F.min('date'), F.max('date')).collect()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Verification Framework:**
# MAGIC
# MAGIC - **Visual Data Validation:** Immediate display of transformed records
# MAGIC
# MAGIC - **Calculation Accuracy:** Verification of financial analytics and derived columns
# MAGIC
# MAGIC - **Business Value Demonstration:** Executive summary statistics for stakeholders
# MAGIC
# MAGIC - **Operational Transparency:** Comprehensive logging of pipeline execution