# Databricks notebook source
# MAGIC %md
# MAGIC # Overview
# MAGIC ### Provide comprehensive analysis of foreign exchange data processed in Task 1, focusing on trends, volatility metrics, data quality, and cross-currency relationships for USD/CAD, EUR/CAD, and GBP/CAD over the last 30 days.

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 1:** Libraries & Configuration
# MAGIC ### This section imports all necessary libraries for data analysis, visualization, and statistical computations. We configure matplotlib and seaborn for professional-quality visualizations.

# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F

plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Analysis Framework:**
# MAGIC
# MAGIC - **Pandas&NumPy**:Data manipulation and numerical computations
# MAGIC
# MAGIC - **Matplotlib&Seaborn**: Professional visualization and plotting
# MAGIC
# MAGIC - **PySpark Functions:** Bridge between Spark DataFrames and pandas
# MAGIC
# MAGIC - **Visual Style:** Clean, publication-ready graphics with harmonious color palette

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 2:** Data Loading
# MAGIC ### This section loads the processed data from the ETL pipeline's Silver and Gold layers, converting Spark DataFrames to pandas for analytical operations and visualization.

# COMMAND ----------

fx_silver_spark = spark.table("nico_chamorro.silver.fx_rates")
fx_gold_spark = spark.table("nico_chamorro.gold.fx_rates_summary")

fx_df = fx_silver_spark.toPandas()
fx_summary_df = fx_gold_spark.toPandas()

fx_df['date'] = pd.to_datetime(fx_df['date'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Data Source Strategy:**
# MAGIC
# MAGIC - **Silver Layer:** Detailed time-series data with all derived columns (pct_change, volatility, trends)
# MAGIC
# MAGIC - **Gold Layer:** Aggregated summary statistics for high-level insights
# MAGIC
# MAGIC - **Data Type Conversion:** Ensures proper datetime handling for time-series analysis
# MAGIC
# MAGIC - **Memory Efficiency:** Converts to pandas only for analysis, maintaining Spark for large-scale processing

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 3:** Exploratory Data Analysis & Summary

# COMMAND ----------

print("Period:", fx_df['date'].min().date(), "to", fx_df['date'].max().date())
print("Total records:", len(fx_df))
print("Currencies:", sorted(fx_df['currency'].unique().tolist()))
print("Average data quality score:", fx_df['data_quality_score'].mean())

display(fx_summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Key Metrics Examined:**
# MAGIC
# MAGIC - **Temporal Coverage:** Date range validation for the 30-day requirement
# MAGIC
# MAGIC - **Data Completeness:** Record count verification across all currency pairs
# MAGIC
# MAGIC - **Currency Coverage:** Confirmation of all three required pairs (USD/CAD, EUR/CAD, GBP/CAD)
# MAGIC
# MAGIC - **Quality Assessment:** Overall data reliability score from the ETL pipeline
# MAGIC
# MAGIC - **Summary Statistics:** High-level metrics from the Gold layer for executive review

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 4:** FX Rate Trends Analysis
# MAGIC ### This section visualizes the historical exchange rate movements for all three currency pairs, identifying patterns, support/resistance levels, and relative performance.

# COMMAND ----------

plt.figure(figsize=(12,5))
for cur in sorted(fx_df['currency'].unique()):
    s = fx_df[fx_df['currency'] == cur].sort_values('date')
    plt.plot(s['date'], s['rate'], marker='o', label=cur, linewidth=1.8)
plt.title("FX Rate Trends (last ~30 days)")
plt.xlabel("Date")
plt.ylabel("Exchange Rate")
plt.legend()
plt.grid(alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Trend Analysis Insights:**
# MAGIC
# MAGIC - **Comparative Performance:** Visual comparison of all three currency pairs on same scale
# MAGIC
# MAGIC - **Pattern Identification:** Detection of trending markets vs. range-bound behavior
# MAGIC
# MAGIC - **Volatility Assessment:** Visual estimation of price fluctuation magnitudes
# MAGIC
# MAGIC - **Market Correlation:** Initial observation of synchronized vs. divergent movements
# MAGIC
# MAGIC - **Temporal Patterns:** Identification of any weekly or intra-month patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 5:** Daily Percentage Change Analysis
# MAGIC ### This section examines the daily volatility and performance characteristics through percentage change distributions and time-series analysis.

# COMMAND ----------

plt.figure(figsize=(10,5))
sns.boxplot(x='currency', y='pct_change', data=fx_df)
plt.title("Daily % Change Distribution by Currency")
plt.ylabel("Percent Change (%)")
plt.grid(alpha=0.3)
plt.tight_layout()
plt.show()

plt.figure(figsize=(12,4))
for cur in sorted(fx_df['currency'].unique()):
    s = fx_df[fx_df['currency'] == cur].sort_values('date')
    plt.plot(s['date'], s['pct_change'], marker='o', label=cur, linewidth=1)
plt.axhline(0, color='red', linestyle='--', alpha=0.7)
plt.title("Daily % Change Over Time")
plt.xlabel("Date")
plt.ylabel("% Change")
plt.legend()
plt.grid(alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Volatility Analysis:**
# MAGIC
# MAGIC - **Distribution Characteristics:** Box plots show volatility ranges, outliers, and central tendencies
# MAGIC
# MAGIC - **Comparative Risk:** Identification of most/least volatile currency pairs
# MAGIC
# MAGIC - **Time-Series Volatility:** Tracking daily fluctuations and identifying high-volatility periods
# MAGIC
# MAGIC - **Zero-Benchmark:** Reference line for positive/negative performance days
# MAGIC
# MAGIC - **Outlier Detection:** Extreme movements that may warrant investigation

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 6:** Rolling Metrics Analysis
# MAGIC ### This section analyzes the derived financial metrics from the ETL pipeline: 3-day moving averages for trend identification and 7-day volatility for risk assessment.

# COMMAND ----------

currencies = sorted(fx_df['currency'].unique())
fig, axes = plt.subplots(len(currencies), 2, figsize=(14, 4 * len(currencies)), sharex=True)

for i, cur in enumerate(currencies):
    s = fx_df[fx_df['currency'] == cur].sort_values('date')

    axes[i,0].plot(s['date'], s['rate'], label='Rate', linewidth=1.5)
    if 'trend_3d_ma' in s.columns:
        axes[i,0].plot(s['date'], s['trend_3d_ma'], label='3D MA', linestyle='--', linewidth=1.5)
    axes[i,0].set_title(f"{cur} - Rate & 3-day MA")
    axes[i,0].legend()
    axes[i,0].grid(alpha=0.3)

    if 'volatility_7d' in s.columns:
        axes[i,1].plot(s['date'], s['volatility_7d'], label='7-day Volatility', color='tab:orange', linewidth=1.5)
        axes[i,1].set_title(f"{cur} - 7-day Volatility")
        axes[i,1].grid(alpha=0.3)
    else:
        axes[i,1].text(0.5, 0.5, 'volatility_7d not present', ha='center')

plt.xlabel("Date")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Technical Analysis Metrics:**
# MAGIC
# MAGIC ### **3-Day Moving Average (Trend Indicator):**
# MAGIC
# MAGIC - **Trend Identification:** Smooths noise to reveal underlying direction
# MAGIC
# MAGIC - **Trading Signals:** Crossovers between price and moving average
# MAGIC
# MAGIC - **Support/Resistance:** Dynamic levels for entry/exit decisions
# MAGIC
# MAGIC - **Momentum Assessment:** Slope and acceleration of trend changes
# MAGIC
# MAGIC ### **7-Day Volatility (Risk Indicator):**
# MAGIC
# MAGIC - **Risk Measurement:** Standard deviation of recent price movement
# MAGIC
# MAGIC - **Regime Detection:** Identification of high vs. low volatility periods
# MAGIC
# MAGIC - **Risk-Adjusted Decisions:** Informs position sizing and risk management
# MAGIC
# MAGIC - **Market Stability:** Periods of consolidation vs. breakout activity

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 7:** Data Quality Assessment
# MAGIC ### This section evaluates the reliability and cleanliness of the processed data using the quality scores generated during the ETL pipeline.

# COMMAND ----------

quality_count = fx_df.groupby(['currency','quality_category']).size().unstack(fill_value=0)
display(quality_count)

quality_avg = fx_df.groupby('currency')['data_quality_score'].mean().reset_index()
plt.figure(figsize=(8,4))
plt.bar(quality_avg['currency'], quality_avg['data_quality_score'])
plt.ylim(0,1)
plt.title("Average Data Quality Score by Currency\n")
plt.ylabel("Avg Quality Score (0-1)")
for i,v in enumerate(quality_avg['data_quality_score']):
    plt.text(i, v + 0.02, f"{v:.2%}", ha='center')
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Data Quality Framework:**
# MAGIC
# MAGIC ### **Categorical Quality Distribution:**
# MAGIC
# MAGIC - **Excellent:** Clean data within statistical bounds (score = 1.0) 
# MAGIC
# MAGIC - **Good:** Minor quality issues (score ≥ 0.7)
# MAGIC
# MAGIC - **Fair:** Moderate data concerns (score ≥ 0.4)
# MAGIC
# MAGIC - **Poor:** Significant quality problems (score < 0.4)
# MAGIC
# MAGIC ### **Quality Insights:**
# MAGIC
# MAGIC - **Reliability Assessment:** Confidence level in analytical conclusions
# MAGIC
# MAGIC - **Currency Comparison:** Identification of data quality variations across pairs
# MAGIC
# MAGIC - **ETL Validation:** Verification of pipeline's data cleaning effectiveness
# MAGIC
# MAGIC - **Monitoring Baseline:** Establishment of quality benchmarks for ongoing operations

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 8:** Correlation Analysis
# MAGIC ### This section examines the relationships between different currency pairs, identifying diversification opportunities and market linkages.

# COMMAND ----------

pivot_rates = fx_df.pivot(index='date', columns='currency', values='rate')
corr = pivot_rates.corr()

plt.figure(figsize=(6,5))
sns.heatmap(corr, annot=True, cmap='coolwarm', center=0, fmt=".2f", square=True)
plt.title("Correlation Matrix - FX Rates")
plt.tight_layout()
plt.show()

print("Numeric correlations:")
print(corr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Correlation Analysis Insights:**
# MAGIC
# MAGIC ## **Interpretation Guidelines:**
# MAGIC
# MAGIC - **+1.0:** Perfect positive correlation (move identically)
# MAGIC
# MAGIC - **+0.7 to +1.0:** Strong positive relationship
# MAGIC
# MAGIC - **+0.3 to +0.7:** Moderate positive relationship
# MAGIC
# MAGIC - **-0.3 to +0.3:** Weak or no relationship
# MAGIC
# MAGIC - **-0.7 to -0.3:** Moderate negative relationship
# MAGIC
# MAGIC - **-1.0 to -0.7:** Strong negative relationship
# MAGIC
# MAGIC ## Business Applications:
# MAGIC
# MAGIC - **Portfolio Diversification:** Identify uncorrelated pairs for risk reduction
# MAGIC
# MAGIC - **Hedging Strategies:** Use negatively correlated pairs for protection
# MAGIC
# MAGIC - **Market Analysis:** Understand fundamental relationships between currencies
# MAGIC
# MAGIC - **Trading Signals:** Correlation breakdowns may indicate market regime changes

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 9:** Executive Summary & Insights
# MAGIC ### This section synthesizes all analytical findings into actionable business intelligence and strategic recommendations.

# COMMAND ----------

total_records = len(fx_df)
date_range = f"{fx_df['date'].min().date()} to {fx_df['date'].max().date()}"
avg_quality = fx_df['data_quality_score'].mean()

print("=== EXECUTIVE SUMMARY ===")
print(f"Period: {date_range}")
print(f"Total records: {total_records}")
print(f"Average data quality score: {avg_quality:.2%}\n")

print("Per-currency quick stats:")
for cur in currencies:
    s = fx_df[fx_df['currency']==cur]
    print(f"- {cur}: avg_rate={s['rate'].mean():.4f}, avg_volatility_7d={s['volatility_7d'].mean():.6f}, max_pct_change={s['pct_change'].max():+.4f}%")

print("\nRecommendations:")
print("- Monitor days with extreme pct_change and alert if > threshold")
print("- If dataset grows, consider Spark-based or sampled plotting")
print("- Push data quality metrics to monitoring in production")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Business Intelligence:
# MAGIC
# MAGIC ## Performance Highlights:
# MAGIC
# MAGIC - **Temporal Coverage:** Complete 30-day analysis period
# MAGIC
# MAGIC - **Data Reliability:** High-quality scores indicating trustworthy analytics
# MAGIC
# MAGIC - **Currency Performance:** Comparative metrics across all pairs
# MAGIC
# MAGIC - **Risk Assessment:** Volatility measurements for informed decision-making
# MAGIC
# MAGIC ## **Strategic Recommendations:**
# MAGIC ## 
# MAGIC ## **Immediate Actions:**
# MAGIC
# MAGIC - **Volatility Monitoring:** Implement alerts for extreme daily movements
# MAGIC
# MAGIC - **Quality Dashboard:** Create real-time data quality monitoring
# MAGIC
# MAGIC - **Performance Tracking:** Establish benchmarks for currency performance
# MAGIC
# MAGIC ## Scalability Planning:
# MAGIC
# MAGIC - **Big Data Ready:** Architecture prepared for increased data volume
# MAGIC
# MAGIC - **Automated Reporting:** Framework for scheduled analytical outputs
# MAGIC
# MAGIC - **Production Integration:** Seamless transition to operational environment