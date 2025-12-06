from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round, udf, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import sys
import os
import requests
import time
from requests.exceptions import RequestException

# =================================================================
# 1. Configuration and UDF Setup
# =================================================================

# Define Configuration
DATABASE_NAME = 'alandb'
TABLE_NAME = 'income_statement'
METASTORE_URI = "thrift://18.134.163.221:9083"
API_KEY = "tiFaviFGi3xdigG3dp7OT7cHgnA0OBmu"
BASE_API_URL = "https://financialmodelingprep.com/stable/profile"

# Define Output Paths
LOCAL_OUTPUT_DIR = "./temp_annual_financial_marketcap_output"
FINAL_CSV_FILE = "./annual_financial_summary_with_marketcap.csv"


# =================================================================
# FIXED UDF with retry logic and StringType for market cap
# =================================================================
def fetch_market_data(symbol: str):
    """Enhanced UDF with retry logic, debug logging, and StringType safety."""
    if not symbol:
        return (None, None)

    for attempt in range(3):
        try:
            url = f"{BASE_API_URL}?symbol={symbol}&apikey={API_KEY}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data and isinstance(data, list) and len(data) > 0:
                mc = data[0].get("marketCap")
                price = data[0].get("price")

                # DEBUG: Print raw values for AAPL specifically
                if symbol == "AAPL":
                    print(f"DEBUG AAPL: raw mc={mc}, raw price={price}, data keys={list(data[0].keys())}")

                # Return market cap as STRING to avoid ALL numeric overflow issues
                return (str(mc) if mc is not None else None,
                        float(price) if price is not None else None)
            else:
                print(f"DEBUG {symbol}: Empty API response (len={len(data if data else [])})")
                return (None, None)

        except RequestException as e:
            print(f"WARN {symbol} attempt {attempt + 1}: Network error {e}")
        except Exception as e:
            print(f"WARN {symbol} attempt {attempt + 1}: {e}")

        if attempt < 2:
            time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s

    print(f"FAILED {symbol} after 3 attempts")
    return (None, None)


# Use StringType for market cap (handles ANY size), DoubleType for price
market_schema = StructType([
    StructField("current_market_cap", StringType(), True),
    StructField("current_price", DoubleType(), True),
])

market_data_udf = udf(fetch_market_data, market_schema)

# =================================================================
# 2. Spark Job Execution
# =================================================================
try:
    spark = SparkSession.builder \
        .appName("AnnualFinancialSummaryAPI_Fixed") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", METASTORE_URI) \
        .enableHiveSupport() \
        .getOrCreate()

    print(f"Spark session created, connected to metastore: {METASTORE_URI}")

    # Load the income statement table
    full_table_name = f"{DATABASE_NAME}.{TABLE_NAME}"
    df = spark.table(full_table_name)
    print(f"Successfully loaded table: {full_table_name}")

    # --- Step 1: Perform Aggregation ---
    annual_summary_df = df.groupBy("symbol", "fiscalyear") \
        .agg(
        round(sum("revenue") / 1e9, 2).alias("Total_Revenue_Billions_USD"),
        round(sum("netincome") / 1e9, 2).alias("Total_NetIncome_Billions_USD")
    ) \
        .orderBy(col("symbol").asc(), col("fiscalyear").desc())

    print(f"Aggregated data for {annual_summary_df.count()} symbol-year combos")

    # --- Step 2: Enrich with Market Cap API Data ---
    print("\n🚀 Starting API enrichment: Fetching Market Cap for each distinct symbol...")
    annual_summary_df.cache()

    # 2a. Get distinct symbols
    distinct_symbols_df = annual_summary_df.select("symbol").distinct()
    print(f"Found {distinct_symbols_df.count()} distinct symbols to enrich")

    # 2b. Apply UDF and DEBUG raw results
    market_data_df = distinct_symbols_df.select(
        "symbol",
        market_data_udf(col("symbol")).alias("market_data")
    ).select(
        "symbol",
        col("market_data.current_market_cap").alias("current_market_cap"),
        col("market_data.current_price").alias("current_price")
    )

    # DEBUG: Show raw API results BEFORE join
    print("\n=== DEBUG: Raw market data per symbol ===")
    market_data_df.orderBy("symbol").show(truncate=False)

    print("\n=== DEBUG: Success rates ===")
    market_data_df.groupBy("symbol").agg(
        sum(col("current_market_cap").isNotNull().cast("int")).alias("mc_success"),
        sum(col("current_price").isNotNull().cast("int")).alias("price_success")
    ).orderBy("symbol").show(truncate=False)

    # 2c. Join the aggregated data with the market cap data
    final_df = annual_summary_df.join(market_data_df, on="symbol", how="left")

    # --- Step 3: Output Results ---
    print("\n✅ Final Annual Financial Summary (Aggregated, Sorted, & Enriched):")
    final_df.show(n=20, truncate=False)

    # Quick AAPL check
    print("\n=== AAPL-specific check ===")
    final_df.filter(col("symbol") == "AAPL").select("symbol", "fiscalyear", "current_market_cap", "current_price").show(
        truncate=False)

    # Save to local file system
    print(f"\n💾 Saving final data to: {LOCAL_OUTPUT_DIR}")
    final_df.coalesce(1).write \
        .mode("overwrite") \
        .option("nullValue", "null") \
        .option("emptyValue", "") \
        .csv(LOCAL_OUTPUT_DIR, header=True)
    print("✅ Temporary files saved. Consolidate with shell commands below.")

    # Clean up
    annual_summary_df.unpersist()

except Exception as e:
    print(f"❌ Error occurred: {e}")
    import traceback

    traceback.print_exc()
    sys.exit(1)

finally:
    if 'spark' in locals() and spark:
        spark.stop()
        print("\n🔌 Spark session stopped.")

    print("\n" + "=" * 60)
    print("*** NEXT STEPS (run these shell commands): ***")
    print(f"cat {LOCAL_OUTPUT_DIR}/part-*.csv > {FINAL_CSV_FILE}")
    print(f"rm -r {LOCAL_OUTPUT_DIR}")
    print("=" * 60)

