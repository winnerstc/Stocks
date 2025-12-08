# -*- coding: utf-8 -*-
# consumer-balance-sheet-statement.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType

spark = SparkSession.builder \
    .appName("Kafka_stocks_Consumer_Balance_Local_Save") \
    .enableHiveSupport() \
    .getOrCreate()

# --- Configuration ---
kafka_bootstrap = "ip-172-31-14-3.eu-west-2.compute.internal:9092"
topic = "balance-sheet-statement-topic"
HIVE_DATABASE = "alandb"
HIVE_TABLE = "balance_sheet"

# --- Schema Definition ---
json_schema = StructType([
    StructField("date", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("reportedCurrency", StringType(), True),
    StructField("cik", StringType(), True),
    StructField("filingDate", StringType(), True),
    StructField("acceptedDate", StringType(), True),
    StructField("fiscalYear", StringType(), True),
    StructField("period", StringType(), True),
    StructField("cashAndCashEquivalents", LongType(), True),
    StructField("shortTermInvestments", LongType(), True),
    StructField("cashAndShortTermInvestments", LongType(), True),
    StructField("netReceivables", LongType(), True),
    StructField("accountsReceivables", LongType(), True),
    StructField("otherReceivables", LongType(), True),
    StructField("inventory", LongType(), True),
    StructField("prepaids", LongType(), True),
    StructField("otherCurrentAssets", LongType(), True),
    StructField("totalCurrentAssets", LongType(), True),
    StructField("propertyPlantEquipmentNet", LongType(), True),
    StructField("goodwill", LongType(), True),
    StructField("intangibleAssets", LongType(), True),
    StructField("goodwillAndIntangibleAssets", LongType(), True),
    StructField("longTermInvestments", LongType(), True),
    StructField("taxAssets", LongType(), True),
    StructField("otherNonCurrentAssets", LongType(), True),
    StructField("totalNonCurrentAssets", LongType(), True),
    StructField("otherAssets", LongType(), True),
    StructField("totalAssets", LongType(), True),
    StructField("totalPayables", LongType(), True),
    StructField("accountPayables", LongType(), True),
    StructField("otherPayables", LongType(), True),
    StructField("accruedExpenses", LongType(), True),
    StructField("shortTermDebt", LongType(), True),
    StructField("capitalLeaseObligationsCurrent", LongType(), True),
    StructField("taxPayables", LongType(), True),
    StructField("deferredRevenue", LongType(), True),
    StructField("otherCurrentLiabilities", LongType(), True),
    StructField("totalCurrentLiabilities", LongType(), True),
    StructField("longTermDebt", LongType(), True),
    StructField("deferredRevenueNonCurrent", LongType(), True),
    StructField("deferredTaxLiabilitiesNonCurrent", LongType(), True),
    StructField("otherNonCurrentLiabilities", LongType(), True),
    StructField("totalNonCurrentLiabilities", LongType(), True),
    StructField("otherLiabilities", LongType(), True),
    StructField("capitalLeaseObligations", LongType(), True),
    StructField("totalLiabilities", LongType(), True),
    StructField("treasuryStock", LongType(), True),
    StructField("preferredStock", LongType(), True),
    StructField("commonStock", LongType(), True),
    StructField("retainedEarnings", LongType(), True),
    StructField("additionalPaidInCapital", LongType(), True),
    StructField("accumulatedOtherComprehensiveIncomeLoss", LongType(), True),
    StructField("otherTotalStockholdersEquity", LongType(), True),
    StructField("totalStockholdersEquity", LongType(), True),
    StructField("totalEquity", LongType(), True),
    StructField("minorityInterest", LongType(), True),
    StructField("totalLiabilitiesAndTotalEquity", LongType(), True),
    StructField("totalInvestments", LongType(), True),
    StructField("totalDebt", LongType(), True),
    StructField("netDebt", LongType(), True),
    StructField("companyName", StringType(), True)
])

# --- Kafka Read ---
print("Reading from Kafka topic...")
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# --- Data Parsing ---
print("Parsing JSON data...")
parsed_df = df.select(from_json(col("value").cast("string"), json_schema).alias("data")) \
    .select("data.*")

########################################
## Data Validation and Print Counts ##
########################################

try:
    row_count = parsed_df.count()
    column_count = len(parsed_df.columns)

    print("#############################")
    print(f"--- DataFrame Dimensions ---")
    print(f"Total Rows: **{row_count}**")
    print(f"Total Columns: **{column_count}**")
    print("#############################")

except Exception as e:
    print(f"Error during count operation: {e}")

# --- Ensure Database Exists ---
spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE}")
print(f"✓ Database {HIVE_DATABASE} ready")

# --- Check if Table Exists ---
table_exists = spark.catalog.tableExists(f"{HIVE_DATABASE}.{HIVE_TABLE}")

if not table_exists:
    print(f"Table {HIVE_DATABASE}.{HIVE_TABLE} does not exist. Creating...")
    
    # Create managed table with CSV format
    parsed_df.write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .saveAsTable(f"{HIVE_DATABASE}.{HIVE_TABLE}")
    
    print(f"✓ Table created: {HIVE_DATABASE}.{HIVE_TABLE}")
else:
    print(f"Table {HIVE_DATABASE}.{HIVE_TABLE} exists. Appending data...")
    
    # Append to existing table
    parsed_df.write \
        .format("csv") \
        .option("header", "true") \
        .mode("append") \
        .saveAsTable(f"{HIVE_DATABASE}.{HIVE_TABLE}")
    
    print(f"✓ Data appended to: {HIVE_DATABASE}.{HIVE_TABLE}")

print(f"\nYou can now query in Hive/Hue with:")
print(f"  SELECT COUNT(*) FROM {HIVE_DATABASE}.{HIVE_TABLE};")
print(f"  SELECT * FROM {HIVE_DATABASE}.{HIVE_TABLE} LIMIT 10;")

# Stop Spark session
spark.stop()
print("\n✓ Pipeline complete!")
