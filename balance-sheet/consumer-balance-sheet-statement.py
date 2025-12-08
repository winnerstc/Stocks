# -*- coding: utf-8 -*-
# consumer-balance-sheet-statement.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType
import os
import shutil

spark = SparkSession.builder \
    .appName("Kafka_stocks_Consumer_Balance_Local_Save") \
    .getOrCreate()

# --- Configuration ---
kafka_bootstrap = "ip-172-31-14-3.eu-west-2.compute.internal:9092"
topic = "balance-sheet-statement-topic"
# Define the TEMPORARY LOCAL PATH relative to your current directory
# This directory MUST NOT exist when the job runs if mode('overwrite') is used
LOCAL_TEMP_PATH = "/home/Consultants/DE011025/stocks/balance-sheet/balance_output"
FINAL_HDFS_PATH = "hdfs://ip-172-31-8-235.eu-west-2.compute.internal:9000/tmp/DE011025/stocks-data/stocks-income-statement-data"

# Clean up previous local temp directory before running
if os.path.exists(LOCAL_TEMP_PATH):
    print(f"Cleaning up previous local directory: {LOCAL_TEMP_PATH}")
    shutil.rmtree(LOCAL_TEMP_PATH)

# --- Schema Definition ---
# *** UPDATED: Added companyName at the end to match the producer's output ***
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
    StructField("companyName", StringType(), True) # <-- NEW FIELD
])

# --- Kafka Read ---
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# --- Data Parsing ---
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

# --- Local Save Operation ---
# Use repartition(1) to ensure only ONE output file is created locally
parsed_df.repartition(1) \
    .write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(LOCAL_TEMP_PATH)

print(f"Data successfully read from Kafka and saved to local disk as CSV at {LOCAL_TEMP_PATH}")
print(f"Next step: Manually move the file to HDFS using the command below.")
print(f"Final HDFS Path: {FINAL_HDFS_PATH}")

# Stop Spark session
spark.stop()
