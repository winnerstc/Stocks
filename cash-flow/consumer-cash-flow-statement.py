# -*- coding: utf-8 -*-
# consumer-income-statement.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType
import os
import shutil

spark = SparkSession.builder \
    .appName("Kafka_stocks_Consumer_cash_flow_Local_Save") \
    .getOrCreate()

# --- Configuration ---
kafka_bootstrap = "ip-172-31-14-3.eu-west-2.compute.internal:9092"
topic = "cash-flow-statement-topic"
# Define the TEMPORARY LOCAL PATH relative to your current directory
# This directory MUST NOT exist when the job runs if mode('overwrite') is used
LOCAL_TEMP_PATH = "/home/Consultants/DE011025/stocks/cash-flow/cash_output"
FINAL_HDFS_PATH = "hdfs://ip-172-31-8-235.eu-west-2.compute.internal:9000/tmp/DE011025/stocks-data/cash-flow-statement-data"

# Clean up previous local temp directory before running
if os.path.exists(LOCAL_TEMP_PATH):
    print(f"Cleaning up previous local directory: {LOCAL_TEMP_PATH}")
    shutil.rmtree(LOCAL_TEMP_PATH)

# --- Correct Cash Flow Schema Definition ---
cash_flow_schema = StructType([
    StructField("date", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("reportedCurrency", StringType(), True),
    StructField("cik", StringType(), True),
    StructField("fillingDate", StringType(), True),
    StructField("acceptedDate", StringType(), True),
    StructField("fiscalYear", StringType(), True),
    StructField("period", StringType(), True),
    StructField("netIncome", LongType(), True),
    StructField("depreciationAndAmortization", LongType(), True),
    StructField("deferredIncomeTax", LongType(), True),
    StructField("stockBasedCompensation", LongType(), True),
    StructField("changeInWorkingCapital", LongType(), True),
    StructField("accountsReceivables", LongType(), True),
    StructField("inventory", LongType(), True),
    StructField("accountsPayables", LongType(), True),
    StructField("otherWorkingCapital", LongType(), True),
    StructField("otherNonCashItems", LongType(), True),
    StructField("netCashProvidedByOperatingActivities", LongType(), True),
    StructField("investmentsInPropertyPlantAndEquipment", LongType(), True),
    StructField("acquisitionsNet", LongType(), True),
    StructField("purchasesOfInvestments", LongType(), True),
    StructField("salesMaturitiesOfInvestments", LongType(), True),
    StructField("otherInvestingActivites", LongType(), True),
    StructField("netCashUsedForInvestingActivites", LongType(), True),
    StructField("debtRepayment", LongType(), True),
    StructField("commonStockIssued", LongType(), True),
    StructField("commonStockRepurchased", LongType(), True),
    StructField("dividendsPaid", LongType(), True),
    StructField("otherFinancingActivites", LongType(), True),
    StructField("netCashUsedProvidedByFinancingActivities", LongType(), True),
    StructField("effectOfExchangeRateChangesOnCash", LongType(), True),
    StructField("netChangeInCash", LongType(), True),
    StructField("cashAtEndOfPeriod", LongType(), True),
    StructField("cashAtBeginningOfPeriod", LongType(), True),
    StructField("operatingCashFlow", LongType(), True),
    StructField("capitalExpenditure", LongType(), True),
    StructField("freeCashFlow", LongType(), True),
    StructField("companyName", StringType(), True) # Added in producer
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
parsed_df = df.select(from_json(col("value").cast("string"), cash_flow_schema).alias("data")) \
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
