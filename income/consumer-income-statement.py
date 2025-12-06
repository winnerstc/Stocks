# -*- coding: utf-8 -*-
# consumer-income-statement.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
import os
import shutil

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Kafka_stocks_Consumer_Local_Save") \
    .getOrCreate()

# --- Configuration ---
kafka_bootstrap = "ip-172-31-14-3.eu-west-2.compute.internal:9092"
topic = "stocks-income-statement-topic"
# Define the TEMPORARY LOCAL PATH relative to your current directory
# This directory MUST NOT exist when the job runs if mode('overwrite') is used
LOCAL_TEMP_PATH = "/home/Consultants/DE011025/stocks/income/income_output"
FINAL_HDFS_PATH = "hdfs://ip-172-31-8-235.eu-west-2.compute.internal:9000/tmp/DE011025/stocks-data/stocks-income-statement-data"

# Clean up previous local temp directory before running
if os.path.exists(LOCAL_TEMP_PATH):
    print(f"Cleaning up previous local directory: {LOCAL_TEMP_PATH}")
    shutil.rmtree(LOCAL_TEMP_PATH)

# --- Schema Definition ---
# Renamed variable for better consistency and clarity
income_statement_schema = StructType([
    StructField("date", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("reportedCurrency", StringType(), True),
    StructField("cik", StringType(), True),
    StructField("filingDate", StringType(), True),
    StructField("acceptedDate", StringType(), True),
    StructField("fiscalYear", StringType(), True),
    StructField("period", StringType(), True),
    StructField("revenue", LongType(), True),
    StructField("costOfRevenue", LongType(), True),
    StructField("grossProfit", LongType(), True),
    StructField("researchAndDevelopmentExpenses", LongType(), True),
    StructField("generalAndAdministrativeExpenses", LongType(), True),
    StructField("sellingAndMarketingExpenses", LongType(), True),
    StructField("sellingGeneralAndAdministrativeExpenses", LongType(), True),
    StructField("otherExpenses", LongType(), True),
    StructField("operatingExpenses", LongType(), True),
    StructField("costAndExpenses", LongType(), True),
    StructField("netInterestIncome", LongType(), True),
    StructField("interestIncome", LongType(), True),
    StructField("interestExpense", LongType(), True),
    StructField("depreciationAndAmortization", LongType(), True),
    StructField("ebitda", LongType(), True),
    StructField("ebit", LongType(), True),
    StructField("nonOperatingIncomeExcludingInterest", LongType(), True),
    StructField("operatingIncome", LongType(), True),
    StructField("totalOtherIncomeExpensesNet", LongType(), True),
    StructField("incomeBeforeTax", LongType(), True),
    StructField("incomeTaxExpense", LongType(), True),
    StructField("netIncomeFromContinuingOperations", LongType(), True),
    StructField("netIncomeFromDiscontinuedOperations", LongType(), True),
    StructField("otherAdjustmentsToNetIncome", LongType(), True),
    StructField("netIncome", LongType(), True),
    StructField("netIncomeDeductions", LongType(), True),
    StructField("bottomLineNetIncome", LongType(), True),
    StructField("eps", DoubleType(), True),
    StructField("epsDiluted", DoubleType(), True),
    StructField("weightedAverageShsOut", LongType(), True),
    StructField("weightedAverageShsOutDil", LongType(), True),
    StructField("companyName", StringType(), True)
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
# Use the renamed schema variable here
parsed_df = df.select(from_json(col("value").cast("string"), income_statement_schema).alias("data")) \
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
