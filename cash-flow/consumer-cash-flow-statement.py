# -*- coding: utf-8 -*-
# consumer-cash-flow-statement.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType

# ---------------------------
# --- YOUR SETTINGS ---
# ---------------------------
KAFKA_BOOTSTRAP = "ip-172-31-14-3.eu-west-2.compute.internal:9092"
TOPIC = "cash-flow-statement-topic"
OUTPUT_PATH = "/tmp/cash_flow_output"

# ---------------------------
# --- Spark Session ---
# ---------------------------
spark = SparkSession.builder.appName("Kafka_Cash_Flow_Consumer").getOrCreate()

# ---------------------------
# --- Schema Definition ---
# ---------------------------
cash_flow_schema = StructType([
    StructField("date", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("reportedCurrency", StringType(), True),
    StructField("cik", StringType(), True),
    StructField("filingDate", StringType(), True),
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
    StructField("companyName", StringType(), True)
])

# ---------------------------
# --- Kafka Read ---
# ---------------------------
df = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# ---------------------------
# --- Data Parsing ---
# ---------------------------
parsed_df = df.select(from_json(col("value").cast("string"), cash_flow_schema).alias("data")) \
              .select("data.*")

# ---------------------------
# --- Data Validation ---
# ---------------------------
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

# ---------------------------
# --- Write CSV (single file) ---
# ---------------------------
parsed_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(OUTPUT_PATH)

print(f"\nCSV saved successfully written to {OUTPUT_PATH}")
print("Run this to see it:")
print(f"  hdfs dfs -ls {OUTPUT_PATH}")
print(f"  hdfs dfs -cat {OUTPUT_PATH}/part-*.csv | head")

spark.stop()
print("\nDone! Your file is ready")

