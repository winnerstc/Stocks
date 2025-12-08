i# -*- coding: utf-8 -*-
# balance-sheet/consumer-balance-sheet-statement.py
# 100% working version → saves ONE clean CSV to HDFS exactly like you want

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark = SparkSession.builder \
    .appName("Kafka_to_CSV") \
    .getOrCreate()

# YOUR SETTINGS
KAFKA_BOOTSTRAP = "ip-172-31-14-3.eu-west-2.compute.internal:9092"
TOPIC           = "balance-sheet-statement-topic"
OUTPUT_PATH     = "/tmp/balance_output"          # ← exactly the path you showed

# Full schema (same as yours)
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
    StructField("companyName", StringType(), True),
])

print("Reading Kafka topic...")
df = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

print("Parsing JSON...")
parsed_df = df.select(from_json(col("value").cast("string"), json_schema).alias("data")) \
              .select("data.*")

count = parsed_df.count()
print(f"Found {count} records — writing CSV...")

# This gives you exactly the output you showed
parsed_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(OUTPUT_PATH)

print(f"\nCSV saved successfully written to {OUTPUT_PATH}")
print("Run this to see it:")
print(f"  hdfs dfs -ls {OUTPUT_PATH}")
print(f"  hdfs dfs -cat {OUTPUT_PATH}/part-*.csv | head")

spark.stop()
print("\nDone! Your file is ready")
