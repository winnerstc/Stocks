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
OUTPUT_PATH = "/user/jenkins/balance_sheet_data"  # Jenkins home directory in HDFS

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

# --- Save to HDFS (Jenkins has permissions here) ---
print(f"Writing data to HDFS: {OUTPUT_PATH}")

try:
    # Write as CSV to HDFS location jenkins can access
    parsed_df.write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(OUTPUT_PATH)
    
    print(f"✓ Data successfully written to: {OUTPUT_PATH}")
    print(f"\nTo load into Hive later, run in Hue/Beeline:")
    print(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS alandb.balance_sheet (
        date STRING,
        symbol STRING,
        reportedCurrency STRING,
        cik STRING,
        filingDate STRING,
        acceptedDate STRING,
        fiscalYear STRING,
        period STRING,
        cashAndCashEquivalents BIGINT,
        shortTermInvestments BIGINT,
        cashAndShortTermInvestments BIGINT,
        netReceivables BIGINT,
        accountsReceivables BIGINT,
        otherReceivables BIGINT,
        inventory BIGINT,
        prepaids BIGINT,
        otherCurrentAssets BIGINT,
        totalCurrentAssets BIGINT,
        propertyPlantEquipmentNet BIGINT,
        goodwill BIGINT,
        intangibleAssets BIGINT,
        goodwillAndIntangibleAssets BIGINT,
        longTermInvestments BIGINT,
        taxAssets BIGINT,
        otherNonCurrentAssets BIGINT,
        totalNonCurrentAssets BIGINT,
        otherAssets BIGINT,
        totalAssets BIGINT,
        totalPayables BIGINT,
        accountPayables BIGINT,
        otherPayables BIGINT,
        accruedExpenses BIGINT,
        shortTermDebt BIGINT,
        capitalLeaseObligationsCurrent BIGINT,
        taxPayables BIGINT,
        deferredRevenue BIGINT,
        otherCurrentLiabilities BIGINT,
        totalCurrentLiabilities BIGINT,
        longTermDebt BIGINT,
        deferredRevenueNonCurrent BIGINT,
        deferredTaxLiabilitiesNonCurrent BIGINT,
        otherNonCurrentLiabilities BIGINT,
        totalNonCurrentLiabilities BIGINT,
        otherLiabilities BIGINT,
        capitalLeaseObligations BIGINT,
        totalLiabilities BIGINT,
        treasuryStock BIGINT,
        preferredStock BIGINT,
        commonStock BIGINT,
        retainedEarnings BIGINT,
        additionalPaidInCapital BIGINT,
        accumulatedOtherComprehensiveIncomeLoss BIGINT,
        otherTotalStockholdersEquity BIGINT,
        totalStockholdersEquity BIGINT,
        totalEquity BIGINT,
        minorityInterest BIGINT,
        totalLiabilitiesAndTotalEquity BIGINT,
        totalInvestments BIGINT,
        totalDebt BIGINT,
        netDebt BIGINT,
        companyName STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '{OUTPUT_PATH}'
    TBLPROPERTIES ('skip.header.line.count'='1');
    """)
    
except Exception as e:
    print(f"Error writing to HDFS: {e}")
    raise

# Stop Spark session
spark.stop()
print("\n✓ Pipeline complete!")
