# -*- coding: utf-8 -*-
# consumer-income-statement.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# ---------------------------
# --- YOUR SETTINGS ---
# ---------------------------
KAFKA_BOOTSTRAP = "ip-172-31-14-3.eu-west-2.compute.internal:9092"
TOPIC = "stocks-income-statement-topic"
OUTPUT_PATH = "/tmp/income_statement_output"

# ---------------------------
# --- Spark Session ---
# ---------------------------
spark = SparkSession.builder.appName("Kafka_Income_Statement_Consumer").getOrCreate()

# ---------------------------
# --- Schema Definition ---
# ---------------------------
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
parsed_df = df.select(from_json(col("value").cast("string"), income_statement_schema).alias("data")) \
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

