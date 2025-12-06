# -*- coding: utf-8 -*-
# income-statement producer
from pyspark.sql import SparkSession
# Import 'lit' and 'when' to add the company name column
from pyspark.sql.functions import col, to_json, struct, lit, when
import requests
from kafka import KafkaProducer
import json
import time
# Import os for directory handling
import os
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("FMP_Stocks_Producer_Income_Statement_Specific_Quarters").getOrCreate()
sc = spark.sparkContext

# Configuration
FMP_API_KEY = "tiFaviFGi3xdigG3dp7OT7cHgnA0OBmu"
BOOTSTRAP_SERVERS = "ip-172-31-14-3.eu-west-2.compute.internal:9092"
kafka_topic = "stocks-income-statement-topic" # Ensure this topic name is correct
TIMEOUT = 2  # Timeout between each API call pause

# --- LOGGING CONFIGURATION ---
LOG_DIR = "producer_data_not_loaded"
LOG_FILE = os.path.join(LOG_DIR, "unloaded_data_log.txt")

# Ensure the log directory exists
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
# -----------------------------

# List of tickers (Updated to match cash producer's structure)
TICKERS = ["NVDA", "AAPL", "MSFT", "AVGO", "GOOGL", "UNH", "MRK", "JPM", "V", "BAC",
           "PYPL", "C", "GS", "WFC", "HOOD", "XOM", "CVX", "MRO", "WMT", "COST", "TGT"]
QUARTERS = ["Q1", "Q2", "Q3", "Q4"]

# Map ticker symbols to full company names (Updated to match cash producer's structure)
TICKER_NAME_MAP = {
    "NVDA": "NVIDIA Corp", "AAPL": "Apple Inc.", "MSFT": "Microsoft Corp.", "AVGO": "Broadcom Inc.",
    "GOOGL": "Alphabet Inc. (Class A)", "UNH": "UnitedHealth Group Inc.", "MRK": "Merck & Co., Inc.",
    "JPM": "JPMorgan Chase & Co.", "V": "Visa Inc.", "BAC": "Bank of America Corp.",
    "PYPL": "PayPal Holdings, Inc.", "C": "Citigroup Inc.", "GS": "The Goldman Sachs Group, Inc.",
    "WFC": "Wells Fargo & Company", "HOOD": "Robinhood Markets Inc.", "XOM": "Exxon Mobil Corp.",
    "CVX": "Chevron Corp.", "MRO": "Marathon Oil Corp.", "WMT": "Walmart Inc.",
    "COST": "Costco Wholesale Corp.", "TGT": "Target Corp."
}

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: v.encode("utf-8")
)

print(f"Starting data ingestion for {len(TICKERS)} tickers...")

for ticker in TICKERS:
    print(f"\nProcessing ticker: {ticker}")
    for quarter in QUARTERS:
        # Fixed API URL structure for INCOME STATEMENT
        api_url = f"https://financialmodelingprep.com/stable/income-statement?symbol={ticker}&period={quarter}&apikey={FMP_API_KEY}"

        try:
            print(f"Fetching data for {ticker} - {quarter}...")
            response = requests.get(api_url, timeout=TIMEOUT)
            response.raise_for_status()
            income_statement_data = response.json()

            if not income_statement_data:
                # --- NEW LOGGING CODE HERE ---
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                log_message = f"{timestamp} - No data received for {ticker} - {quarter} (Income Statement)\n"

                with open(LOG_FILE, "a") as f:
                    f.write(log_message)

                print(f"No data received for {ticker} - {quarter}, skipping and logging to {LOG_FILE}.")
                continue

            # Convert list -> RDD -> DataFrame using SparkContext (sc)
            income_statement_rdd = sc.parallelize(income_statement_data)
            df = spark.read.json(income_statement_rdd)

            # *** Add the 'companyName' column here ***
            company_name = TICKER_NAME_MAP.get(ticker, "Unknown")
            df = df.withColumn("companyName", lit(company_name))

            # Create JSON messages to send via the Python KafkaProducer
            # Struct('*') now includes the new 'companyName' field
            json_messages = (
                df.withColumn("value", to_json(struct("*")))
                .select("value")
                .collect()
            )

            # Send messages manually using the Python loop
            for row in json_messages:
                producer.send(kafka_topic, key=ticker, value=row.value)
                print(f" -> Sent statement for {ticker} - {quarter} to Kafka.")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {ticker} - {quarter} from FMP API: {e}")
        except Exception as e:
            print(f"An unexpected error occurred for {ticker} - {quarter}: {e}")
        finally:
            # Pause for 2 seconds after each quarter's API call
            print(f"Pausing for {TIMEOUT} seconds...")
            time.sleep(TIMEOUT)

# Flush and close producer after all tickers and quarters are processed
producer.flush()
producer.close()

print(f"\nAll data ingestion completed successfully. Unloaded data logged to {LOG_FILE}.")
spark.stop()
