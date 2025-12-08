# -*- coding: utf-8 -*-
# cash-flow-statement producer
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, lit
import requests
from kafka import KafkaProducer
import time
import os
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("FMP_Stocks_Producer_Cash_Flow_Specific_Quarters").getOrCreate()
sc = spark.sparkContext

# Configuration
FMP_API_KEY = "4gBV9fqtyqctZe57TWjpTmqNslCD8mjh"
BOOTSTRAP_SERVERS = "ip-172-31-14-3.eu-west-2.compute.internal:9092"
KAFKA_TOPIC = "cash-flow-statement-topic"
TIMEOUT = 2  # seconds between API calls

# Logging
LOG_DIR = "producer_data_not_loaded"
LOG_FILE = os.path.join(LOG_DIR, "unloaded_data_log.txt")
os.makedirs(LOG_DIR, exist_ok=True)

# Selected 8 diverse tickers
TICKERS = ["NVDA", "AAPL", "MSFT", "UNH", "JPM", "V", "XOM", "WMT"]
QUARTERS = ["Q1", "Q2", "Q3", "Q4"]

# Map ticker symbols to full company names
TICKER_NAME_MAP = {
    "NVDA": "NVIDIA Corp", "AAPL": "Apple Inc.", "MSFT": "Microsoft Corp.",
    "UNH": "UnitedHealth Group Inc.", "JPM": "JPMorgan Chase & Co.", "V": "Visa Inc.",
    "XOM": "Exxon Mobil Corp.", "WMT": "Walmart Inc."
}

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: v.encode("utf-8")
)

print(f"Starting cash-flow data ingestion for {len(TICKERS)} tickers...")

for ticker in TICKERS:
    print(f"\nProcessing ticker: {ticker}")
    for quarter in QUARTERS:
        api_url = f"https://financialmodelingprep.com/stable/cash-flow-statement?symbol={ticker}&period={quarter}&apikey={FMP_API_KEY}"

        try:
            response = requests.get(api_url, timeout=TIMEOUT)
            response.raise_for_status()
            data = response.json()

            if not data:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                with open(LOG_FILE, "a") as f:
                    f.write(f"{timestamp} - No data for {ticker} - {quarter} (Cash Flow)\n")
                print(f"No data for {ticker} - {quarter}, logged to {LOG_FILE}.")
                continue

            df = spark.read.json(sc.parallelize(data))
            df = df.withColumn("companyName", lit(TICKER_NAME_MAP.get(ticker, "Unknown")))

            for row in df.toJSON().collect():
                producer.send(KAFKA_TOPIC, key=ticker, value=row)

            print(f" -> Sent {len(data)} cash-flow records for {ticker} - {quarter} to Kafka.")

        except requests.exceptions.RequestException as e:
            print(f"API error for {ticker} - {quarter}: {e}")
        except Exception as e:
            print(f"Unexpected error for {ticker} - {quarter}: {e}")
        finally:
            time.sleep(TIMEOUT)

producer.flush()
producer.close()
spark.stop()
print(f"\nCash-flow ingestion complete. Unloaded data logged to {LOG_FILE}.")

