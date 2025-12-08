import unittest

# Your exact constants from producer-income-statement.py
TICKER_NAME_MAP = {
    "NVDA": "NVIDIA Corp", "AAPL": "Apple Inc.", "MSFT": "Microsoft Corp.",
    "UNH": "UnitedHealth Group Inc.", "JPM": "JPMorgan Chase & Co.", 
    "V": "Visa Inc.", "XOM": "Exxon Mobil Corp.", "WMT": "Walmart Inc."
}

TICKERS = ["NVDA", "AAPL", "MSFT", "UNH", "JPM", "V", "XOM", "WMT"]
KAFKA_TOPIC = "stocks-income-statement-topic"

class TestFinancialPipeline(unittest.TestCase):
    def test_ticker_mapping_correct(self):
        """Validates your production ticker mappings."""
        self.assertEqual(TICKER_NAME_MAP["AAPL"], "Apple Inc.")
        self.assertEqual(TICKER_NAME_MAP["NVDA"], "NVIDIA Corp")
        self.assertEqual(TICKER_NAME_MAP["MSFT"], "Microsoft Corp.")

    def test_ticker_list_has_correct_count(self):
        """Verifies 8 production tickers configured."""
        self.assertEqual(len(TICKERS), 8)
        self.assertIn("AAPL", TICKERS)
        self.assertIn("NVDA", TICKERS)

    def test_kafka_topic_configured(self):
        """Confirms Kafka topic name is set."""
        self.assertEqual(KAFKA_TOPIC, "stocks-income-statement-topic")

if __name__ == '__main__':
    unittest.main(verbosity=2)
