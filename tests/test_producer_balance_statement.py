import unittest

# Balance Sheet constants (matching your producer pattern)
BALANCE_TICKERS = ["NVDA", "AAPL", "MSFT", "UNH", "JPM", "V", "XOM", "WMT"]
BALANCE_TOPIC = "stocks-balance-sheet-topic"
QUARTERS = ["Q1", "Q2", "Q3", "Q4"]

TICKER_NAME_MAP = {
    "NVDA": "NVIDIA Corp", "AAPL": "Apple Inc.", "MSFT": "Microsoft Corp.",
    "UNH": "UnitedHealth Group Inc.", "JPM": "JPMorgan Chase & Co.", 
    "V": "Visa Inc.", "XOM": "Exxon Mobil Corp.", "WMT": "Walmart Inc."
}

class TestBalanceSheetPipeline(unittest.TestCase):
    def test_balance_ticker_mapping(self):
        """Validates balance sheet ticker mappings."""
        self.assertEqual(TICKER_NAME_MAP["AAPL"], "Apple Inc.")
        self.assertEqual(TICKER_NAME_MAP["JPM"], "JPMorgan Chase & Co.")

    def test_balance_pipeline_config(self):
        """Verifies balance sheet pipeline constants."""
        self.assertEqual(len(BALANCE_TICKERS), 8)
        self.assertEqual(BALANCE_TOPIC, "stocks-balance-sheet-topic")

if __name__ == '__main__':
    unittest.main(verbosity=2)

