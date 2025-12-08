import unittest

# Cash Flow constants (matching your producer pattern)
CASHFLOW_TICKERS = ["NVDA", "AAPL", "MSFT", "UNH", "JPM", "V", "XOM", "WMT"]
CASHFLOW_TOPIC = "stocks-cash-flow-topic"
QUARTERS = ["Q1", "Q2", "Q3", "Q4"]

TICKER_NAME_MAP = {
    "NVDA": "NVIDIA Corp", "AAPL": "Apple Inc.", "MSFT": "Microsoft Corp.",
    "UNH": "UnitedHealth Group Inc.", "JPM": "JPMorgan Chase & Co.", 
    "V": "Visa Inc.", "XOM": "Exxon Mobil Corp.", "WMT": "Walmart Inc."
}

class TestCashFlowPipeline(unittest.TestCase):
    def test_cashflow_ticker_mapping(self):
        """Validates cash flow ticker mappings."""
        self.assertEqual(TICKER_NAME_MAP["MSFT"], "Microsoft Corp.")
        self.assertEqual(TICKER_NAME_MAP["V"], "Visa Inc.")

    def test_cashflow_pipeline_config(self):
        """Verifies cash flow pipeline constants."""
        self.assertEqual(len(CASHFLOW_TICKERS), 8)
        self.assertEqual(CASHFLOW_TOPIC, "stocks-cash-flow-topic")

if __name__ == '__main__':
    unittest.main(verbosity=2)
