#!/usr/bin/env python3
"""
Alternative data source using Yahoo Finance if FMP fails
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def get_yahoo_financials(ticker):
    """Get financial data from Yahoo Finance"""
    print(f"Fetching {ticker} from Yahoo Finance...")
    
    try:
        stock = yf.Ticker(ticker)
        
        # Get income statement (quarterly)
        income_stmt = stock.quarterly_financials
        if income_stmt.empty:
            print(f"  No income statement data for {ticker}")
            return None
        
        # Get balance sheet
        balance_sheet = stock.quarterly_balance_sheet
        cash_flow = stock.quarterly_cashflow
        
        # Transform income statement
        income_df = income_stmt.T.reset_index()
        income_df = income_df.rename(columns={"index": "date"})
        income_df["ticker"] = ticker
        
        # Add key metrics
        income_df["revenue"] = income_df.get("Total Revenue", income_df.get("Revenue", np.nan))
        income_df["netIncome"] = income_df.get("Net Income", income_df.get("Net Income From Continuing Ops", np.nan))
        
        # Calculate EPS if possible
        if "Basic EPS" in income_df.columns:
            income_df["eps"] = income_df["Basic EPS"]
        elif "Diluted EPS" in income_df.columns:
            income_df["eps"] = income_df["Diluted EPS"]
        
        print(f"  Retrieved {len(income_df)} quarters from Yahoo Finance")
        return income_df
        
    except Exception as e:
        print(f"  Yahoo Finance error for {ticker}: {e}")
        return None

# Quick test
if __name__ == "__main__":
    test_tickers = ["AAPL", "MSFT", "GOOGL"]
    for ticker in test_tickers:
        data = get_yahoo_financials(ticker)
        if data is not None:
            print(f"\n{ticker} data columns: {data.columns.tolist()}")
            print(f"Sample:\n{data[['date', 'revenue', 'netIncome']].head()}")