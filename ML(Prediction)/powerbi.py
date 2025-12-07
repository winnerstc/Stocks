import pandas as pd
import numpy as np

# -----------------------------
# 1. SET FILE PATHS - RESTORED TO YOUR ORIGINAL ABSOLUTE PATHS
#    *** ENSURE THESE PATHS ARE CORRECT ON YOUR MACHINE ***
# -----------------------------
BS_FILE = r"C:\Users\Ja-Yuan Pendley\Downloads\query-hive-18651.csv"
IS_FILE = r"C:\Users\Ja-Yuan Pendley\Downloads\query-hive-18653.csv"
CF_FILE = r"C:\Users\Ja-Yuan Pendley\Downloads\query-hive-18652.csv"

# Output file saved to the same directory
OUTPUT_FILE = r"C:\Users\Ja-Yuan Pendley\Downloads\combined_financial_data_full.csv"

# -----------------------------
# 2. Helper function to clean column names
# -----------------------------
def clean_cols(df):
    """
    Cleans column names by stripping, lowercasing, replacing spaces with underscores,
    and removing all non-alphanumeric/underscore characters (including the dot).
    """
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace(r'[^\w]', '', regex=True)
    return df

# -----------------------------
# 3. Load CSVs and clean columns
# -----------------------------
try:
    df_bs = clean_cols(pd.read_csv(BS_FILE))
    df_is = clean_cols(pd.read_csv(IS_FILE))
    df_cf = clean_cols(pd.read_csv(CF_FILE))
except FileNotFoundError as e:
    print(f"\nFATAL ERROR: Could not find a file. Please double-check the file paths in the script's Section 1.")
    print(f"Error details: {e}")
    exit()

# -----------------------------
# 4. Define column aliases (Matching cleaned names)
# -----------------------------
# Balance Sheet Keys & Metrics
TCA = 'balance_sheet_cleanedtotalcurrentassets'
TCL = 'balance_sheet_cleanedtotalcurrentliabilities'
INV = 'balance_sheet_cleanedinventory'
TL = 'balance_sheet_cleanedtotalliabilities'
TE = 'balance_sheet_cleanedtotalequity'
TA = 'balance_sheet_cleanedtotalassets'
SYMBOL_BS = 'balance_sheet_cleanedsymbol'
DATE_BS = 'balance_sheet_cleaneddate'

# Income Statement Keys & Metrics
REV = 'income_statementsrevenue'
GP = 'income_statementsgrossprofit'
EBIT = 'income_statementsoperatingincome'
INT_EXP = 'income_statementsinterestexpense'
NI = 'income_statementsnetincome'
SYMBOL_IS = 'income_statementssymbol'
DATE_IS = 'income_statementsdate'

# Cash Flow Keys & Metrics
OCF = 'cash_flow_analysisnetcashprovidedbyoperatingactivities'
CAPEX = 'cash_flow_analysisinvestmentsinpropertyplantandequipment'
SYMBOL_CF = 'cash_flow_analysissymbol'
DATE_CF = 'cash_flow_analysisdate'

# -----------------------------
# 5. Safe datetime parsing & cleaning
# -----------------------------
date_format = "%Y-%m-%d"

df_bs[DATE_BS] = pd.to_datetime(df_bs[DATE_BS], errors='coerce', format=date_format)
df_is[DATE_IS] = pd.to_datetime(df_is[DATE_IS], errors='coerce', format=date_format)
df_cf[DATE_CF] = pd.to_datetime(df_cf[DATE_CF], errors='coerce', format=date_format)

# Drop rows with invalid dates
df_bs = df_bs.dropna(subset=[DATE_BS])
df_is = df_is.dropna(subset=[DATE_IS])
df_cf = df_cf.dropna(subset=[DATE_CF])

# -----------------------------
# 6. Standardize symbols
# -----------------------------
df_bs[SYMBOL_BS] = df_bs[SYMBOL_BS].astype(str).str.upper()
df_is[SYMBOL_IS] = df_is[SYMBOL_IS].astype(str).str.upper()
df_cf[SYMBOL_CF] = df_cf[SYMBOL_CF].astype(str).str.upper()

# -----------------------------
# 7. Calculate key metrics (on individual DFs)
# -----------------------------
# Liquidity
df_bs['current_ratio'] = np.divide(df_bs[TCA], df_bs[TCL], out=np.full_like(df_bs[TCA], np.nan, dtype=float), where=df_bs[TCL]!=0)
df_bs['quick_ratio'] = np.divide((df_bs[TCA] - df_bs[INV]), df_bs[TCL], out=np.full_like(df_bs[TCA], np.nan, dtype=float), where=df_bs[TCL]!=0)

# Debt & Interest
df_bs['debt_to_equity'] = np.divide(df_bs[TL], df_bs[TE], out=np.full_like(df_bs[TL], np.nan, dtype=float), where=df_bs[TE]!=0)
df_is['interest_coverage'] = np.divide(df_is[EBIT], df_is[INT_EXP].abs(), out=np.full_like(df_is[EBIT], np.nan, dtype=float), where=df_is[INT_EXP]!=0)

# Profitability
df_is['gross_margin'] = np.divide(df_is[GP], df_is[REV], out=np.full_like(df_is[GP], np.nan, dtype=float), where=df_is[REV]!=0)
df_is['operating_margin'] = np.divide(df_is[EBIT], df_is[REV], out=np.full_like(df_is[EBIT], np.nan, dtype=float), where=df_is[REV]!=0)

# Cash Flow
df_cf['free_cash_flow'] = df_cf[OCF] + df_cf[CAPEX]

# -----------------------------
# 8. MERGE ALL TABLES using OUTER JOIN
# -----------------------------
# Merge IS and BS first
df_merged = df_bs.merge(
    df_is,
    left_on=[SYMBOL_BS, DATE_BS],
    right_on=[SYMBOL_IS, DATE_IS],
    how='outer',
    suffixes=('_bs', '_is')
)

# Merge the result with CF
df_merged = df_merged.merge(
    df_cf,
    left_on=[SYMBOL_BS, DATE_BS],
    right_on=[SYMBOL_CF, DATE_CF],
    how='outer',
    suffixes=('', '_cf')
)

# -----------------------------
# 9. Final calculations
# -----------------------------
df_merged['roe'] = np.divide(df_merged[NI], df_merged[TE], out=np.full_like(df_merged[NI], np.nan, dtype=float), where=df_merged[TE]!=0)
df_merged['roa'] = np.divide(df_merged[NI], df_merged[TA], out=np.full_like(df_merged[NI], np.nan, dtype=float), where=df_merged[TA]!=0)
df_merged['fcf_vs_net_income'] = np.divide(df_merged['free_cash_flow'], df_merged[NI], out=np.full_like(df_merged['free_cash_flow'], np.nan, dtype=float), where=df_merged[NI]!=0)


# -----------------------------
# 10. Finalize Index Columns (coalesce keys and add Year & Quarter)
# -----------------------------
# Create a single, consistent date and symbol column from the 6 keys
df_merged['date'] = df_merged[DATE_BS].fillna(df_merged[DATE_IS]).fillna(df_merged[DATE_CF])
df_merged['symbol'] = df_merged[SYMBOL_BS].fillna(df_merged[SYMBOL_IS]).fillna(df_merged[SYMBOL_CF])

# Add Year & Quarter
df_merged['year'] = df_merged['date'].dt.year
df_merged['quarter'] = df_merged['date'].dt.quarter

# Drop the individual statement key columns now that we have unified 'date' and 'symbol'
cols_to_drop = [DATE_BS, SYMBOL_BS, DATE_IS, SYMBOL_IS, DATE_CF, SYMBOL_CF]
df_merged = df_merged.drop(columns=[col for col in cols_to_drop if col in df_merged.columns])

# -----------------------------
# 11. Sort for readability
# -----------------------------
df_merged.sort_values(by=['symbol', 'date'], inplace=True)

# -----------------------------
# 12. Save CSV
# -----------------------------
df_merged.to_csv(OUTPUT_FILE, index=False)
print(f"\nSUCCESS: The combined financial CSV has been saved to: {OUTPUT_FILE}")
print(f"Total rows in the combined file: {len(df_merged)}")