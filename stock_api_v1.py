import requests
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

API_KEY = ""
SYMBOL = "AAPL"

# ======================================================
# 1️⃣ Load income statement
# ======================================================
def load_income_statement(symbol):
    url = f"https://financialmodelingprep.com/stable/income-statement?symbol={symbol}&apikey={API_KEY}"
    print(f"Fetching income statement for {symbol} ...")
    r = requests.get(url)

    if r.status_code != 200:
        print("❌ API ERROR:", r.text)
        return pd.DataFrame()

    try:
        data = r.json()
    except Exception as e:
        print("❌ JSON parse error:", e)
        return pd.DataFrame()

    if not isinstance(data, list) or len(data) == 0:
        print(f"❌ No data returned")
        return pd.DataFrame()

    return pd.DataFrame(data)


# ======================================================
# 2️⃣ Transform for ML
# ======================================================
def transform_income(df):
    required = ["date", "revenue", "netIncome", "eps", "grossProfit", "operatingIncome"]
    df = df[required].copy()
    df = df.rename(columns={"eps": "epsDiluted"})
    df["gross_margin"] = df["grossProfit"] / df["revenue"]
    df["operating_margin"] = df["operatingIncome"] / df["revenue"]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "revenue", "netIncome", "epsDiluted", "gross_margin", "operating_margin"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


# ======================================================
# 3️⃣ Feature engineering
# ======================================================
def prepare_features(df):
    df = df.sort_values("date").copy()
    df["revenue_lag1"] = df["revenue"].shift(1)
    df["netIncome_lag1"] = df["netIncome"].shift(1)
    df["epsDiluted_lag1"] = df["epsDiluted"].shift(1)
    df["gross_margin_lag1"] = df["gross_margin"].shift(1)
    df["operating_margin_lag1"] = df["operating_margin"].shift(1)
    df = df.dropna().reset_index(drop=True)
    return df


# ======================================================
# 4️⃣ Train Random Forests
# ======================================================
def train_random_forest(df):
    df = prepare_features(df)
    features = ["revenue_lag1", "netIncome_lag1", "epsDiluted_lag1", "gross_margin_lag1", "operating_margin_lag1"]

    X = df[features]
    y_rev = df["revenue"]
    y_net = df["netIncome"]
    y_eps = df["epsDiluted"]

    X_train, X_test, y_train_rev, y_test_rev = train_test_split(X, y_rev, test_size=0.2, random_state=42)
    _, _, y_train_net, y_test_net = train_test_split(X, y_net, test_size=0.2, random_state=42)
    _, _, y_train_eps, y_test_eps = train_test_split(X, y_eps, test_size=0.2, random_state=42)

    m_rev = RandomForestRegressor(n_estimators=200, random_state=42)
    m_net = RandomForestRegressor(n_estimators=200, random_state=42)
    m_eps = RandomForestRegressor(n_estimators=200, random_state=42)

    m_rev.fit(X_train, y_train_rev)
    m_net.fit(X_train, y_train_net)
    m_eps.fit(X_train, y_train_eps)

    # RMSE
    rmse_rev = mean_squared_error(y_test_rev, m_rev.predict(X_test)) ** 0.5
    rmse_net = mean_squared_error(y_test_net, m_net.predict(X_test)) ** 0.5
    rmse_eps = mean_squared_error(y_test_eps, m_eps.predict(X_test)) ** 0.5

    def fmt(value, is_currency=True):
        if is_currency:
            if abs(value) >= 1e9:
                return f"${value/1e9:.2f}B"
            elif abs(value) >= 1e6:
                return f"${value/1e6:.2f}M"
            else:
                return f"${value:,.0f}"
        else:
            return f"{value:.2f}"

    print("\n======= MODEL RMSE =======")
    print("Revenue RMSE:    ", fmt(rmse_rev))
    print("Net Income RMSE: ", fmt(rmse_net))
    print("EPS RMSE:        ", fmt(rmse_eps, is_currency=False))
    print("===========================\n")

    return m_rev, m_net, m_eps


# ======================================================
# 5️⃣ Predict next quarter only
# ======================================================
def predict_next_quarter(df, m_rev, m_net, m_eps):
    df = df.sort_values("date").reset_index(drop=True)
    last = df.iloc[-1]

    feature_names = ["revenue_lag1", "netIncome_lag1", "epsDiluted_lag1", "gross_margin_lag1", "operating_margin_lag1"]
    features_df = pd.DataFrame([[
        last["revenue"],
        last["netIncome"],
        last["epsDiluted"],
        last["gross_margin"],
        last["operating_margin"]
    ]], columns=feature_names)

    pred_rev = m_rev.predict(features_df)[0]
    pred_net = m_net.predict(features_df)[0]
    pred_eps = m_eps.predict(features_df)[0]

    def fmt(value, is_currency=True):
        if is_currency:
            if abs(value) >= 1e9:
                return f"${value/1e9:.2f}B"
            elif abs(value) >= 1e6:
                return f"${value/1e6:.2f}M"
            else:
                return f"${value:,.0f}"
        else:
            return f"{value:.2f}"

    print("====== NEXT QUARTER FORECAST ======")
    print(f"Revenue:       {fmt(pred_rev)}")
    print(f"Net Income:    {fmt(pred_net)}")
    print(f"EPS (diluted): {fmt(pred_eps, is_currency=False)}")
    print("==================================\n")


# ======================================================
# 6️⃣ Main
# ======================================================
if __name__ == "__main__":
    df_income = load_income_statement(SYMBOL)

    if df_income.empty:
        print("❌ No income statement data available. Exiting.")
        exit(1)

    df = transform_income(df_income)
    if df.empty:
        print("❌ Transformed DataFrame is empty. Exiting.")
        exit(1)

    print("Loaded income statement rows:", len(df))
    print(df.head())

    model_r, model_n, model_e = train_random_forest(df)
    predict_next_quarter(df, model_r, model_n, model_e)
