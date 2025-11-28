📈 Real-Time Stock Pipeline & AI Prediction System

A complete real-time stock analysis pipeline that ingests live financial data, processes it into a data lake, transforms it for analytics, and uses multiple AI/ML models — including Google Gemini — to predict next-quarter revenue, EPS, and net income.

🚀 Overview

This project builds a real-time financial analytics and prediction system using:

Data & Storage

📡 Stock Market API

📑 Balance Sheet, Cash Flow, Income Statement

🔄 Standardized Q1 / Q2 / Q3 / Q4 ingestion

📦 Apache Kafka for real-time streaming

🗂 HDFS for raw data lake storage

🏛 Apache Hive for data transformation

AI & Machine Learning

🤖 Google Gemini API (LLM predictions)

📊 Traditional ML models:

Logistic Regression

Random Forest Classifier

XGBoost Classifier

LightGBM Classifier

Extra Trees Classifier

Gradient Boosting

SVM (RBF Kernel)

k-Nearest Neighbors

Gaussian Naïve Bayes

MLP Neural Network (scikit-learn)

The final goal is to predict next quarter performance for major stocks such as:
AAPL, MSFT, NVDA, AVGO, GOOGL, UNH, MRK, JPM, V, BAC, PYPL, GS, WFC, HOOD, XOM, CVX, MRO, WMT, COST, TGT, and more.

🏗 System Architecture
             ┌──────────────────┐
             │  Stock API (FMP) │
             └─────────┬────────┘
                       │ (Q1–Q4 Financials)
                ┌──────▼───────┐
                │ Kafka Stream │
                └──────┬───────┘
                (Raw JSON events)
                       │
              ┌────────▼────────┐
              │      HDFS       │
              │  Data Lake RAW  │
              └────────┬────────┘
                       │
              ┌────────▼────────┐
              │      Hive       │
              │  Transform/ETL  │
              └────────┬────────┘
                       │
          ┌────────────▼────────────┐
          │    Feature Engineering   │
          └────────────┬────────────┘
                       │
       ┌───────────────▼────────────────┐
       │     Machine Learning Models    │
       │  (LR, RF, XGBoost, LGBM, SVM…) │
       └───────────────┬────────────────┘
                       │
     ┌─────────────────▼─────────────────┐
     │         Gemini LLM Prediction     │
     └─────────────────┬─────────────────┘
                       │
          ┌────────────▼────────────┐
          │   Prediction Output     │
          │   Revenue / Net Income  │
          │   EPS Next Quarter      │
          └─────────────────────────┘

🔌 Data Sources

We use the Financial Modeling Prep API (stable endpoint):

Example API calls:

https://financialmodelingprep.com/stable/income-statement?symbol=AAPL&period=Q1&apikey=YOUR_KEY
https://financialmodelingprep.com/stable/balance-sheet-statement?symbol=AAPL&apikey=YOUR_KEY
https://financialmodelingprep.com/stable/cash-flow-statement?symbol=AAPL&apikey=YOUR_KEY


All quarterly data is loaded:

Q1

Q2

Q3

Q4

All stocks listed in tickers.txt or within the script are processed automatically.

🚚 Real-Time Data Ingestion (Kafka)

The pipeline streams data like:

Stock prices

API financial statements

Company metadata

Each record is published into Kafka topics:

stocks.raw
stocks.financials
stocks.quarterly


Kafka → HDFS sink writes raw JSON to:

/user/<name>/stocks/raw/YYYY/MM/DD

🗂 ETL in Hive

Hive transforms the data into analytics-ready tables:

CREATE EXTERNAL TABLE income_statements (...)
CREATE EXTERNAL TABLE balance_sheets (...)
CREATE EXTERNAL TABLE cash_flows (...)


Transformations include:

Converting strings → numeric

Date normalization

Handling missing quarterly data

Combining multiple sources into a master table

🧪 Feature Engineering

Features include:

Revenue YoY, QoQ

EPS change

Net income margin

Debt-to-equity

Free cash flow

Operating income growth

Volatility indicators

Rolling averages

Model-ready numeric vectors

This dataset feeds all ML models.

🤖 Machine Learning Models

The following models are trained:

Classification / Regression Models

Logistic Regression

Random Forest

Extra Trees

Gradient Boosting

XGBoost

LightGBM

SVM (RBF Kernel)

k-Nearest Neighbors

Gaussian Naïve Bayes

MLPClassifier (Neural Network)

Each model outputs:

📈 Next quarter Revenue prediction

🏦 Next quarter Net Income

💵 Next quarter EPS

🎯 Confidence score

✨ Gemini AI Prediction

We combine structured ML models with Gemini LLM reasoning.

The prompt includes:

Last 4 quarters financials

Full income statement

Full balance sheet

Full cash flow

Market conditions

Analyst expectations

ML model predictions

Gemini produces a final “hybrid AI” answer.

📤 Output

Final JSON looks like:

{
  "symbol": "AAPL",
  "predicted_revenue_q_next": 121500000000,
  "predicted_eps_q_next": 1.32,
  "predicted_net_income_q_next": 28900000000,
  "ml_model_used": "XGBoost",
  "gemini_reasoning": "Based on YoY revenue acceleration..."
}

🧪 Unit Testing

Testing includes:

API call unit tests (mocked)

Kafka producer simulation tests

HDFS/Hive integration tests

ML model accuracy tests

Format checks (revenue formatting, EPS formatting, rounding)

End-to-end pipeline test

🛠 Tech Stack
Layer	Technology
API	Financial Modeling Prep
Streaming	Kafka
Storage	HDFS
Processing	Hive / PySpark
ML	scikit-learn, XGBoost, LightGBM
LLM	Google Gemini API
Pipeline	Python
Orchestration	Airflow (optional)
📅 Roadmap

 Add Airflow DAGs

 Deploy ML models as REST API

 Add real-time dashboard (Grafana or Streamlit)

 Integrate feature store

 Add LLM fine-tuning mode

If you'd like, I can also generate:

✅ a diagram image version
✅ a Dockerfile for the whole system
✅ the exact folder structure for your repo
