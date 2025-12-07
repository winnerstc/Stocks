Real-Time Stock Pipeline and AI Prediction System
Overview

This project implements a real-time financial data ingestion, analytics, and prediction platform. It collects quarterly financial statements and market data, processes them through a streaming and data lake architecture, and applies multiple machine learning models and large language models to predict next-quarter financial performance.

Primary prediction targets:

Next-quarter revenue

Next-quarter net income

Next-quarter earnings per share (EPS)

Key Capabilities

Real-time financial data ingestion

Kafka-based streaming architecture

HDFS-backed raw data lake

Hive-based ETL and analytical modeling

Feature engineering on structured financial data

Traditional machine learning and ensemble models

Hybrid predictions using Google Gemini API

End-to-end testing coverage

System Architecture

             +---------------------+
             |   Stock API (FMP)   |
             +----------+----------+
                        |
                 Quarterly Financials
                        |
              +---------v----------+
              |    Kafka Streams   |
              +---------+----------+
                        |
                  Raw JSON Events
                        |
              +---------v----------+
              |        HDFS        |
              |   Raw Data Lake    |
              +---------+----------+
                        |
              +---------v----------+
              |        Hive        |
              |  ETL / Modeling   |
              +---------+----------+
                        |
              +---------v----------+
              | Feature Engineering|
              +---------+----------+
                        |
        +---------------v----------------+
        |   Machine Learning Models      |
        | (LR, RF, XGB, LGBM, SVM, etc.) |
        +---------------+----------------+
                        |
              +---------v----------+
              |   Gemini AI Model  |
              +---------+----------+
                        |
              +---------v----------+
              |   Prediction Output|
              +--------------------+
Data Sources

Financial data is retrieved using the Financial Modeling Prep (FMP) API.

Example API Endpoints

https://financialmodelingprep.com/stable/income-statement?symbol=AAPL&period=Q1&apikey=YOUR_KEY
https://financialmodelingprep.com/stable/balance-sheet-statement?symbol=AAPL&apikey=YOUR_KEY
https://financialmodelingprep.com/stable/cash-flow-statement?symbol=AAPL&apikey=YOUR_KEY
Financial Coverage

Income statement

Balance sheet

Cash flow statement

Quarterly data (Q1, Q2, Q3, Q4)

Real-Time Data Ingestion

Kafka streams:

Market prices

Quarterly financial statements

Company metadata

Kafka Topics
stocks.raw
stocks.financials
stocks.quarterly

HDFS Storage Layout
/user/<username>/stocks/raw/YYYY/MM/DD/


Raw JSON is stored unchanged for replay and audit purposes.

ETL Processing with Hive

External tables are created for analytics:

CREATE EXTERNAL TABLE income_statements (...)
CREATE EXTERNAL TABLE balance_sheets (...)
CREATE EXTERNAL TABLE cash_flows (...)

Transformations

String to numeric casting

Date normalization

Missing quarter handling

Dataset consolidation

Feature Engineering

Features include:

Year-over-year revenue growth

Quarter-over-quarter revenue growth

EPS change

Net income margin

Debt-to-equity ratio

Free cash flow trends

Operating income growth

Volatility indicators

Rolling aggregates

All features are converted to numeric vectors.

Machine Learning Models

Supported models:

Logistic regression

Random forest

Extra trees

Gradient boosting

XGBoost

LightGBM

Support vector machine (RBF)

k-nearest neighbors

Gaussian naive Bayes

Multilayer perceptron

Each model predicts:

Next-quarter revenue

Next-quarter net income

Next-quarter EPS

Confidence score

Gemini AI Integration

Gemini is used for reasoning on top of structured ML outputs.

Prompt inputs:

Last four quarters of financials

Full income statement

Full balance sheet

Full cash flow statement

Market context

ML model predictions

Prediction Output Format
{
  "symbol": "AAPL",
  "predicted_revenue_q_next": 121500000000,
  "predicted_eps_q_next": 1.32,
  "predicted_net_income_q_next": 28900000000,
  "ml_model_used": "XGBoost",
  "gemini_reasoning": "Based on recent revenue growth and margin trends"
}

Testing

Testing includes:

Mocked API unit tests

Kafka producer and consumer tests

HDFS and Hive integration tests

Feature validation tests

Model accuracy checks

End-to-end pipeline tests

Technology Stack
Layer	Technology
API	Financial Modeling Prep
Streaming	Kafka
Storage	HDFS
Processing	Hive, PySpark
ML	scikit-learn, XGBoost, LightGBM
LLM	Google Gemini API
Pipeline	Python
Orchestration	Airflow (optional)
Roadmap

Add Airflow DAG orchestration

Deploy ML models as REST services

Build real-time dashboards

Introduce a feature store

Add LLM fine-tuning support
