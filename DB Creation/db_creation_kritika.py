"""
FINAL FIXED retail_etl_pipeline.py
All FK/PK/UNIQUE errors resolved.
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime

# -------------------------------------------------------------------
# 1. Helper functions
# -------------------------------------------------------------------

def parse_mixed_date(series: pd.Series) -> pd.Series:
    s = series.astype(str)
    res = pd.Series(pd.NaT, index=s.index, dtype='datetime64[ns]')
    mask_slash = s.str.contains("/")
    mask_dash = s.str.contains("-")

    res.loc[mask_slash] = pd.to_datetime(s.loc[mask_slash],
        format="%m/%d/%Y", errors="coerce")

    res.loc[mask_dash] = pd.to_datetime(s.loc[mask_dash],
        format="%m-%d-%y", errors="coerce")

    return res


def safe_int_str(series: pd.Series) -> pd.Series:
    return series.astype("Int64").astype(str)


# -------------------------------------------------------------------
# 2. Load raw data
# -------------------------------------------------------------------

CSV_PATH = "/kaggle/working/good_data.csv"
DB_URL = "sqlite:///retail.db"

print("🔹 Loading CSV...")
raw = pd.read_csv(CSV_PATH)

raw["tx_date"] = parse_mixed_date(raw["Date"])
raw["customer_id"] = safe_int_str(raw["Customer_ID"])
raw["transaction_id"] = safe_int_str(raw["Transaction_ID"])

# -------------------------------------------------------------------
# 3. BUILD NORMALIZED TABLES
# -------------------------------------------------------------------

def build_customer_master(df):
    g = df.dropna(subset=["customer_id"]).groupby("customer_id")

    cm = g.agg({
        "Name":"first","Email":"first","Phone":"first",
        "Address":"first","City":"first","State":"first",
        "Zipcode":"first","Country":"first","Age":"first",
        "Gender":"first","Income":"first","Customer_Segment":"first",
        "tx_date":["min","max"]
    }).reset_index()

    cm.columns = [
        "customer_id","name","email","phone","address","city","state","zipcode",
        "country","age","gender","income","customer_segment",
        "customer_since","last_purchase_date"
    ]

    cm["is_loyalty_member"] = True
    cm["total_loyalty_points"] = 0
    cm["bonus_points"] = 0
    cm["last_points_update"] = pd.NaT

    cm["zipcode"] = cm["zipcode"].astype("Int64").astype(str)
    cm["age"] = cm["age"].astype("Int64")
    cm["income"] = pd.to_numeric(cm["income"], errors="coerce")

    # CLEAN duplicated keys
    cm = cm.drop_duplicates(subset=["customer_id"])

    return cm


def build_product_master(df):
    pm = df[["Product_Category","Product_Brand","Product_Type","products"]].drop_duplicates()
    pm = pm.rename(columns={
        "Product_Category":"product_category",
        "Product_Brand":"product_brand",
        "Product_Type":"product_type",
        "products":"product_full_name"
    })

    pm = pm.reset_index(drop=True)
    pm["product_key"] = pm.index.to_series().add(1).map(lambda x: f"P{x:06d}")
    pm["is_active"] = True

    return pm[[
        "product_key","product_category","product_brand","product_type",
        "product_full_name","is_active"
    ]]


def build_sales_transactions(df):
    st = df.copy()
    st["date"] = st["tx_date"]
    st["year"] = st["date"].dt.year
    st["month"] = st["date"].dt.month

    month_map = {
        "January":1,"February":2,"March":3,"April":4,"May":5,"June":6,
        "July":7,"August":8,"September":9,"October":10,"November":11,"December":12
    }
    nat_mask = st["date"].isna()
    st.loc[nat_mask,"year"] = st.loc[nat_mask,"Year"].astype("Int64")
    st.loc[nat_mask,"month"] = st.loc[nat_mask,"Month"].map(month_map)

    st["ingestion_timestamp"] = datetime.utcnow()
    st["data_quality_flag"] = "PASS"
    st["reject_reason"] = pd.NA

    st_df = st.rename(columns={
        "Time":"time",
        "Total_Purchases":"total_purchases",
        "Amount":"amount",
        "Total_Amount":"total_amount",
        "Product_Category":"product_category",
        "Product_Brand":"product_brand",
        "Product_Type":"product_type",
        "Shipping_Method":"shipping_method",
        "Payment_Method":"payment_method",
        "Order_Status":"order_status",
        "Ratings":"ratings",
        "Feedback":"feedback"
    })

    st_df = st_df[[
        "transaction_id","customer_id","date","year","month","time",
        "total_purchases","amount","total_amount","product_category",
        "product_brand","product_type","shipping_method","payment_method",
        "order_status","ratings","feedback","ingestion_timestamp",
        "data_quality_flag","reject_reason"
    ]]

    # CLEAN null + duplicate transaction IDs
    st_df = st_df.dropna(subset=["transaction_id"])
    st_df = st_df[st_df["transaction_id"] != "<NA>"]
    st_df = st_df.drop_duplicates(subset=["transaction_id"])

    return st_df


def build_customer_analytics(st_df):
    snapshot_date = st_df["date"].max()
    g = st_df.groupby("customer_id")

    base = g.agg({
        "date":"max","transaction_id":"nunique","total_amount":"sum",
        "product_type":"nunique","ratings":"mean"
    }).reset_index()

    base = base.rename(columns={
        "date":"last_purchase_date","transaction_id":"frequency",
        "total_amount":"monetary","product_type":"product_diversity",
        "ratings":"avg_rating"
    })

    base["recency"] = (snapshot_date - base["last_purchase_date"]).dt.days
    base["avg_rating"] = base["avg_rating"].fillna(0)
    base["monetary"] = base["monetary"].fillna(0)
    base["recency"] = base["recency"].fillna(base["recency"].max())

    base["rfm_score"] = 1
    base["segment"] = "Medium"
    base["clv_score"] = (
        base["monetary"] * base["frequency"] / (base["recency"] + 1)
    )

    base["snapshot_date"] = snapshot_date

    return base[[
        "customer_id","recency","frequency","monetary",
        "rfm_score","segment","product_diversity","avg_rating",
        "clv_score","snapshot_date"
    ]]


def build_loyalty_transactions(st_df):
    lt = st_df[[
        "transaction_id","customer_id","date","total_amount"
    ]].copy()

    lt["points_earned"] = lt["total_amount"].fillna(0).floordiv(10).astype(int)
    lt["points_redeemed"] = 0
    lt["bonus_points"] = 0

    lt = lt.sort_values(["customer_id","date","transaction_id"])
    lt["balance_after"] = lt.groupby("customer_id")["points_earned"].cumsum()

    lt["event_date"] = lt["date"]
    lt["event_type"] = "EARN"

    lt = lt.reset_index(drop=True)
    lt["loyalty_txn_id"] = lt.index.to_series().add(1).map(lambda x: f"L{x:07d}")

    return lt[[
        "loyalty_txn_id","customer_id","transaction_id","points_earned",
        "points_redeemed","bonus_points","balance_after","event_date",
        "event_type"
    ]]


# -------------------------------------------------------------------
# 4. Build all DataFrames
# -------------------------------------------------------------------

print("🔹 Building normalized tables...")

customer_master_df = build_customer_master(raw)
product_master_df = build_product_master(raw)
sales_transactions_df = build_sales_transactions(raw)
customer_analytics_df = build_customer_analytics(sales_transactions_df)
loyalty_transactions_df = build_loyalty_transactions(sales_transactions_df)

# -------------------------------------------------------------------
# 5. CREATE TABLES
# -------------------------------------------------------------------

engine = create_engine(DB_URL)

DDL = [
    "DROP TABLE IF EXISTS loyalty_transactions;",
    "DROP TABLE IF EXISTS customer_analytics;",
    "DROP TABLE IF EXISTS sales_transactions;",
    "DROP TABLE IF EXISTS product_master;",
    "DROP TABLE IF EXISTS customer_master;",

    """
    CREATE TABLE customer_master (
        customer_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        email VARCHAR,
        phone VARCHAR,
        address VARCHAR,
        city VARCHAR,
        state VARCHAR,
        zipcode VARCHAR,
        country VARCHAR,
        age INT,
        gender VARCHAR,
        income DECIMAL,
        customer_segment VARCHAR,
        is_loyalty_member BOOLEAN,
        customer_since DATE,
        total_loyalty_points INT,
        bonus_points INT,
        last_points_update DATE,
        last_purchase_date DATE
    );
    """,

    """
    CREATE TABLE product_master (
        product_key VARCHAR PRIMARY KEY,
        product_category VARCHAR,
        product_brand VARCHAR,
        product_type VARCHAR,
        product_full_name VARCHAR,
        is_active BOOLEAN
    );
    """,

    """
    CREATE TABLE sales_transactions (
        transaction_id VARCHAR PRIMARY KEY,
        customer_id VARCHAR,
        date DATE,
        year INT,
        month INT,
        time VARCHAR,
        total_purchases INT,
        amount DECIMAL,
        total_amount DECIMAL,
        product_category VARCHAR,
        product_brand VARCHAR,
        product_type VARCHAR,
        shipping_method VARCHAR,
        payment_method VARCHAR,
        order_status VARCHAR,
        ratings INT,
        feedback TEXT,
        ingestion_timestamp TIMESTAMP,
        data_quality_flag VARCHAR,
        reject_reason TEXT,
        FOREIGN KEY(customer_id) REFERENCES	customer_master(customer_id)
    );
    """,

    """
    CREATE TABLE customer_analytics (
        customer_id VARCHAR PRIMARY KEY,
        recency INT,
        frequency INT,
        monetary DECIMAL,
        rfm_score INT,
        segment VARCHAR,
        product_diversity INT,
        avg_rating DECIMAL,
        clv_score DECIMAL,
        snapshot_date DATE,
        FOREIGN KEY(customer_id) REFERENCES customer_master(customer_id)
    );
    """,

    """
    CREATE TABLE loyalty_transactions (
        loyalty_txn_id VARCHAR PRIMARY KEY,
        customer_id VARCHAR,
        transaction_id VARCHAR,
        points_earned INT,
        points_redeemed INT,
        bonus_points INT,
        balance_after INT,
        event_date DATE,
        event_type VARCHAR,
        FOREIGN KEY(customer_id) REFERENCES customer_master(customer_id),
        FOREIGN KEY(transaction_id) REFERENCES sales_transactions(transaction_id)
    );
    """
]

print("🔹 Creating database schema...")
with engine.begin() as conn:
    conn.exec_driver_sql("PRAGMA foreign_keys = OFF;")   # 🔥 FIX
    for stmt in DDL:
        conn.exec_driver_sql(stmt)
    conn.exec_driver_sql("PRAGMA foreign_keys = ON;")    # 🔥 RE-ENABLE

# -------------------------------------------------------------------
# 6. LOAD DATA
# -------------------------------------------------------------------

print("🔹 Loading data into DB...")

customer_master_df.to_sql("customer_master", engine, if_exists="append", index=False)
product_master_df.to_sql("product_master", engine, if_exists="append", index=False)
sales_transactions_df.to_sql("sales_transactions", engine, if_exists="append", index=False)
customer_analytics_df.to_sql("customer_analytics", engine, if_exists="append", index=False)
loyalty_transactions_df.to_sql("loyalty_transactions", engine, if_exists="append", index=False)

print("✅ ETL pipeline finished. retail.db created!")