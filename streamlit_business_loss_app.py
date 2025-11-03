import pandas as pd
import streamlit as st
import plotly.express as px
import math
import unicodedata
from google.cloud import bigquery
from google.oauth2 import service_account

# -------------------------------
# CONFIG
# -------------------------------
INVENTORY_URL = "https://docs.google.com/spreadsheets/d/1nLdtjYwVD1AFa1VqCUlPS2W8t4lRYJnyMOwMX8sNkfU/export?format=csv&gid=0"
ARR_DRR_URL   = "https://docs.google.com/spreadsheets/d/1nLdtjYwVD1AFa1VqCUlPS2W8t4lRYJnyMOwMX8sNkfU/export?format=csv&gid=1079657777"
B2B_URL       = "https://docs.google.com/spreadsheets/d/1nLdtjYwVD1AFa1VqCUlPS2W8t4lRYJnyMOwMX8sNkfU/export?format=csv&gid=2131638248"

# -------------------------------
# BIGQUERY CONNECTION
# -------------------------------
@st.cache_resource
def get_bq_client():
    creds_dict = st.secrets["bigquery"]
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    return bigquery.Client(credentials=credentials, project=creds_dict["project_id"])

client = get_bq_client()

# -------------------------------
# SKU CLEANING FIX (Encoding)
# -------------------------------
def clean_sku_input(text):
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", str(text))
    text = text.encode("ascii", "ignore").decode("ascii")
    return text.strip().upper()

# -------------------------------
# FIXED BIGQUERY FUNCTION
# -------------------------------
@st.cache_data(ttl=300)
def fetch_warehouse_summary(sku):
    sku = clean_sku_input(sku)

    query = f"""
        -- Blocked inventory from BlockedInv
        WITH blocked AS (
            SELECT
              REPLACE(REPLACE(TRIM(SKU), "'", ""), "‘", "") AS Clean_SKU,
              CASE 
                WHEN Location = 'Heavenly Secrets Private Limited - Emiza Bilaspur' THEN 'Bilaspur'
                WHEN Location = 'Heavenly Secrets Private Limited - Bangalore' THEN 'Bangalore'
                WHEN Location = 'Heavenly Secrets Pvt Ltd - Kolkata' THEN 'Kolkata'
                WHEN Location = 'Heavenly Secrets Private Limited - Mumbai - B2B' THEN 'Mumbai B2B'
                ELSE Location
              END AS Warehouse,
              SUM(CAST(Total_Blocked_Inventory AS FLOAT64)) AS Blocked_Inventory
            FROM `shopify-pubsub-project.adhoc_data_asia.BlockedInv`
            WHERE REPLACE(REPLACE(TRIM(SKU), "'", ""), "‘", "") = '{sku}'
            GROUP BY Warehouse, Clean_SKU
        ),

        -- Available inventory from Live Inventory
        available AS (
            SELECT
              CASE 
                WHEN Company_Name = 'Heavenly Secrets Private Limited - Emiza Bilaspur' THEN 'Bilaspur'
                WHEN Company_Name = 'Heavenly Secrets Private Limited - Bangalore' THEN 'Bangalore'
                WHEN Company_Name = 'Heavenly Secrets Pvt Ltd - Kolkata' THEN 'Kolkata'
                WHEN Company_Name = 'Heavenly Secrets Private Limited - Mumbai - B2B' THEN 'Mumbai B2B'
                ELSE Company_Name
              END AS Warehouse,
              SAFE_CAST(Sku AS STRING) AS Sku,
              SUM(CAST(Quantity AS FLOAT64)) AS Total_Inventory
            FROM `shopify-pubsub-project.adhoc_data_asia.Live_Inventory_Report`
            WHERE SAFE_CAST(Sku AS STRING) = '{sku}'
              AND LOWER(CAST(Status AS STRING)) = 'available'
              AND (SAFE_CAST(GreaterThanEig AS BOOL) OR SAFE_CAST(GREATERTHANEIG AS BOOL)) = TRUE
            GROUP BY Warehouse, Sku
        ),

        -- Ensure all warehouses appear
        all_warehouses AS (
            SELECT DISTINCT Warehouse FROM blocked
            UNION DISTINCT
            SELECT DISTINCT Warehouse FROM available
        )

        SELECT
          w.Warehouse AS Company_Name,
          '{sku}' AS Sku,
          IFNULL(a.Total_Inventory, 0) AS Total_Inventory,
          IFNULL(b.Blocked_Inventory, 0) AS Blocked_Inventory,
          (IFNULL(a.Total_Inventory, 0) - IFNULL(b.Blocked_Inventory, 0)) AS Available_Inventory
        FROM all_warehouses w
        LEFT JOIN available a ON w.Warehouse = a.Warehouse
        LEFT JOIN blocked b ON w.Warehouse = b.Warehouse
        ORDER BY w.Warehouse
    """

    df = client.query(query).to_dataframe()
    if not df.empty:
        df["Blocked_%"] = (
            df["Blocked_Inventory"] / df["Total_Inventory"].replace(0, pd.NA) * 100
        ).fillna(0).round(1)
        df["Business_Loss_(₹)"] = df["Blocked_Inventory"] * 200

        total_row = pd.DataFrame({
            "Company_Name": ["TOTAL"],
            "Sku": [sku],
            "Total_Inventory": [df["Total_Inventory"].sum()],
            "Blocked_Inventory": [df["Blocked_Inventory"].sum()],
            "Available_Inventory": [df["Available_Inventory"].sum()],
            "Blocked_%": [
                (df["Blocked_Inventory"].sum() / df["Total_Inventory"].sum() * 100)
                if df["Total_Inventory"].sum() > 0 else 0
            ],
            "Business_Loss_(₹)": [df["Business_Loss_(₹)"].sum()]
        })
        df = pd.concat([df, total_row], ignore_index=True)

    return df.fillna(0)

# -------------------------------
# REST OF YOUR EXISTING CODE BELOW
# -------------------------------
# (unchanged business loss calculation, visualization, simulation, and layout)
# — This includes calculate_business_loss(), reshape_inventory(), pie chart, DOH highlights, etc.
# Everything else stays EXACTLY the same as your working version.

