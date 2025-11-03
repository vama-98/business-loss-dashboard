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
# SKU INPUT CLEANING
# -------------------------------
def clean_sku_input(text):
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", str(text))
    text = text.encode("ascii", "ignore").decode("ascii")
    return text.strip().upper()

# -------------------------------
# BIGQUERY: WAREHOUSE SUMMARY
# -------------------------------
@st.cache_data(ttl=300)
def fetch_warehouse_summary(sku):
    sku = clean_sku_input(sku)
    query = f"""
        -- Clean and standardize blocked data
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
# DASHBOARD UI
# -------------------------------
st.set_page_config(page_title="Business Loss Dashboard", layout="wide")
st.title("💸 Business Loss Dashboard (with BigQuery Drilldown + Simulation)")

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date")
with col2:
    end_date = st.date_input("End Date")

show_debug = st.toggle("Show Debug Info", value=False)

if st.button("🚀 Calculate Business Loss"):
    from datetime import date
    st.session_state["report"] = pd.DataFrame({
        "variant_label": ["Squalane Glow Serum (42604012765413)", "Hair Regrowth Kit (42850642985189)"],
        "sku": ["PGSGCSERUM1", "PGK-HRKITNS1"],
        "latest_inventory": [2960, 0],
        "b2b_inventory": [964, 0],
        "doh": [124, 0],
        "days_out_of_stock": [0, 2],
        "drr": [24.7, 6.5],
        "asp": [540, 300],
        "business_loss": [0, 3900],
    })

report = st.session_state.get("report", None)

if report is not None and not report.empty:
    st.subheader("📊 Business Loss Summary Metrics")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Unique Variants", report["variant_label"].nunique())
    c2.metric("Total OOS Days", int(report["days_out_of_stock"].sum()))
    c3.metric("Avg DRR", round(report["drr"].mean(), 1))
    c4.metric("Total Business Loss", f"₹{report['business_loss'].sum():,.0f}")

    st.markdown("### 🧾 Variant-wise Business Loss")
    st.dataframe(report, use_container_width=True)

    st.markdown("### 🥧 Contribution to Total Business Loss")
    pie_df = report[report["business_loss"] > 0]
    if not pie_df.empty:
        fig = px.pie(pie_df, names="variant_label", values="business_loss",
                     title="Contribution to Total Business Loss",
                     color_discrete_sequence=px.colors.sequential.RdBu)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("🏭 Live Warehouse Breakdown from BigQuery")
    sku_options = report["sku"].unique().tolist()
    selected_sku = st.selectbox("Select SKU for Warehouse Breakdown:", options=sku_options)

    if selected_sku:
        st.info(f"Fetching live warehouse data for SKU: `{selected_sku}`")
        try:
            warehouse_df = fetch_warehouse_summary(selected_sku)
            if not warehouse_df.empty:
                def highlight_blocked(val):
                    color = "#FF9999" if val > 50 else "#FFF6A5" if val > 20 else "#C6F6C6"
                    return f"background-color: {color}"

                st.dataframe(
                    warehouse_df.style.applymap(highlight_blocked, subset=["Blocked_%"]).format({
                        "Total_Inventory": "{:,.0f}",
                        "Blocked_Inventory": "{:,.0f}",
                        "Available_Inventory": "{:,.0f}",
                        "Blocked_%": "{:.1f}%",
                        "Business_Loss_(₹)": "₹{:,.0f}"
                    }),
                    use_container_width=True
                )
            else:
                st.warning("No warehouse data found for this SKU in BigQuery.")
        except Exception as e:
            st.error(f"Error fetching warehouse data: {e}")
else:
    st.info("Please calculate business loss first using the 🚀 button.")
