import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import plotly.express as px

# ==============================================================
# 🔐 BigQuery Authentication
# ==============================================================

@st.cache_resource
def get_bq_client():
    creds_dict = st.secrets["bigquery"]  # <-- Make sure your Streamlit secrets.toml has [bigquery] JSON creds
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=credentials, project=creds_dict["project_id"])
    return client

client = get_bq_client()

# ==============================================================
# 🗂️ Warehouse summary query (Fixed Locked issue)
# ==============================================================

@st.cache_data(ttl=300)
def fetch_warehouse_summary(sku):
    query = f"""
        SELECT 
          Company_Name,
          SUM(CAST(Quantity AS FLOAT64)) AS Total_Quantity,
          SUM(
            CASE 
              WHEN LOWER(CAST(Locked AS STRING)) = 'true' THEN CAST(Quantity AS FLOAT64)
              ELSE 0 
            END
          ) AS Blocked_Quantity,
          SUM(
            CASE 
              WHEN LOWER(CAST(Locked AS STRING)) = 'false' THEN CAST(Quantity AS FLOAT64)
              ELSE 0 
            END
          ) AS Available_Quantity
        FROM `shopify-pubsub-project.adhoc_data_asia.Live_Inventory_Report`
        WHERE SAFE_CAST(Sku AS STRING) = '{sku}'
          AND SAFE_CAST(Quantity AS FLOAT64) IS NOT NULL
        GROUP BY Company_Name
        ORDER BY Total_Quantity DESC
    """
    df = client.query(query).to_dataframe()
    return df

# ==============================================================
# 📈 Inventory Trend (based on Manufacturing_Date)
# ==============================================================

@st.cache_data(ttl=300)
def fetch_inventory_trend(sku):
    query = f"""
        SELECT
          DATE(Manufacturing_Date) AS Date,
          SUM(CAST(Quantity AS FLOAT64)) AS Total_Inventory
        FROM `shopify-pubsub-project.adhoc_data_asia.Live_Inventory_Report`
        WHERE SAFE_CAST(Sku AS STRING) = '{sku}'
          AND SAFE_CAST(Quantity AS FLOAT64) IS NOT NULL
        GROUP BY Date
        ORDER BY Date
    """
    df = client.query(query).to_dataframe()
    return df

# ==============================================================
# 🧠 Sample product data fetch for debug
# ==============================================================

@st.cache_data(ttl=300)
def fetch_sample_data(sku):
    query = f"""
        SELECT *
        FROM `shopify-pubsub-project.adhoc_data_asia.Live_Inventory_Report`
        WHERE SAFE_CAST(Sku AS STRING) = '{sku}'
        LIMIT 5
    """
    df = client.query(query).to_dataframe()
    return df

# ==============================================================
# 🎨 Streamlit UI
# ==============================================================

st.set_page_config(page_title="Inventory & Business Loss Dashboard", layout="wide")

st.title("📊 Pilgrim Inventory & Business Loss Dashboard")

sku_input = st.text_input("Enter SKU to inspect:", placeholder="e.g. PGSL-SRAHS1")

if sku_input:
    st.subheader(f"📦 Warehouse Summary for SKU: {sku_input}")

    try:
        summary_df = fetch_warehouse_summary(sku_input)
        trend_df = fetch_inventory_trend(sku_input)
        sample_df = fetch_sample_data(sku_input)

        # --- Warehouse Summary Table ---
        if not summary_df.empty:
            st.dataframe(summary_df, use_container_width=True)
        else:
            st.warning("⚠️ No warehouse summary data found for this SKU.")

        # --- Inventory Trend Chart ---
        st.subheader("📈 Inventory Trend Over Time")
        if not trend_df.empty:
            fig2 = px.line(
                trend_df,
                x="Date",
                y="Total_Inventory",
                markers=True,
                title=f"📈 Inventory Trend Over Time – {sku_input}"
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.warning("⚠️ No trend data available for this SKU.")

        # --- Debug Section ---
        with st.expander("🧠 Debug Preview"):
            st.markdown(
                f"**Debug query:** `SELECT * FROM shopify-pubsub-project.adhoc_data_asia.Live_Inventory_Report WHERE Sku = '{sku_input}' LIMIT 5`"
            )
            if not sample_df.empty:
                st.markdown("📋 **Sample data:**")
                st.dataframe(sample_df, use_container_width=True)
            else:
                st.warning("No sample data available for debug.")

    except Exception as e:
        st.error(f"❌ Error: {e}")

else:
    st.info("Enter a SKU code above to start analysis.")

# ==============================================================
# 🧾 Notes:
# - Available_Quantity = Total - Blocked
# - Trend is calculated by grouping inventory by Manufacturing_Date
# - Locked column is normalized for mixed boolean/string cases
# ==============================================================
