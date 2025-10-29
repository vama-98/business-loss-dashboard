import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# ==============================================================
# 🔐 BigQuery Authentication
# ==============================================================

@st.cache_resource
def get_bq_client():
    """Create a cached BigQuery client using Streamlit secrets."""
    creds_dict = st.secrets["bigquery"]
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=credentials, project=creds_dict["project_id"])
    return client

client = get_bq_client()

# ==============================================================
# 📦 Warehouse Summary Function
# ==============================================================

@st.cache_data(ttl=300)
def fetch_warehouse_summary(sku):
    """
    Fetch warehouse-wise total, blocked, and available inventory for a given SKU.
    Also compute business loss using a placeholder logic (customizable).
    """
    query = f"""
        SELECT 
          Company_Name,
          SAFE_CAST(SUM(CAST(Quantity AS FLOAT64)) AS FLOAT64) AS Total_Inventory,
          SAFE_CAST(SUM(
            CASE 
              WHEN LOWER(CAST(Locked AS STRING)) = 'true' THEN CAST(Quantity AS FLOAT64)
              ELSE 0 
            END
          ) AS FLOAT64) AS Blocked_Inventory,
          SAFE_CAST(SUM(
            CASE 
              WHEN LOWER(CAST(Locked AS STRING)) = 'false' THEN CAST(Quantity AS FLOAT64)
              ELSE 0 
            END
          ) AS FLOAT64) AS Available_Inventory
        FROM `shopify-pubsub-project.adhoc_data_asia.Live_Inventory_Report`
        WHERE SAFE_CAST(Sku AS STRING) = '{sku}'
          AND SAFE_CAST(Quantity AS FLOAT64) IS NOT NULL
        GROUP BY Company_Name
        ORDER BY Total_Inventory DESC
    """
    df = client.query(query).to_dataframe()

    # Add business loss logic (example placeholder)
    # You can adjust this to use actual DRR/ASP data later
    df["Business_Loss_(₹)"] = df["Blocked_Inventory"] * 200  # example: ₹200 per blocked unit

    # Fill NaNs for clarity
    df = df.fillna(0)
    return df


# ==============================================================
# 🧠 Raw Sample Data for Debug
# ==============================================================

@st.cache_data(ttl=300)
def fetch_sample_data(sku):
    query = f"""
        SELECT *
        FROM `shopify-pubsub-project.adhoc_data_asia.Live_Inventory_Report`
        WHERE SAFE_CAST(Sku AS STRING) = '{sku}'
        LIMIT 10
    """
    df = client.query(query).to_dataframe()
    return df


# ==============================================================
# 🎨 Streamlit App UI
# ==============================================================

st.set_page_config(page_title="Pilgrim Warehouse Inventory Dashboard", layout="wide")

st.title("📊 Pilgrim Warehouse Inventory Dashboard")

sku_input = st.text_input("🔍 Enter SKU Code", placeholder="e.g. PGSL-SRAHS1")

if sku_input:
    try:
        st.subheader(f"📦 Warehouse Inventory Summary for SKU: `{sku_input}`")

        summary_df = fetch_warehouse_summary(sku_input)
        sample_df = fetch_sample_data(sku_input)

        # --- Warehouse Summary ---
        if not summary_df.empty:
            st.dataframe(
                summary_df.style.format({
                    "Total_Inventory": "{:,.0f}",
                    "Blocked_Inventory": "{:,.0f}",
                    "Available_Inventory": "{:,.0f}",
                    "Business_Loss_(₹)": "₹{:,.0f}"
                }),
                use_container_width=True
            )
        else:
            st.warning("⚠️ No warehouse data found for this SKU.")

        # --- Optional: Totals summary ---
        if not summary_df.empty:
            totals = {
                "Total_Inventory": summary_df["Total_Inventory"].sum(),
                "Blocked_Inventory": summary_df["Blocked_Inventory"].sum(),
                "Available_Inventory": summary_df["Available_Inventory"].sum(),
                "Business_Loss_(₹)": summary_df["Business_Loss_(₹)"].sum(),
            }
            st.metric("Total Inventory", f"{totals['Total_Inventory']:,.0f}")
            st.metric("Blocked Inventory", f"{totals['Blocked_Inventory']:,.0f}")
            st.metric("Available Inventory", f"{totals['Available_Inventory']:,.0f}")
            st.metric("Business Loss (₹)", f"₹{totals['Business_Loss_(₹)']:,.0f}")

        # --- Debug Data ---
        with st.expander("🧠 Debug Data Preview"):
            st.markdown(
                f"**Query executed:** `SELECT * FROM shopify-pubsub-project.adhoc_data_asia.Live_Inventory_Report WHERE Sku = '{sku_input}' LIMIT 10`"
            )
            if not sample_df.empty:
                st.dataframe(sample_df, use_container_width=True)
            else:
                st.warning("No sample data found for this SKU.")

    except Exception as e:
        st.error(f"❌ Error fetching data: {e}")

else:
    st.info("Enter a SKU code above to view warehouse-wise inventory summary.")

# ==============================================================
# 🧾 Notes
# - Available_Inventory = Total - Blocked
# - Business_Loss formula can be customized using DRR/ASP or other metrics.
# - All queries are run live from BigQuery.
# ==============================================================
