import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account

# ===============================================================
# 🔐 BIGQUERY CLIENT
# ===============================================================
@st.cache_resource
def get_bq_client():
    creds_dict = st.secrets["bigquery"]
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=credentials, project=creds_dict["project_id"])
    return client

client = get_bq_client()


# ===============================================================
# 🧮 FETCH WAREHOUSE INVENTORY SUMMARY (for collapsible SKU)
# ===============================================================
@st.cache_data(ttl=300)
def fetch_warehouse_summary(sku):
    query = f"""
        SELECT 
          Company_Name,
          SUM(CAST(Quantity AS FLOAT64)) AS Total_Inventory,
          SUM(
            CASE 
              WHEN LOWER(CAST(Locked AS STRING)) = 'true' THEN CAST(Quantity AS FLOAT64)
              ELSE 0 
            END
          ) AS Blocked_Inventory,
          SUM(
            CASE 
              WHEN LOWER(CAST(Locked AS STRING)) = 'false' THEN CAST(Quantity AS FLOAT64)
              ELSE 0 
            END
          ) AS Available_Inventory
        FROM `shopify-pubsub-project.adhoc_data_asia.Live_Inventory_Report`
        WHERE SAFE_CAST(Sku AS STRING) = '{sku}'
          AND SAFE_CAST(Quantity AS FLOAT64) IS NOT NULL
        GROUP BY Company_Name
        ORDER BY Total_Inventory DESC
    """
    df = client.query(query).to_dataframe()
    df["Business_Loss_(₹)"] = df["Blocked_Inventory"] * 200  # Example loss logic
    return df.fillna(0)


# ===============================================================
# 📊 MAIN APP LAYOUT
# ===============================================================
st.set_page_config(page_title="Pilgrim Business Loss Dashboard", layout="wide")

st.title("💰 Pilgrim Business Loss Dashboard (with Live Inventory Drilldown)")

# --- File Uploads ---
uploaded_drr = st.file_uploader("📤 Upload ARR/DRR Sheet (Excel)", type=["xlsx"])
uploaded_inv = st.file_uploader("📤 Upload Inventory Sheet (Excel)", type=["xlsx"])

if uploaded_drr and uploaded_inv:
    try:
        drr_df = pd.read_excel(uploaded_drr, sheet_name=None)
        inv_df = pd.read_excel(uploaded_inv, sheet_name=None)

        # Automatically pick the first sheet if names differ
        arr_drr = next(iter(drr_df.values()))
        inv_data = next(iter(inv_df.values()))

        arr_drr.columns = arr_drr.columns.str.lower().str.replace(" ", "_")
        inv_data.columns = inv_data.columns.str.lower().str.replace(" ", "_")

        # Merge on SKU or variant_id depending on sheet
        merged = pd.merge(
            inv_data,
            arr_drr,
            how="left",
            left_on="sku_code" if "sku_code" in inv_data.columns else "sku",
            right_on="sku_code" if "sku_code" in arr_drr.columns else "sku",
        )

        # Compute derived metrics
        merged["business_loss"] = merged["drr"] * merged["days_out_of_stock"]
        merged["business_loss"] = merged["business_loss"].fillna(0)
        merged["business_loss"] = merged["business_loss"].round(0)

        # Display table
        st.subheader("📈 Business Loss Summary")
        styled_df = merged[["variant_id", "product_title", "days_out_of_stock", "drr", "asp", "business_loss"]]
        st.dataframe(styled_df, use_container_width=True)

        # --- Collapsible drilldown for each SKU ---
        st.subheader("🔍 Click below any SKU for live warehouse-level breakdown")

        for i, row in styled_df.iterrows():
            with st.expander(f"▶️ {row['product_title']} ({row['variant_id']})"):
                sku_code = str(row.get("sku_code", "") or row.get("variant_id", ""))
                st.markdown(f"**Fetching warehouse data for SKU:** `{sku_code}` ...")
                try:
                    warehouse_df = fetch_warehouse_summary(sku_code)
                    if not warehouse_df.empty:
                        st.dataframe(
                            warehouse_df.style.format({
                                "Total_Inventory": "{:,.0f}",
                                "Blocked_Inventory": "{:,.0f}",
                                "Available_Inventory": "{:,.0f}",
                                "Business_Loss_(₹)": "₹{:,.0f}"
                            }),
                            use_container_width=True
                        )
                    else:
                        st.info("No warehouse data found for this SKU.")
                except Exception as e:
                    st.error(f"Error fetching warehouse data: {e}")

        # Optional total summary
        total_loss = merged["business_loss"].sum()
        st.metric("💸 Total Estimated Business Loss (ARR/DRR Data)", f"₹{total_loss:,.0f}")

    except Exception as e:
        st.error(f"❌ Error processing files: {e}")
else:
    st.info("Please upload ARR/DRR and Inventory sheets to begin.")


# ===============================================================
# 🧾 Notes:
# - Click any SKU in the table to expand warehouse-wise summary.
# - Warehouse data is fetched live from BigQuery.
# - Business loss formula for BigQuery can be modified as needed.
# ===============================================================
