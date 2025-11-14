# Warehouse-wise Days on Hand (DOH) Dashboard

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
import math

# -------------------------------
# CONFIG
# -------------------------------
ARR_DRR_URL = "https://docs.google.com/spreadsheets/d/1nLdtjYwVD1AFa1VqCUlPS2W8t4lRYJnyMOwMX8sNkfU/export?format=csv&gid=1079657777"

# Warehouse DRR Attribution Percentages
WAREHOUSE_DRR_SPLIT = {
    "Bilaspur": 0.36,
    "Mumbai B2C": 0.27,
    "Bangalore": 0.20,
    "Kolkata": 0.17,
    "Mumbai B2B": 0.00  # No calculation for Mumbai B2B
}

# -------------------------------
# BIGQUERY CONNECTION
# -------------------------------
@st.cache_resource
def get_bq_client():
    """Initialize BigQuery client with credentials from Streamlit secrets"""
    creds_dict = st.secrets["bigquery"]
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    return bigquery.Client(credentials=credentials, project=creds_dict["project_id"])

client = get_bq_client()

# -------------------------------
# HELPER FUNCTIONS
# -------------------------------
def clean_sku(s):
    """Clean SKU by removing decimals and converting to uppercase"""
    return str(s).strip().replace(".0", "").replace(".00", "").upper()

def clean_id(v):
    """Clean variant ID by removing decimal points"""
    return str(v).strip().replace(".0", "").replace(".00", "")

# -------------------------------
# DATA FETCHING FUNCTIONS
# -------------------------------
@st.cache_data(ttl=300)
def fetch_all_warehouse_inventory():
    """Fetch all warehouse inventory from BigQuery"""
    query = """
        SELECT 
          Company_Name,
          SAFE_CAST(Sku AS STRING) AS SKU,
          SUM(CAST(Quantity AS FLOAT64)) AS Total_Inventory,
          SUM(
            CASE 
              WHEN LOWER(CAST(Status AS STRING)) = 'available'
                   AND LOWER(CAST(Greaterthaneig AS STRING)) = 'true'
                   AND LOWER(CAST(Locked AS STRING)) = 'false'
              THEN CAST(Quantity AS FLOAT64)
              ELSE 0
            END
          ) AS Available_Inventory
        FROM `shopify-pubsub-project.adhoc_data_asia.Live_Inventory_Report`
        WHERE LOWER(CAST(Status AS STRING)) = 'available'
              AND LOWER(CAST(Greaterthaneig AS STRING)) = 'true'
        GROUP BY Company_Name, SKU
        ORDER BY Company_Name, Total_Inventory DESC
    """
    df = client.query(query).to_dataframe()
    df["SKU"] = df["SKU"].apply(clean_sku)
    
    # Clean warehouse names to match our split dictionary
    warehouse_map = {
        "Heavenly Secrets Private Limited - Bangalore ": "Bangalore",
        "Heavenly Secrets Private Limited - Mumbai - B2B": "Mumbai B2B",
        "Heavenly Secrets Pvt Ltd - Kolkata": "Kolkata",
        "Heavenly Secrets Private Limited - Emiza Bilaspur": "Bilaspur",
    }
    df["Company_Name"] = df["Company_Name"].replace(warehouse_map)
    df["Company_Name"] = df["Company_Name"].astype(str).str.strip()
    
    # Map Mumbai B2C (if it exists in data with different name)
    df["Company_Name"] = df["Company_Name"].apply(
        lambda x: "Mumbai B2C" if "mumbai" in x.lower() and "b2b" not in x.lower() else x
    )
    
    return df.fillna(0)

@st.cache_data(ttl=600)
def load_drr_data():
    """Load DRR data from Google Sheets"""
    arr_drr = pd.read_csv(ARR_DRR_URL)
    
    # Keep original column names first to check
    original_cols = arr_drr.columns.tolist()
    
    # Now normalize column names
    arr_drr.columns = arr_drr.columns.str.strip().str.lower().str.replace(" ", "_")
    
    # Check for SKU column with various possible names
    sku_col_found = False
    for possible_name in ["sku_code", "sku", "sku_code", "skucode"]:
        if possible_name in arr_drr.columns:
            if possible_name != "sku":
                arr_drr.rename(columns={possible_name: "sku"}, inplace=True)
            sku_col_found = True
            break
    
    if not sku_col_found:
        st.error(f"SKU column not found! Available columns: {original_cols}")
        return pd.DataFrame()
    
    # Clean and process SKU
    arr_drr["sku"] = arr_drr["sku"].astype(str).apply(clean_sku)
    
    # Process DRR column
    if "drr" in arr_drr.columns:
        arr_drr["drr"] = pd.to_numeric(arr_drr["drr"], errors="coerce").fillna(0)
    else:
        st.error("DRR column not found in the sheet!")
        return pd.DataFrame()
    
    # Handle variant_id if exists
    if "variant_id" in arr_drr.columns:
        arr_drr["variant_id"] = arr_drr["variant_id"].apply(clean_id)
    
    # Handle product_title
    if "product_title" not in arr_drr.columns:
        arr_drr["product_title"] = "Unknown Product"
    
    # Keep only necessary columns and remove rows with zero/missing SKU or DRR
    result = arr_drr[["sku", "product_title", "drr"]].copy()
    result = result[(result["sku"] != "") & (result["sku"] != "0") & (result["drr"] > 0)]
    
    return result

# -------------------------------
# DOH CALCULATION
# -------------------------------
def calculate_warehouse_doh():
    """
    Calculate warehouse-wise Days on Hand (DOH) based on:
    - Warehouse inventory from BigQuery
    - DRR from Google Sheets
    - Warehouse-specific DRR attribution percentages
    """
    
    # Fetch data
    warehouse_inv = fetch_all_warehouse_inventory()
    drr_data = load_drr_data()
    
    # Ensure SKU columns are properly formatted before merge
    warehouse_inv["sku"] = warehouse_inv["SKU"].astype(str).str.strip().str.upper()
    drr_data["sku"] = drr_data["sku"].astype(str).str.strip().str.upper()
    
    # Merge inventory with DRR
    merged = pd.merge(
        warehouse_inv,
        drr_data[["sku", "product_title", "drr"]],
        on="sku",
        how="left"
    )
    
    # Fill missing DRR values with 0
    merged["drr"] = merged["drr"].fillna(0)
    merged["product_title"] = merged["product_title"].fillna("Unknown Product")
    
    # Calculate warehouse-specific DRR
    merged["warehouse_drr"] = merged.apply(
        lambda row: row["drr"] * WAREHOUSE_DRR_SPLIT.get(row["Company_Name"], 0),
        axis=1
    )
    
    # Calculate DOH: Available Inventory / Warehouse-specific DRR
    merged["doh"] = merged.apply(
        lambda row: math.ceil(row["Available_Inventory"] / row["warehouse_drr"]) 
        if row["warehouse_drr"] > 0 else 0,
        axis=1
    )
    
    # Add DRR percentage column for display
    merged["drr_percentage"] = merged["Company_Name"].map(
        lambda x: WAREHOUSE_DRR_SPLIT.get(x, 0) * 100
    )
    
    # Remove duplicate SKU column (keep the original SKU)
    if "SKU" in merged.columns:
        merged = merged.drop(columns=["SKU"])
    
    return merged

# -------------------------------
# STREAMLIT DASHBOARD
# -------------------------------
st.set_page_config(page_title="Warehouse DOH Dashboard", layout="wide")
st.title("🏭 Warehouse-wise Days on Hand (DOH) Dashboard")

st.markdown("""
This dashboard calculates **Days on Hand (DOH)** for each warehouse based on:
- **Available Inventory** from BigQuery
- **DRR (Daily Run Rate)** from Google Sheets
- **Warehouse-specific DRR attribution**: 
  - 36% Bilaspur | 27% Mumbai B2C | 20% Bangalore | 17% Kolkata | 0% Mumbai B2B
""")

# Add debug toggle
show_debug = st.sidebar.checkbox("Show Debug Info", value=False)

# Refresh button
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Load data
with st.spinner("Loading warehouse inventory and DRR data..."):
    doh_report = calculate_warehouse_doh()

# Debug section
if show_debug:
    st.sidebar.markdown("---")
    st.sidebar.subheader("🔍 Debug Info")
    
    warehouse_inv = fetch_all_warehouse_inventory()
    drr_data = load_drr_data()
    
    st.sidebar.write(f"**Warehouse Inventory rows:** {len(warehouse_inv)}")
    st.sidebar.write(f"**DRR Data rows:** {len(drr_data)}")
    st.sidebar.write(f"**Merged rows:** {len(doh_report)}")
    st.sidebar.write(f"**Matched SKUs:** {(doh_report['drr'] > 0).sum()}")
    st.sidebar.write(f"**Unmatched SKUs:** {(doh_report['drr'] == 0).sum()}")
    
    with st.expander("🔍 Sample SKUs from Warehouse"):
        st.write("First 10 SKUs from Warehouse:")
        st.write(warehouse_inv[["sku", "Company_Name", "Available_Inventory"]].head(10))
    
    with st.expander("🔍 Sample SKUs from DRR Sheet"):
        st.write("First 10 SKUs from DRR:")
        st.write(drr_data[["sku", "product_title", "drr"]].head(10))
    
    with st.expander("🔍 Sample Mismatched SKUs"):
        mismatched = doh_report[doh_report["drr"] == 0][["sku", "Company_Name", "Available_Inventory"]].head(10)
        st.write("SKUs with no DRR match:")
        st.write(mismatched)

if not doh_report.empty:
    # ------------------ SUMMARY METRICS ------------------
    st.subheader("📊 Summary Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    total_skus = doh_report["sku"].nunique()
    total_inventory = doh_report["Available_Inventory"].sum()
    avg_doh = doh_report[doh_report["doh"] > 0]["doh"].mean()
    warehouses_count = doh_report["Company_Name"].nunique()
    
    col1.metric("Total Unique SKUs", f"{total_skus:,}")
    col2.metric("Total Available Inventory", f"{total_inventory:,.0f}")
    col3.metric("Average DOH (All Warehouses)", f"{avg_doh:.1f} days")
    col4.metric("Active Warehouses", warehouses_count)
    
    # ------------------ FILTERS ------------------
    st.markdown("---")
    st.subheader("🎚️ Filters")
    
    col_f1, col_f2, col_f3 = st.columns(3)
    
    # Warehouse filter
    warehouses = ["All"] + sorted(doh_report["Company_Name"].unique().tolist())
    with col_f1:
        selected_warehouse = st.selectbox("Warehouse", options=warehouses)
    
    # SKU filter
    skus = ["All"] + sorted(doh_report["sku"].unique().tolist())
    with col_f2:
        selected_sku = st.selectbox("SKU", options=skus)
    
    # Product filter
    products = ["All"] + sorted(doh_report["product_title"].dropna().unique().tolist())
    with col_f3:
        selected_product = st.selectbox("Product", options=products)
    
    # Apply filters
    filtered_doh = doh_report.copy()
    
    if selected_warehouse != "All":
        filtered_doh = filtered_doh[filtered_doh["Company_Name"] == selected_warehouse]
    
    if selected_sku != "All":
        filtered_doh = filtered_doh[filtered_doh["sku"] == selected_sku]
    
    if selected_product != "All":
        filtered_doh = filtered_doh[filtered_doh["product_title"] == selected_product]
    
    # ------------------ MAIN TABLE ------------------
    st.markdown("---")
    st.subheader(f"📋 Warehouse-wise DOH Table ({len(filtered_doh)} rows)")
    
    # Prepare display columns
    display_cols = [
        "Company_Name", "product_title", "sku", 
        "Available_Inventory", "drr", "drr_percentage", "warehouse_drr", "doh"
    ]
    
    rename_map = {
        "Company_Name": "Warehouse",
        "product_title": "Product Title",
        "sku": "SKU",
        "Available_Inventory": "Available Inventory",
        "drr": "Total DRR",
        "drr_percentage": "DRR % for Warehouse",
        "warehouse_drr": "Warehouse DRR",
        "doh": "DOH (Days)"
    }
    
    # Ensure columns exist
    for c in display_cols:
        if c not in filtered_doh.columns:
            filtered_doh[c] = ""
    
    # Display table
    st.dataframe(
        filtered_doh[display_cols].rename(columns=rename_map).style.format({
            "Available Inventory": "{:,.0f}",
            "Total DRR": "{:.0f}",
            "DRR % for Warehouse": "{:.0f}%",
            "Warehouse DRR": "{:.2f}",
            "DOH (Days)": "{:.0f}"
        }),
        use_container_width=True,
        height=500
    )
    
    # ------------------ VISUALIZATIONS ------------------
    st.markdown("---")
    st.subheader("📈 Visualizations")
    
    viz_col1, viz_col2 = st.columns(2)
    
    # Chart 1: Average DOH by Warehouse
    with viz_col1:
        st.markdown("##### Average DOH by Warehouse")
        avg_doh_by_wh = filtered_doh.groupby("Company_Name")["doh"].mean().reset_index()
        avg_doh_by_wh = avg_doh_by_wh[avg_doh_by_wh["doh"] > 0]
        
        if not avg_doh_by_wh.empty:
            fig1 = px.bar(
                avg_doh_by_wh,
                x="Company_Name",
                y="doh",
                title="Average Days on Hand by Warehouse",
                labels={"Company_Name": "Warehouse", "doh": "Avg DOH (Days)"},
                color="doh",
                color_continuous_scale="RdYlGn_r"
            )
            fig1.update_layout(showlegend=False)
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.info("No data available for visualization")
    
    # Chart 2: Inventory Distribution by Warehouse
    with viz_col2:
        st.markdown("##### Inventory Distribution by Warehouse")
        inv_by_wh = filtered_doh.groupby("Company_Name")["Available_Inventory"].sum().reset_index()
        
        if not inv_by_wh.empty:
            fig2 = px.pie(
                inv_by_wh,
                names="Company_Name",
                values="Available_Inventory",
                title="Available Inventory Distribution"
            )
            fig2.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No data available for visualization")
    
    # ------------------ DOH HEALTH INDICATORS ------------------
    st.markdown("---")
    st.subheader("🚦 DOH Health Indicators")
    st.markdown("""
    **Color coding:**
    - 🟢 **Green (>30 days)**: Healthy stock levels
    - 🟡 **Yellow (15-30 days)**: Monitor closely
    - 🔴 **Red (<15 days)**: Low stock alert
    - ⚫ **Gray (0 days)**: Out of stock
    """)
    
    # Calculate health categories
    health_summary = filtered_doh.copy()
    health_summary["health_status"] = health_summary["doh"].apply(
        lambda x: "🟢 Healthy (>30)" if x > 30 
        else "🟡 Monitor (15-30)" if x >= 15 
        else "🔴 Low (<15)" if x > 0 
        else "⚫ Out of Stock"
    )
    
    health_counts = health_summary["health_status"].value_counts().reset_index()
    health_counts.columns = ["Status", "Count"]
    
    col_h1, col_h2 = st.columns([1, 2])
    
    with col_h1:
        st.dataframe(health_counts, use_container_width=True, hide_index=True)
    
    with col_h2:
        fig3 = px.bar(
            health_counts,
            x="Status",
            y="Count",
            title="SKU Count by DOH Health Status",
            color="Status",
            color_discrete_map={
                "🟢 Healthy (>30)": "green",
                "🟡 Monitor (15-30)": "gold",
                "🔴 Low (<15)": "red",
                "⚫ Out of Stock": "gray"
            }
        )
        st.plotly_chart(fig3, use_container_width=True)
    
    # ------------------ DOWNLOAD OPTION ------------------
    st.markdown("---")
    st.subheader("💾 Download Data")
    
    csv = filtered_doh[display_cols].rename(columns=rename_map).to_csv(index=False)
    st.download_button(
        label="📥 Download Warehouse DOH Report (CSV)",
        data=csv,
        file_name="warehouse_doh_report.csv",
        mime="text/csv"
    )

else:
    st.warning("No data available. Please check your BigQuery connection and data sources.")

# ------------------ FOOTER ------------------
st.markdown("---")
st.caption("🏭 Warehouse DOH Dashboard | Data refreshed from BigQuery and Google Sheets")
