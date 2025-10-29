import pandas as pd
import streamlit as st
import plotly.express as px
import math
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
    client = bigquery.Client(credentials=credentials, project=creds_dict["project_id"])
    return client

client = get_bq_client()

@st.cache_data(ttl=300)
def fetch_warehouse_summary(sku):
    """Fetch warehouse-wise inventory from BigQuery for a given SKU"""
    query = f"""
        SELECT 
          Company_Name,
          SUM(CAST(Quantity AS FLOAT64)) AS Total_Inventory,
          SUM(CASE WHEN LOWER(CAST(Locked AS STRING)) = 'true' THEN CAST(Quantity AS FLOAT64) ELSE 0 END) AS Blocked_Inventory,
          SUM(CASE WHEN LOWER(CAST(Locked AS STRING)) = 'false' THEN CAST(Quantity AS FLOAT64) ELSE 0 END) AS Available_Inventory
        FROM `shopify-pubsub-project.adhoc_data_asia.Live_Inventory_Report`
        WHERE SAFE_CAST(Sku AS STRING) = '{sku}'
        GROUP BY Company_Name
        ORDER BY Total_Inventory DESC
    """
    df = client.query(query).to_dataframe()
    if not df.empty:
        df["Blocked_%"] = (df["Blocked_Inventory"] / df["Total_Inventory"] * 100).round(1)
        df["Business_Loss_(₹)"] = df["Blocked_Inventory"] * 200  # Example placeholder
    return df.fillna(0)

# -------------------------------
# HELPERS
# -------------------------------
def clean_id(v):
    v = str(v).strip().replace(".0", "").replace(".00", "")
    return v

def clean_sku(s):
    s = str(s).strip().replace(".0", "").replace(".00", "").upper()
    return s

def reshape_inventory(sheet_url, start_date=None, end_date=None):
    df = pd.read_csv(sheet_url, header=[0, 1])
    new_cols, last_variant = [], None
    for top, sub in df.columns:
        top, sub = str(top).strip().lower(), str(sub).strip().lower()
        if "unnamed" not in top and top != "time stamp":
            last_variant = top
        if sub in ["status", "inventory"]:
            new_cols.append(f"{last_variant}_{sub}" if last_variant else sub)
        elif top == "time stamp":
            new_cols.append("timestamp")
        else:
            new_cols.append(f"{top}_{sub}".strip("_"))
    df.columns = new_cols
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["date"] = df["timestamp"].dt.date
    long_df = df.melt(id_vars=["timestamp", "date"], var_name="variant_field", value_name="value")
    long_df[["variant_id", "field"]] = long_df["variant_field"].str.rsplit("_", n=1, expand=True)
    tidy = long_df.pivot_table(index=["timestamp", "date", "variant_id"], columns="field",
                               values="value", aggfunc="first").reset_index()
    tidy.columns = [str(c).strip().lower() for c in tidy.columns]
    tidy["inventory"] = pd.to_numeric(tidy.get("inventory", 0), errors="coerce").fillna(0)
    tidy["status"] = tidy.get("status", "").astype(str).str.lower()
    if start_date:
        tidy = tidy[tidy["date"] >= pd.to_datetime(start_date).date()]
    if end_date:
        tidy = tidy[tidy["date"] <= pd.to_datetime(end_date).date()]
    return tidy

# -------------------------------
# BUSINESS LOSS CALCULATION
# -------------------------------
def calculate_business_loss(inventory_url, arr_drr_url, b2b_url, start_date, end_date, show_debug=False):
    tidy = reshape_inventory(inventory_url, start_date, end_date)
    tidy = tidy[tidy["status"] == "active"]

    oos_days = tidy[tidy["inventory"] == 0].groupby("variant_id").size().reset_index(name="days_out_of_stock")
    latest_inv = tidy.sort_values("timestamp").groupby("variant_id").tail(1)[["variant_id", "inventory"]]
    latest_inv.rename(columns={"inventory": "latest_inventory"}, inplace=True)
    report = pd.merge(oos_days, latest_inv, on="variant_id", how="outer").fillna(0)

    arr_drr = pd.read_csv(arr_drr_url)
    arr_drr.columns = arr_drr.columns.str.strip().str.lower().str.replace(" ", "_")
    arr_drr.rename(columns={"sku_code": "sku"}, inplace=True)
    arr_drr["variant_id"] = arr_drr["variant_id"].apply(clean_id)
    arr_drr["sku"] = arr_drr["sku"].apply(clean_sku)
    report["variant_id"] = report["variant_id"].apply(clean_id)

    b2b_raw = pd.read_csv(b2b_url)
    b2b_raw.columns = b2b_raw.columns.map(str).str.strip().str.upper()
    date_mask = b2b_raw.iloc[:, 0].astype(str).str.match(r"\d{2}-\d{2}")
    b2b_data = b2b_raw[date_mask].copy()
    b2b_data["parsed_date"] = pd.to_datetime(b2b_data.iloc[:, 0], format="%d-%m", errors="coerce")
    latest_row = b2b_data.loc[b2b_data["parsed_date"].idxmax()]
    b2b_latest = latest_row.drop(labels=["parsed_date"]).reset_index()
    b2b_latest.columns = ["sku", "b2b_inventory"]
    b2b_latest["sku"] = b2b_latest["sku"].apply(clean_sku)
    b2b_latest["b2b_inventory"] = pd.to_numeric(b2b_latest["b2b_inventory"], errors="coerce").fillna(0)

    report = pd.merge(report, arr_drr[["variant_id", "product_title", "drr", "asp", "sku"]],
                      on="variant_id", how="left")
    report["sku"] = report["sku"].apply(clean_sku)
    report = pd.merge(report, b2b_latest, on="sku", how="left").fillna(0)

    for col in ["drr", "asp", "latest_inventory"]:
        report[col] = pd.to_numeric(report[col], errors="coerce").fillna(0)
    report["business_loss"] = report["days_out_of_stock"] * report["drr"] * report["asp"]
    report["doh"] = report.apply(lambda x: math.ceil(x["latest_inventory"]/x["drr"]) if x["drr"] > 0 else 0, axis=1)
    report["variant_label"] = report.apply(
        lambda x: f"{x['product_title']} ({x['variant_id']})" if pd.notna(x["product_title"]) else str(x["variant_id"]),
        axis=1
    )

    if show_debug:
        with st.expander("🧩 Debug Preview", expanded=False):
            st.dataframe(report.head(10))

    return report.fillna(0)

# -------------------------------
# STREAMLIT DASHBOARD
# -------------------------------
st.set_page_config(page_title="Business Loss Dashboard", layout="wide")
st.title("💸 Business Loss Dashboard (with BigQuery Drilldown)")

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date")
with col2:
    end_date = st.date_input("End Date")

show_debug = st.toggle("Show Debug Info", value=False)

if st.button("🚀 Calculate Business Loss"):
    with st.spinner("Crunching numbers... please wait ⏳"):
        report = calculate_business_loss(INVENTORY_URL, ARR_DRR_URL, B2B_URL, start_date, end_date, show_debug)
    st.session_state["report"] = report

report = st.session_state.get("report", None)

# -------------------------------
# VISUALIZATION SECTION
# -------------------------------
if report is not None and not report.empty:
    st.subheader("📊 Business Loss Summary Metrics")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Unique Variants", report["variant_id"].nunique())
    c2.metric("Total OOS Days", int(report["days_out_of_stock"].sum()))
    c3.metric("Avg DRR", round(report["drr"].mean(), 1))
    c4.metric("Total Business Loss", f"₹{report['business_loss'].sum():,.0f}")

    # Highlight DOH
    def highlight_doh(row):
        if row["latest_inventory"] == 0 or row["doh"] <= 7:
            color = "background-color: #FFC7C7"
        elif 8 <= row["doh"] <= 15:
            color = "background-color: #FFF6A5"
        else:
            color = ""
        return [color]*len(row)

    st.markdown("### 🧾 Variant-wise Business Loss")
    styled_df = (
        report[["variant_label", "latest_inventory", "b2b_inventory", "doh",
                "days_out_of_stock", "drr", "asp", "business_loss"]]
        .style.apply(highlight_doh, axis=1)
        .format({"latest_inventory": "{:.0f}", "b2b_inventory": "{:.0f}",
                 "drr": "{:.1f}", "asp": "₹{:.0f}", "business_loss": "₹{:.0f}"})
    )
    st.dataframe(styled_df, use_container_width=True)

    # --- NEW SECTION: EXPANDABLE SKU DRILLDOWN ---
    st.markdown("---")
    st.subheader("🏭 Live Warehouse Inventory (from BigQuery)")

    for _, row in report.iterrows():
        sku = str(row["sku"])
        product = row["variant_label"]
        with st.expander(f"🔍 {product}"):
            try:
                warehouse_df = fetch_warehouse_summary(sku)
                if not warehouse_df.empty:
                    st.dataframe(
                        warehouse_df.style.format({
                            "Total_Inventory": "{:,.0f}",
                            "Blocked_Inventory": "{:,.0f}",
                            "Available_Inventory": "{:,.0f}",
                            "Blocked_%": "{:.1f}%",
                            "Business_Loss_(₹)": "₹{:,.0f}"
                        }),
                        use_container_width=True
                    )
                else:
                    st.info("No warehouse data found for this SKU.")
            except Exception as e:
                st.error(f"Error fetching warehouse data: {e}")
else:
    st.info("Please calculate business loss first using the 🚀 button.")
