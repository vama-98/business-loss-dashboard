# This Works for Real — Final Version with Blocked Inventory Section

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
    return bigquery.Client(credentials=credentials, project=creds_dict["project_id"])

client = get_bq_client()

@st.cache_data(ttl=300)
def fetch_warehouse_summary(sku):
    query = f"""
        SELECT 
          Company_Name,
          SUM(CAST(Quantity AS FLOAT64)) AS Total_Inventory,
          SUM(
            CASE 
              WHEN LOWER(CAST(Locked AS STRING)) = 'true'
                   AND LOWER(CAST(Status AS STRING)) = 'available'
                   AND LOWER(CAST(Greaterthaneig AS STRING)) = 'true'
              THEN CAST(Quantity AS FLOAT64)
              ELSE 0
            END
          ) AS Blocked_Inventory,
          SUM(
            CASE 
              WHEN LOWER(CAST(Locked AS STRING)) = 'false'
                   AND LOWER(CAST(Status AS STRING)) = 'available'
                   AND LOWER(CAST(Greaterthaneig AS STRING)) = 'true'
              THEN CAST(Quantity AS FLOAT64)
              ELSE 0
            END
          ) AS Available_Inventory
        FROM `shopify-pubsub-project.adhoc_data_asia.Live_Inventory_Report`
        WHERE SAFE_CAST(Sku AS STRING) = '{sku}'
              AND LOWER(CAST(Status AS STRING)) = 'available'
              AND LOWER(CAST(Greaterthaneig AS STRING)) = 'true'
        GROUP BY Company_Name
        ORDER BY Total_Inventory DESC
    """
    df = client.query(query).to_dataframe()
    if not df.empty:
        df["Blocked_%"] = (df["Blocked_Inventory"] / df["Total_Inventory"] * 100).round(1)
        df["Business_Loss_(₹)"] = df["Blocked_Inventory"] * 200  # placeholder metric
    return df.fillna(0)

# -------------------------------
# HELPERS
# -------------------------------
def clean_id(v):
    return str(v).strip().replace(".0", "").replace(".00", "")

def clean_sku(s):
    return str(s).strip().replace(".0", "").replace(".00", "").upper()

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

    # === ARR/DRR ===
    arr_drr = pd.read_csv(arr_drr_url)
    arr_drr.columns = arr_drr.columns.str.strip().str.lower().str.replace(" ", "_")
    arr_drr.rename(columns={"sku_code": "sku"}, inplace=True)
    arr_drr["variant_id"] = arr_drr["variant_id"].apply(clean_id)
    arr_drr["sku"] = arr_drr["sku"].apply(clean_sku)
    report["variant_id"] = report["variant_id"].apply(clean_id)

    # === B2B SHEET PARSING (Row-based layout) ===
    b2b_raw = pd.read_csv(b2b_url, header=None)
    b2b_raw = b2b_raw.applymap(lambda x: str(x).strip() if pd.notna(x) else "")
    meta_raw = b2b_raw.iloc[:5, :]
    data_raw = b2b_raw.iloc[5:, :]
    meta_t = meta_raw.T
    meta_t.columns = meta_t.iloc[0]
    meta_t = meta_t.drop(meta_t.index[0])
    meta_t.rename(columns={
        "SKU Code": "sku",
        "Product Name": "product_name_b2b",
        "Size": "size_b2b",
        "CATEGORY": "category_b2b",
        "Range": "range_b2b"
    }, inplace=True)
    meta_t["sku"] = meta_t["sku"].apply(clean_sku)
    data_raw.columns = b2b_raw.iloc[0, :]
    date_mask = data_raw.iloc[:, 0].astype(str).str.match(r"\d{2}-\d{2}")
    data_dates = data_raw[date_mask].copy()
    if not data_dates.empty:
        data_dates["parsed_date"] = pd.to_datetime(data_dates.iloc[:, 0], format="%d-%m", errors="coerce")
        latest_row = data_dates.loc[data_dates["parsed_date"].idxmax()]
        b2b_latest = latest_row.drop(labels=["parsed_date"]).reset_index()
        b2b_latest.columns = ["sku", "b2b_inventory"]
    else:
        b2b_latest = pd.DataFrame(columns=["sku", "b2b_inventory"])
    b2b_latest["sku"] = b2b_latest["sku"].apply(clean_sku)
    b2b_latest["b2b_inventory"] = pd.to_numeric(b2b_latest["b2b_inventory"], errors="coerce").fillna(0)
    b2b_enriched = pd.merge(b2b_latest, meta_t, on="sku", how="left")

    # === Merge ARR/DRR and B2B ===
    report = pd.merge(report, arr_drr[["variant_id", "product_title", "drr", "asp", "sku"]],
                      on="variant_id", how="left")
    report["sku"] = report["sku"].apply(clean_sku)
    report = pd.merge(report, b2b_enriched, on="sku", how="left").fillna(0)

    for col in ["drr", "asp", "latest_inventory"]:
        report[col] = pd.to_numeric(report[col], errors="coerce").fillna(0)

    report["business_loss"] = report["days_out_of_stock"] * report["drr"] * report["asp"]
    report["doh"] = report.apply(lambda x: math.ceil(x["latest_inventory"]/x["drr"]) if x["drr"] > 0 else 0, axis=1)
    report["variant_label"] = report.apply(
        lambda x: f"{x['product_title']} ({x['variant_id']})" if pd.notna(x["product_title"]) else str(x["variant_id"]),
        axis=1
    )
    return report.fillna(0)

# -------------------------------
# STREAMLIT DASHBOARD
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

    def highlight_doh(row):
        if row["latest_inventory"] == 0 or row["doh"] <= 7:
            color = "background-color: #FFC7C7"
        elif 8 <= row["doh"] <= 15:
            color = "background-color: #FFF6A5"
        else:
            color = ""
        return [color]*len(row)

    st.markdown("### 🧾 Variant-wise Business Loss")
    display_cols = [
        "variant_label",
        "sku", "product_name_b2b", "size_b2b", "category_b2b", "range_b2b",
        "latest_inventory", "b2b_inventory", "doh",
        "days_out_of_stock", "drr", "asp", "business_loss"
    ]
    for c in display_cols:
        if c not in report.columns:
            report[c] = ""
    styled_df = (
        report[display_cols]
        .style.apply(highlight_doh, axis=1)
        .format({
            "latest_inventory": "{:.0f}",
            "b2b_inventory": "{:.0f}",
            "drr": "{:.1f}",
            "asp": "₹{:.0f}",
            "business_loss": "₹{:.0f}"
        })
    )
    st.dataframe(styled_df, use_container_width=True)

    # --- PIE CHART ---
    st.markdown("### 🥧 Contribution to Total Business Loss")
    total_loss = report["business_loss"].sum()
    pie_df = report[report["business_loss"] > 0.03 * total_loss]
    if not pie_df.empty:
        fig = px.pie(pie_df, names="variant_label", values="business_loss",
                     title="Contribution to Total Business Loss (Active SKUs)",
                     color_discrete_sequence=px.colors.sequential.RdBu)
        st.plotly_chart(fig, use_container_width=True)


    # --- UNIFIED WAREHOUSE + BLOCKED INVENTORY SECTION ---
    st.markdown("---")
    st.subheader("🏭 Live Warehouse + Blocked Inventory (from BigQuery)")

    @st.cache_data(ttl=600)
    def fetch_blocked_inventory_clean():
        query = """
            SELECT Location, Product_Name, SKU, EAN, Total_Blocked_Inventory
            FROM `shopify-pubsub-project.adhoc_data_asia.BlockedInv`
            WHERE Total_Blocked_Inventory IS NOT NULL
        """
        df = client.query(query).to_dataframe()
        df["SKU"] = df["SKU"].astype(str).str.replace("`", "").str.strip().str.upper()
        df["Total_Blocked_Inventory"] = pd.to_numeric(df["Total_Blocked_Inventory"], errors="coerce").fillna(0)
        if "EAN" in df.columns:
            df.drop(columns=["EAN"], inplace=True)
        location_map = {
            "Heavenly Secrets Private Limited - Bangalore ": "Bangalore",
            "Heavenly Secrets Private Limited - Mumbai - B2B": "Mumbai B2B",
            "Heavenly Secrets Pvt Ltd - Kolkata": "Kolkata",
            "Heavenly Secrets Private Limited - Emiza Bilaspur": "Bilaspur",
        }
        df["Location"] = df["Location"].replace(location_map)
        df["Location"] = df["Location"].astype(str).str.strip()
        return df.fillna("")

    # ✅ Fetch all blocked data and live SKUs
    try:
        blocked_df_all = fetch_blocked_inventory_clean()
        blocked_skus = blocked_df_all["SKU"].unique().tolist()
        product_titles = sorted(blocked_df_all["Product_Name"].dropna().unique().tolist())
        live_skus = report["sku"].unique().tolist()
        all_skus = sorted(list(set(live_skus + blocked_skus)))
    except Exception as e:
        st.error(f"Error fetching blocked SKUs: {e}")
        all_skus, product_titles = [], []

    # --- Filter Controls ---
    colsku, coltitle = st.columns(2)
    with colsku:
        selected_sku = st.selectbox("Select SKU (optional):", options=["None"] + all_skus)
    with coltitle:
        selected_title = st.selectbox("Select Product Title (optional):", options=["None"] + product_titles)

    # --- Logic for Fetching ---
    if selected_sku != "None" or selected_title != "None":
        st.info(f"Fetching live warehouse and blocked data for selection...")

        try:
            # Initialize empty DataFrames
            warehouse_df = pd.DataFrame()
            blocked_filtered = pd.DataFrame()

            # --- CASE 1: SKU selected ---
            if selected_sku != "None":
                warehouse_df = fetch_warehouse_summary(selected_sku)
                blocked_filtered = blocked_df_all[blocked_df_all["SKU"] == selected_sku]

            # --- CASE 2: Product Title selected (may include multiple SKUs) ---
            elif selected_title != "None":
                blocked_filtered = blocked_df_all[blocked_df_all["Product_Name"] == selected_title]
                # Try fetching warehouse data for all SKUs under that product
                skus_for_title = blocked_filtered["SKU"].unique().tolist()
                all_wh = []
                for sku in skus_for_title:
                    temp_df = fetch_warehouse_summary(sku)
                    temp_df["SKU"] = sku
                    all_wh.append(temp_df)
                if all_wh:
                    warehouse_df = pd.concat(all_wh, ignore_index=True)

            # --- Merge logic ---
            if not warehouse_df.empty or not blocked_filtered.empty:
                merged = pd.merge(
                    warehouse_df,
                    blocked_filtered[["Location", "Total_Blocked_Inventory"]],
                    left_on="Company_Name",
                    right_on="Location",
                    how="outer"
                ).fillna({"Total_Inventory": 0, "Available_Inventory": 0, "Total_Blocked_Inventory": 0})

                merged.drop(columns=["Location"], inplace=True, errors="ignore")
                merged.rename(columns={"Total_Blocked_Inventory": "Blocked_Inventory"}, inplace=True)

                # ✅ Add Total = Available + Blocked
                merged["Available_Inventory"] = merged["Available_Inventory"].astype(float)
                merged["Blocked_Inventory"] = merged["Blocked_Inventory"].astype(float)
                merged["Total"] = merged["Available_Inventory"] + merged["Blocked_Inventory"]

                # ✅ Clean up names
                merged["Company_Name"] = merged["Company_Name"].astype(str).str.strip()

                # ✅ Display only relevant columns
                display_cols = ["Company_Name", "Available_Inventory", "Blocked_Inventory", "Total"]

                st.dataframe(
                    merged[display_cols].style.format({
                        "Available_Inventory": "{:,.0f}",
                        "Blocked_Inventory": "{:,.0f}",
                        "Total": "{:,.0f}"
                    }),
                    use_container_width=True
                )

            else:
                st.warning("No matching data found for your selection.")

        except Exception as e:
            st.error(f"Error fetching merged data: {e}")

else:
    st.info("Please calculate business loss first using the 🚀 button.")

