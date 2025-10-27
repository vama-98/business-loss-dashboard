import pandas as pd
import streamlit as st
import plotly.express as px
import math

# -------------------------------
# CONFIG
# -------------------------------
INVENTORY_URL = "https://docs.google.com/spreadsheets/d/1nLdtjYwVD1AFa1VqCUlPS2W8t4lRYJnyMOwMX8sNkfU/export?format=csv&gid=0"
ARR_DRR_URL   = "https://docs.google.com/spreadsheets/d/1nLdtjYwVD1AFa1VqCUlPS2W8t4lRYJnyMOwMX8sNkfU/export?format=csv&gid=1079657777"
B2B_URL       = "https://docs.google.com/spreadsheets/d/1nLdtjYwVD1AFa1VqCUlPS2W8t4lRYJnyMOwMX8sNkfU/export?format=csv&gid=2131638248"

# -------------------------------
# FUNCTIONS
# -------------------------------
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

    tidy = long_df.pivot_table(
        index=["timestamp", "date", "variant_id"],
        columns="field",
        values="value",
        aggfunc="first"
    ).reset_index()
    tidy.columns = [str(c).strip().lower() for c in tidy.columns]

    tidy["inventory"] = pd.to_numeric(tidy.get("inventory", 0), errors="coerce").fillna(0)
    tidy["status"] = tidy.get("status", "").astype(str).str.lower()

    if start_date:
        start_date = pd.to_datetime(start_date).date()
        tidy = tidy[tidy["date"] >= start_date]
    if end_date:
        end_date = pd.to_datetime(end_date).date()
        tidy = tidy[tidy["date"] <= end_date]

    return tidy


def calculate_business_loss(inventory_url, arr_drr_url, b2b_url, start_date, end_date):
    tidy = reshape_inventory(inventory_url, start_date, end_date)
    if tidy.empty:
        return pd.DataFrame(), pd.DataFrame()

    tidy = tidy[tidy["status"] == "active"]
    all_variants = tidy["variant_id"].unique()

    oos_days = (
        tidy[tidy["inventory"] == 0]
        .groupby("variant_id")
        .size()
        .reindex(all_variants, fill_value=0)
        .reset_index(name="days_out_of_stock")
    )

    latest_inv = (
        tidy.sort_values("timestamp")
        .groupby("variant_id")
        .tail(1)[["variant_id", "inventory"]]
        .rename(columns={"inventory": "latest_inventory"})
    )

    report = pd.merge(oos_days, latest_inv, on="variant_id", how="left")

    # 🟢 ARR_DRR LOAD & NORMALIZE
    arr_drr = pd.read_csv(arr_drr_url)

    arr_drr.columns = (
        arr_drr.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("₹", "", regex=False)
        .str.replace("(", "", regex=False)
        .str.replace(")", "", regex=False)
    )

    arr_drr.rename(columns={
        "sku_code": "sku_code",
        "product_name": "product_title",
        "product": "product_title",
        "asp_inr": "asp",
        "asp_₹": "asp",
        "asp_rs": "asp",
        "avg_selling_price": "asp",
        "daily_run_rate": "drr",
        "drr_units_day": "drr"
    }, inplace=True)

    # Normalize SKU codes
    if "sku_code" in arr_drr.columns:
        arr_drr["sku_code"] = arr_drr["sku_code"].astype(str).str.strip().str.upper().str.replace("-", "", regex=False)

    required_cols = {"variant_id", "product_title", "drr", "asp", "sku_code"}
    missing = required_cols - set(arr_drr.columns)
    if missing:
        raise ValueError(f"❌ Missing columns in ARR/DRR sheet: {missing}")

    arr_drr["variant_id"] = arr_drr["variant_id"].astype(str)
    report["variant_id"] = report["variant_id"].astype(str)

    report = pd.merge(report, arr_drr[["variant_id", "product_title", "drr", "asp", "sku_code"]],
                      on="variant_id", how="left")

    # 🟢 B2B INVENTORY LOAD
    b2b_raw = pd.read_csv(b2b_url, header=0)
    b2b_raw.columns = b2b_raw.columns.map(str).str.strip().str.upper()

    date_mask = b2b_raw.iloc[:, 0].astype(str).str.match(r"\d{2}-\d{2}")
    b2b_data = b2b_raw[date_mask].copy()

    b2b_data["parsed_date"] = pd.to_datetime(b2b_data.iloc[:, 0], format="%d-%m", errors="coerce")
    latest_row = b2b_data.loc[b2b_data["parsed_date"].idxmax()]

    b2b_latest = latest_row.drop(labels=["parsed_date"]).reset_index()
    b2b_latest.columns = ["sku_code", "b2b_inventory"]

    b2b_latest["sku_code"] = (
        b2b_latest["sku_code"].astype(str)
        .str.strip()
        .str.upper()
        .str.replace("-", "", regex=False)
    )
    b2b_latest["b2b_inventory"] = pd.to_numeric(b2b_latest["b2b_inventory"], errors="coerce").fillna(0)

    report["sku_code"] = (
        report["sku_code"].astype(str)
        .str.strip()
        .str.upper()
        .str.replace("-", "", regex=False)
    )

    # 🧩 Debug Output
    with st.expander("🧩 Debug Info: SKU Matching Details"):
        st.write("🧾 ARR_DRR sample SKUs:", report["sku_code"].head(10).tolist())
        st.write("📦 B2B sample SKUs:", b2b_latest["sku_code"].head(10).tolist())

        common_skus = set(report["sku_code"]).intersection(set(b2b_latest["sku_code"]))
        st.write(f"✅ Common SKUs matched: {len(common_skus)}")
        if len(common_skus) > 0:
            st.write("🔹 Example matched SKUs:", list(common_skus)[:10])

    # Merge with B2B
    report = pd.merge(report, b2b_latest, on="sku_code", how="left")
    report["b2b_inventory"] = report["b2b_inventory"].fillna(0)

    # 🧠 Debug check for a known SKU
    example_sku = "PGSGCSERUM1"
    if example_sku in list(report["sku_code"]):
        val = report.loc[report["sku_code"] == example_sku, "b2b_inventory"].values
        st.write(f"🧠 Debug: B2B Inventory for {example_sku} = {val}")

    # Compute metrics
    report["drr"] = pd.to_numeric(report["drr"], errors="coerce").fillna(0)
    report["asp"] = pd.to_numeric(report["asp"], errors="coerce").fillna(0)
    report["latest_inventory"] = pd.to_numeric(report["latest_inventory"], errors="coerce").fillna(0)

    report["business_loss"] = report["days_out_of_stock"] * report["drr"] * report["asp"]
    report["doh"] = report.apply(
        lambda x: math.ceil(x["latest_inventory"] / x["drr"]) if x["drr"] > 0 else None,
        axis=1
    )

    report["variant_label"] = report.apply(
        lambda x: f"{x['product_title']} ({x['variant_id']})"
        if pd.notna(x["product_title"]) else x["variant_id"],
        axis=1
    )

    return report, tidy


# -------------------------------
# STREAMLIT DASHBOARD
# -------------------------------
st.set_page_config(page_title="Business Loss Dashboard", layout="wide")
st.title("💸 Business Loss Dashboard")

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date")
with col2:
    end_date = st.date_input("End Date")

if "report" not in st.session_state:
    st.session_state["report"] = None

if st.button("🚀 Calculate Business Loss"):
    with st.spinner("Crunching numbers... please wait ⏳"):
        report, tidy = calculate_business_loss(INVENTORY_URL, ARR_DRR_URL, B2B_URL, start_date, end_date)
    st.session_state["report"] = report

report = st.session_state["report"]

if report is not None and not report.empty:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Unique Variants", report["variant_id"].nunique())
    c2.metric("Total OOS Days", int(report["days_out_of_stock"].sum()))
    c3.metric("Avg DRR", round(report["drr"].mean(), 1))
    c4.metric("Total Business Loss", f"₹{report['business_loss'].sum():,.0f}")

    st.markdown("---")
    st.subheader("📋 Variant-wise Business Loss (Active SKUs Only)")

    def highlight_doh(row):
        color = ""
        if row["latest_inventory"] == 0:
            color = "background-color: #FFC7C7"
        elif row["doh"] is not None and row["doh"] <= 7:
            color = "background-color: #FFC7C7"
        elif row["doh"] is not None and 8 <= row["doh"] <= 15:
            color = "background-color: #fff6a5"
        return [color] * len(row)

    st.dataframe(
        report[[
            "variant_label", "latest_inventory", "b2b_inventory",
            "doh", "days_out_of_stock", "drr", "asp", "business_loss"
        ]],
        use_container_width=True
    )
else:
    st.info("Please calculate business loss first using the 🚀 button.")
