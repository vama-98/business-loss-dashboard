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
    """Reshape multi-level inventory sheet into tidy daily data."""
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

    # Long → tidy format
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

    # Optional date filter
    if start_date:
        start_date = pd.to_datetime(start_date).date()
        tidy = tidy[tidy["date"] >= start_date]
    if end_date:
        end_date = pd.to_datetime(end_date).date()
        tidy = tidy[tidy["date"] <= end_date]

    return tidy


def calculate_business_loss(inventory_url, arr_drr_url, b2b_url, start_date, end_date):
    """Calculate business loss per variant and merge latest B2B inventory."""
    tidy = reshape_inventory(inventory_url, start_date, end_date)
    if tidy.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Only keep active variants
    tidy = tidy[tidy["status"] == "active"]
    all_variants = tidy["variant_id"].unique()

    # ✅ Days out of stock
    oos_days = (
        tidy[tidy["inventory"] == 0]
        .groupby("variant_id")
        .size()
        .reindex(all_variants, fill_value=0)
        .reset_index(name="days_out_of_stock")
    )

    # Latest inventory snapshot
    latest_inv = (
        tidy.sort_values("timestamp")
        .groupby("variant_id")
        .tail(1)[["variant_id", "inventory"]]
        .rename(columns={"inventory": "latest_inventory"})
    )

    report = pd.merge(oos_days, latest_inv, on="variant_id", how="left")

    # Read ARR/DRR (has SKU Code now)
    arr_drr = pd.read_csv(arr_drr_url)
    arr_drr.columns = arr_drr.columns.str.strip().str.lower().str.replace(" ", "_")
    arr_drr.rename(columns={"sku_code": "sku"}, inplace=True)

    required_cols = {"variant_id", "drr", "asp", "product_title", "sku"}
    missing = required_cols - set(arr_drr.columns)
    if missing:
        raise ValueError(f"❌ Missing columns in ARR/DRR sheet: {missing}")

    report["variant_id"] = report["variant_id"].astype(str)
    arr_drr["variant_id"] = arr_drr["variant_id"].astype(str)

    report = pd.merge(report, arr_drr[["variant_id", "product_title", "drr", "asp", "sku"]], on="variant_id", how="left")

    # 🟢 Merge B2B Inventory
    b2b = pd.read_csv(b2b_url, header=0, dtype=str)
    b2b.columns = b2b.columns.map(str).str.strip().str.lower().str.replace(" ", "_")

    # Identify date rows (those with digits in the first column)
    b2b_data = b2b.copy()
    b2b_data = b2b_data.rename(columns={"sku_code": "sku"})
    b2b_data["sku"] = b2b_data["sku"].astype(str)

    # Get latest date row (last row with numeric values)
    date_rows = [r for r in b2b_data.index if any(ch.isdigit() for ch in str(b2b_data.iloc[r, 0]))]
    last_row = max(date_rows) if date_rows else None

    if last_row:
        latest_inventory_values = b2b_data.iloc[last_row].to_dict()
        b2b_df = pd.DataFrame(list(latest_inventory_values.items()), columns=["sku", "b2b_inventory"])
    else:
        b2b_df = pd.DataFrame(columns=["sku", "b2b_inventory"])

    b2b_df["b2b_inventory"] = pd.to_numeric(b2b_df["b2b_inventory"], errors="coerce").fillna(0)

    # Merge into report
    report = pd.merge(report, b2b_df, on="sku", how="left")
    report["b2b_inventory"] = report["b2b_inventory"].fillna(0)

    # Compute business loss & DOH
    report["drr"] = pd.to_numeric(report.get("drr", 0), errors="coerce").fillna(0)
    report["asp"] = pd.to_numeric(report.get("asp", 0), errors="coerce").fillna(0)
    report["latest_inventory"] = pd.to_numeric(report.get("latest_inventory", 0), errors="coerce").fillna(0)
    report["business_loss"] = report["days_out_of_stock"] * report["drr"] * report["asp"]
    report["doh"] = report.apply(lambda x: math.ceil(x["latest_inventory"] / x["drr"]) if x["drr"] > 0 else None, axis=1)

    report["variant_label"] = report.apply(
        lambda x: f"{x['product_title']} ({x['variant_id']})" if pd.notna(x["product_title"]) else x["variant_id"],
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

if st.button("🚀 Calculate Business Loss"):
    with st.spinner("Crunching numbers... please wait ⏳"):
        report, tidy = calculate_business_loss(INVENTORY_URL, ARR_DRR_URL, B2B_URL, start_date, end_date)

    if report.empty:
        st.warning("⚠️ No data available for this range.")
    else:
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
                color = "background-color: #FFC7C7"      # red
            elif row["doh"] is not None and row["doh"] <= 7:
                color = "background-color: #FFC7C7"      # red
            elif row["doh"] is not None and 8 <= row["doh"] <= 15:
                color = "background-color: #fff6a5"      # yellow
            return [color] * len(row)

        styled_df = (
            report[[
                "variant_label", "latest_inventory", "b2b_inventory",
                "doh", "days_out_of_stock", "drr", "asp", "business_loss"
            ]]
            .style.apply(highlight_doh, axis=1)
            .format({
                "latest_inventory": "{:.0f}",
                "b2b_inventory": "{:.0f}",
                "doh": "{:.0f}",
                "drr": "{:.1f}",
                "asp": "₹{:.0f}",
                "business_loss": "₹{:.0f}"
            })
        )

        st.dataframe(styled_df, use_container_width=True)
