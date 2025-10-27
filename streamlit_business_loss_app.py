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
# HELPERS
# -------------------------------
def clean_id(v):
    v = str(v).strip().replace(".0", "").replace(".00", "")
    return v

def clean_sku(s):
    s = str(s).strip().replace(".0", "").replace(".00", "").upper()
    return s

# -------------------------------
# MAIN FUNCTION
# -------------------------------
def calculate_business_loss(inventory_url, arr_drr_url, b2b_url, start_date, end_date):
    # --- Inventory reshape ---
    df = pd.read_csv(inventory_url, header=[0, 1])
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
    tidy = tidy[tidy["status"] == "active"]

    # --- Calculate OOS ---
    oos_days = tidy[tidy["inventory"] == 0].groupby("variant_id").size().reset_index(name="days_out_of_stock")
    latest_inv = tidy.sort_values("timestamp").groupby("variant_id").tail(1)[["variant_id", "inventory"]]
    latest_inv.rename(columns={"inventory": "latest_inventory"}, inplace=True)
    report = pd.merge(oos_days, latest_inv, on="variant_id", how="outer").fillna(0)

    # --- ARR/DRR merge ---
    st.markdown("### 🧾 ARR_DRR Debug Info")
    arr_drr = pd.read_csv(arr_drr_url)
    arr_drr.columns = arr_drr.columns.str.strip().str.lower().str.replace(" ", "_")
    arr_drr.rename(columns={"sku_code": "sku"}, inplace=True)

    arr_drr["variant_id"] = arr_drr["variant_id"].apply(clean_id)
    report["variant_id"] = report["variant_id"].apply(clean_id)
    arr_drr["sku"] = arr_drr["sku"].apply(clean_sku)

    common_variants = set(report["variant_id"]).intersection(set(arr_drr["variant_id"]))
    st.write(f"✅ Common variant_id count after cleaning: {len(common_variants)}")

    report = pd.merge(report, arr_drr[["variant_id", "product_title", "drr", "asp", "sku"]],
                      on="variant_id", how="left")

    # --- B2B merge ---
    st.markdown("### 📦 B2B Debug Info")
    b2b_raw = pd.read_csv(b2b_url)
    b2b_raw.columns = b2b_raw.columns.map(str).str.strip().str.upper()

    # Detect date rows
    date_mask = b2b_raw.iloc[:, 0].astype(str).str.match(r"\d{2}-\d{2}")
    b2b_data = b2b_raw[date_mask].copy()
    b2b_data["parsed_date"] = pd.to_datetime(b2b_data.iloc[:, 0], format="%d-%m", errors="coerce")
    latest_row = b2b_data.loc[b2b_data["parsed_date"].idxmax()]

    b2b_latest = latest_row.drop(labels=["parsed_date"]).reset_index()
    b2b_latest.columns = ["sku", "b2b_inventory"]

    b2b_latest["sku"] = b2b_latest["sku"].apply(clean_sku)
    b2b_latest["b2b_inventory"] = pd.to_numeric(b2b_latest["b2b_inventory"], errors="coerce").fillna(0)

    report["sku"] = report["sku"].apply(clean_sku)
    common_skus = set(report["sku"]).intersection(set(b2b_latest["sku"]))
    st.write(f"✅ Common SKUs matched after cleaning: {len(common_skus)}")
    st.write("🧾 Sample SKUs (ARR_DRR):", report["sku"].head(10).tolist())
    st.write("📦 Sample SKUs (B2B):", b2b_latest["sku"].head(10).tolist())

    report = pd.merge(report, b2b_latest, on="sku", how="left")
    report["b2b_inventory"] = report["b2b_inventory"].fillna(0)

    # --- Compute metrics ---
    for col in ["drr", "asp", "latest_inventory", "b2b_inventory"]:
        report[col] = pd.to_numeric(report[col], errors="coerce").fillna(0)
    report["business_loss"] = report["days_out_of_stock"] * report["drr"] * report["asp"]
    report["doh"] = report.apply(
        lambda x: math.ceil(x["latest_inventory"] / x["drr"]) if x["drr"] > 0 else 0, axis=1
    )

    report["variant_label"] = report.apply(
        lambda x: f"{x['product_title']} ({x['variant_id']})" if pd.notna(x["product_title"]) else str(x["variant_id"]),
        axis=1,
    )

    st.success(f"✅ Final merged report generated: {len(report)} rows")
    st.dataframe(report.head(10))

    return report.fillna(0)


# -------------------------------
# STREAMLIT DASHBOARD
# -------------------------------
st.set_page_config(page_title="Business Loss Dashboard (Fixed SKU Match)", layout="wide")
st.title("💸 Business Loss Dashboard (Fixed SKU Matching)")

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date")
with col2:
    end_date = st.date_input("End Date")

if st.button("🚀 Calculate Business Loss"):
    with st.spinner("Crunching data... please wait ⏳"):
        report = calculate_business_loss(INVENTORY_URL, ARR_DRR_URL, B2B_URL, start_date, end_date)
    st.session_state["report"] = report

report = st.session_state.get("report", None)

if report is not None and not report.empty:
    st.markdown("---")
    st.subheader("📊 Business Loss Summary")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Unique Variants", report["variant_id"].nunique())
    c2.metric("Total OOS Days", int(report["days_out_of_stock"].sum()))
    c3.metric("Avg DRR", round(report["drr"].mean(), 1))
    c4.metric("Total Business Loss", f"₹{report['business_loss'].sum():,.0f}")
else:
    st.info("Click 🚀 to calculate business loss.")
