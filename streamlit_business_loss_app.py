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

    # ----- ARR/DRR merge -----
    arr_drr = pd.read_csv(arr_drr_url)
    arr_drr.columns = arr_drr.columns.str.strip().str.lower().str.replace(" ", "_")
    arr_drr.rename(columns={"sku_code": "sku"}, inplace=True)
    arr_drr["variant_id"] = arr_drr["variant_id"].apply(clean_id)
    arr_drr["sku"] = arr_drr["sku"].apply(clean_sku)
    report["variant_id"] = report["variant_id"].apply(clean_id)

    common_variants = set(report["variant_id"]).intersection(set(arr_drr["variant_id"]))

    # ----- B2B merge -----
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
    common_skus = set(report["sku"]).intersection(set(b2b_latest["sku"]))
    report = pd.merge(report, b2b_latest, on="sku", how="left")
    report["b2b_inventory"] = report["b2b_inventory"].fillna(0)

    # ----- Metrics -----
    for col in ["drr", "asp", "latest_inventory"]:
        report[col] = pd.to_numeric(report[col], errors="coerce").fillna(0)
    report["business_loss"] = report["days_out_of_stock"] * report["drr"] * report["asp"]
    report["doh"] = report.apply(lambda x: math.ceil(x["latest_inventory"]/x["drr"]) if x["drr"] > 0 else 0, axis=1)
    report["variant_label"] = report.apply(
        lambda x: f"{x['product_title']} ({x['variant_id']})" if pd.notna(x["product_title"]) else str(x["variant_id"]),
        axis=1
    )

    # ----- Debug Section -----
    if show_debug:
        with st.expander("🧩 Debug Preview", expanded=False):
            st.write(f"✅ Common variant_id count after cleaning: {len(common_variants)}")
            st.write(f"✅ Common SKUs matched after cleaning: {len(common_skus)}")
            st.markdown("**Sample variant_ids (Inventory):**")
            st.json(report["variant_id"].head(10).tolist())
            st.markdown("**Sample variant_ids (ARR_DRR):**")
            st.json(arr_drr["variant_id"].head(10).tolist())
            st.markdown("**Sample SKUs (ARR_DRR):**")
            st.json(report["sku"].head(10).tolist())
            st.markdown("**Sample SKUs (B2B):**")
            st.json(b2b_latest["sku"].head(10).tolist())
            st.markdown("**Sample rows:**")
            st.dataframe(report.head(10))
            report["days_out_of_stock"] = report["days_out_of_stock"].fillna(0).round().astype(int)
    return report.fillna(0)


# -------------------------------
# STREAMLIT DASHBOARD
# -------------------------------
st.set_page_config(page_title="Business Loss Dashboard", layout="wide")
st.title("💸 Business Loss Dashboard (Full + Debug Mode)")

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date")
with col2:
    end_date = st.date_input("End Date")

show_debug = st.toggle("Show Debug Info", value=True)

if st.button("🚀 Calculate Business Loss (Debug Mode)"):
    with st.spinner("Crunching numbers... please wait ⏳"):
        report = calculate_business_loss(INVENTORY_URL, ARR_DRR_URL, B2B_URL, start_date, end_date, show_debug)
    st.session_state["report"] = report

report = st.session_state.get("report", None)

# -------------------------------
# VISUALIZATION SECTION
# -------------------------------
if report is not None and not report.empty:
    st.markdown("---")
    st.subheader("📊 Business Loss Summary Metrics")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Unique Variants", report["variant_id"].nunique())
    c2.metric("Total OOS Days", int(report["days_out_of_stock"].sum()))
    c3.metric("Avg DRR", round(report["drr"].mean(), 1))
    c4.metric("Total Business Loss", f"₹{report['business_loss'].sum():,.0f}")

    # ----- Styled Table -----
    st.markdown("### 🧾 Variant-wise Business Loss (with DOH highlights)")
    def highlight_doh(row):
        if row["latest_inventory"] == 0 or row["doh"] <= 7:
            color = "background-color: #FFC7C7"
        elif 8 <= row["doh"] <= 15:
            color = "background-color: #FFF6A5"
        else:
            color = ""
        return [color]*len(row)

    styled_df = (
        report[["variant_label", "latest_inventory", "b2b_inventory", "doh",
                "days_out_of_stock", "drr", "asp", "business_loss"]]
        .style.apply(highlight_doh, axis=1)
        .format({"latest_inventory": "{:.0f}", "b2b_inventory": "{:.0f}",
                 "drr": "{:.1f}", "asp": "₹{:.0f}", "business_loss": "₹{:.0f}"})
    )
    st.dataframe(styled_df, use_container_width=True)

    # ----- Pie Chart -----
    st.markdown("### 🥧 Contribution to Total Business Loss")
    total_loss = report["business_loss"].sum()
    pie_df = report[report["business_loss"] > 0.03 * total_loss]
    if not pie_df.empty:
        fig = px.pie(pie_df, names="variant_label", values="business_loss",
                     title="Contribution to Total Business Loss (Active SKUs)",
                     color_discrete_sequence=px.colors.sequential.RdBu)
        st.plotly_chart(fig, use_container_width=True)

    # ----- Top/Bottom DRR -----
    st.markdown("---")
    st.subheader("🏆 D2C Performance Insights")
    drr_df = report[["variant_label", "drr", "latest_inventory", "doh"]].copy()
    drr_df["drr"] = pd.to_numeric(drr_df["drr"], errors="coerce").fillna(0)
    top5 = drr_df.sort_values("drr", ascending=False).head(5)
    bottom5 = drr_df[drr_df["drr"] > 0].sort_values("drr", ascending=True).head(5)

    col_left, col_right = st.columns(2)
    with col_left:
        st.markdown("### 🥇 Top 5 D2C Performers")
        st.dataframe(top5, use_container_width=True)
    with col_right:
        st.markdown("### 🪫 Bottom 5 D2C Performers")
        st.dataframe(bottom5, use_container_width=True)

    # ----- Sidebar Simulation -----
    st.sidebar.markdown("## 🧱 Block Inventory Simulation")
    selected_product = st.sidebar.selectbox("Select Product", options=report["variant_label"].tolist())
    qty_to_block = st.sidebar.number_input("Enter Quantity to Block", min_value=0, value=0, step=1)
    if st.sidebar.button("Simulate Impact"):
        row = report.loc[report["variant_label"] == selected_product].iloc[0]
        latest_inv = row["latest_inventory"]
        drr = row["drr"]
        if drr <= 0:
            st.sidebar.error("❌ Invalid DRR — cannot simulate impact.")
        else:
            new_doh = math.ceil((latest_inv - qty_to_block) / drr)
            if new_doh < 15:
                st.sidebar.warning(f"⚠️ Blocking will reduce DOH to **{new_doh} days** — risky for D2C!")
            else:
                st.sidebar.success(f"✅ Safe to block. DOH after block: **{new_doh} days**")

else:
    st.info("Please calculate business loss first using the 🚀 button.")

