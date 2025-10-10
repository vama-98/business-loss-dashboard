import pandas as pd
import streamlit as st
import plotly.express as px

# -------------------------------
# CONFIG
# -------------------------------
INVENTORY_URL = "https://docs.google.com/spreadsheets/d/1nLdtjYwVD1AFa1VqCUlPS2W8t4lRYJnyMOwMX8sNkfU/export?format=csv&gid=0"
ARR_DRR_URL   = "https://docs.google.com/spreadsheets/d/1nLdtjYwVD1AFa1VqCUlPS2W8t4lRYJnyMOwMX8sNkfU/export?format=csv&gid=1079657777"

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


def calculate_business_loss(inventory_url, arr_drr_url, start_date, end_date):
    """Calculate business loss per variant based on OOS days × DRR × ASP."""
    tidy = reshape_inventory(inventory_url, start_date, end_date)
    if tidy.empty:
        return pd.DataFrame(), pd.DataFrame()

    all_variants = tidy["variant_id"].unique()

    # ✅ Days out of stock where product is active
    oos_days = (
        tidy[(tidy["status"] == "active") & (tidy["inventory"] == 0)]
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

    # Merge ARR/DRR data (includes product_title)
    arr_drr = pd.read_csv(arr_drr_url)
    arr_drr.columns = arr_drr.columns.str.strip().str.lower().str.replace(" ", "_")

    # Validate presence of key columns
    required_cols = {"variant_id", "drr", "asp", "product_title"}
    missing = required_cols - set(arr_drr.columns)
    if missing:
        raise ValueError(f"❌ Missing columns in ARR/DRR sheet: {missing}")

    report["variant_id"] = report["variant_id"].astype(str)
    arr_drr["variant_id"] = arr_drr["variant_id"].astype(str)
    report = pd.merge(report, arr_drr[["variant_id", "product_title", "drr", "asp"]], on="variant_id", how="left")

    # Compute business loss
    report["drr"] = pd.to_numeric(report.get("drr", 0), errors="coerce").fillna(0)
    report["asp"] = pd.to_numeric(report.get("asp", 0), errors="coerce").fillna(0)
    report["business_loss"] = report["days_out_of_stock"] * report["drr"] * report["asp"]

    report["variant_label"] = report.apply(
        lambda x: f"{x['product_title']} ({x['variant_id']})" if pd.notna(x["product_title"]) else x["variant_id"],
        axis=1
    )

    return report, tidy


# -------------------------------
# STREAMLIT DASHBOARD
# -------------------------------
st.set_page_config(page_title="Business Loss Dashboard", layout="wide")
st.title("💸 Business Loss Dashboard (Optimized, No Shopify API)")

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date")
with col2:
    end_date = st.date_input("End Date")

if st.button("🚀 Calculate Business Loss"):
    with st.spinner("Crunching numbers... please wait ⏳"):
        report, tidy = calculate_business_loss(INVENTORY_URL, ARR_DRR_URL, start_date, end_date)

    if report.empty:
        st.warning("⚠️ No data available for this range.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Unique Variants", report["variant_id"].nunique())
        c2.metric("Total OOS Days", int(report["days_out_of_stock"].sum()))
        c3.metric("Avg DRR", round(report["drr"].mean(), 1))
        c4.metric("Total Business Loss", f"₹{report['business_loss'].sum():,.0f}")

        st.markdown("---")

        st.subheader("📋 Variant-wise Business Loss")
        st.dataframe(
            report[["variant_label", "days_out_of_stock", "drr", "asp", "business_loss"]],
            use_container_width=True
        )

        st.subheader("📊 Visual Insights")

        fig1 = px.bar(
            report.sort_values("business_loss", ascending=False).head(15),
            x="variant_label", y="business_loss",
            title="Top 15 Variants by Business Loss",
            text_auto=".2s", color="business_loss",
            color_continuous_scale="Reds"
        )
        st.plotly_chart(fig1, use_container_width=True)

        fig2 = px.pie(
            report,
            names="variant_label",
            values="business_loss",
            title="Contribution to Total Loss",
            color_discrete_sequence=px.colors.sequential.RdBu
        )
        st.plotly_chart(fig2, use_container_width=True)

        top_loss = report.sort_values("business_loss", ascending=False).head(5)
        st.markdown("### 💥 Top 5 Variants with Maximum Loss")
        st.table(top_loss[["variant_label", "days_out_of_stock", "drr", "asp", "business_loss"]])
