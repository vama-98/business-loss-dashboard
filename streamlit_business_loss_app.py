# Final Integrated Business Loss Dashboard (with Filters + Inventory Trend)

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
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
    """Initialize BigQuery client with credentials from Streamlit secrets"""
    creds_dict = st.secrets["bigquery"]
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    return bigquery.Client(credentials=credentials, project=creds_dict["project_id"])

client = get_bq_client()

@st.cache_data(ttl=300)
def fetch_warehouse_summary(sku):
    """Fetch warehouse inventory summary (excluding blocked logic)"""
    query = f"""
        SELECT 
          Company_Name,
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
        WHERE SAFE_CAST(Sku AS STRING) = '{sku}'
              AND LOWER(CAST(Status AS STRING)) = 'available'
              AND LOWER(CAST(Greaterthaneig AS STRING)) = 'true'
        GROUP BY Company_Name
        ORDER BY Total_Inventory DESC
    """
    df = client.query(query).to_dataframe()
    return df.fillna(0)

def get_inventory_trend_from_sheet(inventory_url, sku, start_date=None, end_date=None):
    """Get inventory trend for a SKU from the Google Sheets inventory data"""
    try:
        # Reshape the inventory data
        tidy = reshape_inventory(inventory_url, start_date, end_date)
        
        # Find the variant_id(s) that match this SKU
        # We need to get the mapping from the report
        report = st.session_state.get("report", None)
        if report is None or report.empty:
            return pd.DataFrame()
        
        # Get variant IDs for this SKU
        sku_data = report[report["sku"] == sku]
        if sku_data.empty:
            return pd.DataFrame()
        
        variant_ids = sku_data["variant_id"].unique()
        
        # Filter tidy data for these variants
        trend_data = tidy[tidy["variant_id"].isin(variant_ids)].copy()
        
        # Group by date and sum inventory
        trend_summary = trend_data.groupby("date")["inventory"].sum().reset_index()
        trend_summary.columns = ["Date", "Total_Quantity"]
        trend_summary["Date"] = pd.to_datetime(trend_summary["Date"])
        trend_summary = trend_summary.sort_values("Date")
        
        return trend_summary
    except Exception as e:
        st.error(f"Error fetching inventory trend: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def fetch_blocked_inventory_clean():
    """Fetch blocked inventory data from BigQuery"""
    query = """
        SELECT Location, Product_Name, SKU, Total_Blocked_Inventory
        FROM `shopify-pubsub-project.adhoc_data_asia.BlockedInv`
        WHERE Total_Blocked_Inventory IS NOT NULL
    """
    try:
        df = client.query(query).to_dataframe()
        df["SKU"] = df["SKU"].astype(str).str.replace("`", "").str.strip().str.upper()
        df["Total_Blocked_Inventory"] = pd.to_numeric(df["Total_Blocked_Inventory"], errors="coerce").fillna(0)
        
        # Clean location names
        location_map = {
            "Heavenly Secrets Private Limited - Bangalore ": "Bangalore",
            "Heavenly Secrets Private Limited - Mumbai - B2B": "Mumbai B2B",
            "Heavenly Secrets Pvt Ltd - Kolkata": "Kolkata",
            "Heavenly Secrets Private Limited - Emiza Bilaspur": "Bilaspur",
        }
        df["Location"] = df["Location"].replace(location_map)
        df["Location"] = df["Location"].astype(str).str.strip()
        return df.fillna("")
    except Exception as e:
        st.error(f"Error fetching blocked inventory: {e}")
        return pd.DataFrame()

# -------------------------------
# HELPER FUNCTIONS
# -------------------------------
def clean_id(v):
    """Clean variant ID by removing decimal points"""
    return str(v).strip().replace(".0", "").replace(".00", "")

def clean_sku(s):
    """Clean SKU by removing decimals and converting to uppercase"""
    return str(s).strip().replace(".0", "").replace(".00", "").upper()

def safe_get_column(df, col_name, default_val=""):
    """Safely get column from dataframe, return default if missing"""
    if col_name in df.columns:
        return df[col_name]
    return default_val

def reshape_inventory(sheet_url, start_date=None, end_date=None):
    """Reshape inventory data from multi-level header format to tidy format"""
    df = pd.read_csv(sheet_url, header=[0, 1])
    
    # Process multi-level columns
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
    
    # Melt to long format
    long_df = df.melt(id_vars=["timestamp", "date"], var_name="variant_field", value_name="value")
    long_df[["variant_id", "field"]] = long_df["variant_field"].str.rsplit("_", n=1, expand=True)
    
    # Pivot back to tidy format
    tidy = long_df.pivot_table(
        index=["timestamp", "date", "variant_id"], 
        columns="field",
        values="value", 
        aggfunc="first"
    ).reset_index()
    
    tidy.columns = [str(c).strip().lower() for c in tidy.columns]
    tidy["inventory"] = pd.to_numeric(tidy.get("inventory", 0), errors="coerce").fillna(0)
    tidy["status"] = tidy.get("status", "").astype(str).str.lower()
    
    # Apply date filters
    if start_date:
        tidy = tidy[tidy["date"] >= pd.to_datetime(start_date).date()]
    if end_date:
        tidy = tidy[tidy["date"] <= pd.to_datetime(end_date).date()]
    
    return tidy

# -------------------------------
# BUSINESS LOSS CALCULATION
# -------------------------------
def calculate_business_loss(inventory_url, arr_drr_url, b2b_url, start_date, end_date, show_debug=False):
    """Calculate business loss metrics by combining inventory, ARR/DRR, and B2B data"""
    
    # Process inventory data
    tidy = reshape_inventory(inventory_url, start_date, end_date)
    tidy = tidy[tidy["status"] == "active"]

    # Calculate out-of-stock days
    oos_days = tidy[tidy["inventory"] == 0].groupby("variant_id").size().reset_index(name="days_out_of_stock")
    latest_inv = tidy.sort_values("timestamp").groupby("variant_id").tail(1)[["variant_id", "inventory"]]
    latest_inv.rename(columns={"inventory": "latest_inventory"}, inplace=True)
    report = pd.merge(oos_days, latest_inv, on="variant_id", how="outer").fillna(0)

    # === ARR/DRR DATA ===
    arr_drr = pd.read_csv(arr_drr_url)
    arr_drr.columns = arr_drr.columns.str.strip().str.lower().str.replace(" ", "_")
    arr_drr.rename(columns={"sku_code": "sku"}, inplace=True)
    arr_drr["variant_id"] = arr_drr["variant_id"].apply(clean_id)
    arr_drr["sku"] = arr_drr["sku"].apply(clean_sku)
    report["variant_id"] = report["variant_id"].apply(clean_id)

    # === B2B SHEET PARSING ===
    b2b_raw = pd.read_csv(b2b_url, header=None)
    b2b_raw = b2b_raw.applymap(lambda x: str(x).strip() if pd.notna(x) else "")
    
    # Extract metadata (first 5 rows)
    meta_raw = b2b_raw.iloc[:5, :]
    data_raw = b2b_raw.iloc[5:, :]
    
    # Transpose metadata
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
    
    # Extract latest B2B inventory
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

    # === MERGE ALL DATA ===
    report = pd.merge(
        report, 
        arr_drr[["variant_id", "product_title", "drr", "asp", "sku"]],
        on="variant_id", 
        how="left"
    )
    report["sku"] = report["sku"].apply(clean_sku)
    report = pd.merge(report, b2b_enriched, on="sku", how="left").fillna(0)

    # Convert numeric columns
    for col in ["drr", "asp", "latest_inventory"]:
        report[col] = pd.to_numeric(report[col], errors="coerce").fillna(0)

    # === CALCULATE METRICS ===
    # Business loss
    report["business_loss"] = report["days_out_of_stock"] * report["drr"] * report["asp"]
    
    # Days on hand
    report["doh"] = report.apply(
        lambda x: math.ceil(x["latest_inventory"]/x["drr"]) if x["drr"] > 0 else 0, 
        axis=1
    )
    
    # Calculate total days in period
    if start_date and end_date:
        total_days = (end_date - start_date).days + 1
    else:
        total_days = 1
    
    # Clean product title (remove variant ID from title)
    report["product_title_clean"] = report["product_title"].astype(str).str.replace(r"\s*\(.*\)$", "", regex=True)
    
    # Quantity misses = DRR × Days out of stock
    report["qty_misses"] = (report["drr"] * report["days_out_of_stock"]).round(0)
    
    # On-shelf availability = (1 - Days OOS / Total Days) × 100
    report["on_shelf_availability"] = report.apply(
        lambda x: round((1 - (x["days_out_of_stock"] / total_days)) * 100, 1)
        if total_days > 0 else 0, 
        axis=1
    )
    
    # Variant label for visualizations
    report["variant_label"] = report.apply(
        lambda x: f"{x['product_title']} ({x['variant_id']})" 
        if pd.notna(x["product_title"]) else str(x["variant_id"]),
        axis=1
    )
    
    # Ensure integer formatting for key columns
    report["drr"] = report["drr"].round(0)
    report["days_out_of_stock"] = report["days_out_of_stock"].round(0)
    
    # Remove duplicates based on SKU (keep first occurrence)
    report = report.drop_duplicates(subset=['sku'], keep='first')
    
    return report.fillna(0)

# -------------------------------
# STREAMLIT DASHBOARD
# -------------------------------
st.set_page_config(page_title="Business Loss Dashboard", layout="wide")
st.title("💸 Business Loss Dashboard (Unified Warehouse + Blocked Inventory)")

# Date inputs
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date")
with col2:
    end_date = st.date_input("End Date")

show_debug = st.toggle("Show Debug Info", value=False)

# Calculate button
if st.button("🚀 Calculate Business Loss"):
    with st.spinner("Crunching numbers... please wait ⏳"):
        report = calculate_business_loss(INVENTORY_URL, ARR_DRR_URL, B2B_URL, start_date, end_date, show_debug)
    st.session_state["report"] = report
    st.success("✅ Business loss calculated successfully!")

report = st.session_state.get("report", None)

# -------------------------------
# VISUALIZATION SECTION
# -------------------------------
if report is not None and not report.empty:
    # ------------------ SUMMARY METRICS ------------------
    st.subheader("📊 Business Loss Summary Metrics")
    c1, c2, c3 = st.columns(3)
    c1.metric("Unique Variants", report["variant_id"].nunique())
    total_loss = report["business_loss"].sum()
    c2.metric("Total Business Loss", f"₹{total_loss:,.0f}")
    total_misses = report["qty_misses"].sum()
    #c3.metric("Total Qty Misses", f"{total_misses:,.0f} units")

    # ------------------ ENHANCED BUSINESS LOSS TABLE ------------------
    st.markdown("### 🧾 Variant-wise Business Loss (with Availability & Misses)")
    
    # --- 🎛️ FILTER CONTROLS ---
    st.markdown("#### 🎚️ Filter Options (optional)")
    
    # Safely get unique values for filters
    categories = ["All"]
    ranges = ["All"]
    skus = ["All"]
    titles = ["All"]
    
    try:
        if "category_b2b" in report.columns:
            cat_values = report["category_b2b"].dropna().astype(str)
            cat_values = cat_values[cat_values != "0"]  # Exclude zeros
            categories.extend(sorted(cat_values.unique().tolist()))
    except Exception:
        pass
    
    try:
        if "range_b2b" in report.columns:
            range_values = report["range_b2b"].dropna().astype(str)
            range_values = range_values[range_values != "0"]  # Exclude zeros
            ranges.extend(sorted(range_values.unique().tolist()))
    except Exception:
        pass
    
    try:
        if "sku" in report.columns:
            sku_values = report["sku"].dropna().astype(str)
            sku_values = sku_values[sku_values != ""]
            skus.extend(sorted(sku_values.unique().tolist()))
    except Exception:
        pass
    
    try:
        if "product_title_clean" in report.columns:
            title_values = report["product_title_clean"].dropna().astype(str)
            title_values = title_values[title_values != ""]
            titles.extend(sorted(title_values.unique().tolist()))
    except Exception:
        pass
    
    # Filter dropdowns (4 columns now)
    colf1, colf2, colf3, colf4 = st.columns(4)
    with colf1:
        selected_category = st.selectbox("Category", options=categories)
    with colf2:
        selected_range = st.selectbox("Range", options=ranges)
    with colf3:
        selected_sku = st.selectbox("SKU", options=skus)
    with colf4:
        selected_product = st.selectbox("Product Title", options=titles)

    # --- APPLY FILTERS (non-destructive) ---
    filtered_report = report.copy()
    
    try:
        if selected_category != "All" and "category_b2b" in filtered_report.columns:
            filtered_report = filtered_report[
                filtered_report["category_b2b"].astype(str) == str(selected_category)
            ]
    except Exception as e:
        st.warning(f"Category filter error: {e}")
    
    try:
        if selected_range != "All" and "range_b2b" in filtered_report.columns:
            filtered_report = filtered_report[
                filtered_report["range_b2b"].astype(str) == str(selected_range)
            ]
    except Exception as e:
        st.warning(f"Range filter error: {e}")
    
    try:
        if selected_sku != "All" and "sku" in filtered_report.columns:
            filtered_report = filtered_report[
                filtered_report["sku"].astype(str) == str(selected_sku)
            ]
    except Exception as e:
        st.warning(f"SKU filter error: {e}")
    
    try:
        if selected_product != "All" and "product_title_clean" in filtered_report.columns:
            filtered_report = filtered_report[
                filtered_report["product_title_clean"].astype(str) == str(selected_product)
            ]
    except Exception as e:
        st.warning(f"Product filter error: {e}")

    # --- SHOW FILTERED TABLE ---
    st.markdown(f"#### 📋 Business Loss Table ({len(filtered_report)} rows)")
    
    # Define column order (without sparkline)
    display_cols = [
        "product_title_clean", "sku", "variant_id",
        "size_b2b", "category_b2b", "range_b2b","business_loss",
        "latest_inventory", "b2b_inventory", "doh",
        "days_out_of_stock", "drr", "asp",
        "qty_misses", "on_shelf_availability"
    ]
    
    # Rename for readability
    rename_map = {
        "product_title_clean": "Product Title",
        "sku": "SKU",
        "variant_id": "Variant ID",
        "size_b2b": "Size",
        "category_b2b": "Category",
        "range_b2b": "Range",
        "business_loss": "Business Loss (₹)",
        "latest_inventory": "Latest Inventory",
        "b2b_inventory": "B2B Inventory",
        "doh": "DOH",
        "days_out_of_stock": "Days OOS",
        "drr": "DRR",
        "asp": "ASP (₹)",
        "qty_misses": "Qty Misses (Units)",
        "on_shelf_availability": "On-Shelf Availability (%)"

    }
    
    # Ensure all columns exist
    for c in display_cols:
        if c not in filtered_report.columns:
            filtered_report[c] = ""
    
    # Display table with formatting
    st.dataframe(
        filtered_report[display_cols].rename(columns=rename_map).style.format({
            "Latest Inventory": "{:.0f}",
            "B2B Inventory": "{:.0f}",
            "DOH": "{:.0f}",
            "Days OOS": "{:.0f}",
            "DRR": "{:.0f}",
            "ASP (₹)": "₹{:.0f}",
            "Qty Misses (Units)": "{:.0f}",
            "On-Shelf Availability (%)": "{:.1f}%",
            "Business Loss (₹)": "₹{:.0f}"
        }),
        use_container_width=True,
        height=600
    )
    
    # --- 🔍 DRILL-DOWN SECTION ---
    st.markdown("---")
    st.markdown("### 🔍 Drill-Down Analysis")
    st.markdown("Select a row from the table above to view detailed inventory trends and warehouse breakdown")
    
    # Create a selectbox with product + SKU for easy identification
    filtered_report["display_label"] = filtered_report.apply(
        lambda x: f"{x['product_title_clean']} - {x['sku']}" if x['product_title_clean'] else x['sku'],
        axis=1
    )
    drill_options = ["None"] + filtered_report["display_label"].tolist()
    selected_drill = st.selectbox("Select Product/SKU:", options=drill_options, key="drill_down")
    
    if selected_drill != "None":
        # Get the selected row data
        selected_row = filtered_report[filtered_report["display_label"] == selected_drill].iloc[0]
        drill_sku = selected_row["sku"]
        drill_product = selected_row["product_title_clean"]
        
        # Display header with product and SKU info
        st.markdown(f"#### 📦 Details: **{drill_product}** | SKU: **{drill_sku}**")
        
        # Create two columns for side-by-side display
        col_left, col_right = st.columns([1, 1])
        
        # LEFT COLUMN: Inventory Trend
        with col_left:
            st.markdown("##### 📈 Inventory Trend")
            with st.spinner("Loading inventory trend..."):
                trend_df = get_inventory_trend_from_sheet(INVENTORY_URL, drill_sku, start_date, end_date)
            
            if not trend_df.empty and len(trend_df) > 0:
                # Create line chart
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=trend_df["Date"],
                    y=trend_df["Total_Quantity"],
                    mode='lines+markers',
                    name='Inventory',
                    line=dict(color='#1f77b4', width=2),
                    marker=dict(size=4)
                ))
                
                fig.update_layout(
                    xaxis_title="Date",
                    yaxis_title="Quantity",
                    hovermode='x unified',
                    template='plotly_white',
                    height=300,
                    margin=dict(l=10, r=10, t=10, b=10)
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Show mini stats
                c1, c2, c3 = st.columns(3)
                c1.metric("Avg", f"{trend_df['Total_Quantity'].mean():,.0f}")
                c2.metric("Max", f"{trend_df['Total_Quantity'].max():,.0f}")
                c3.metric("Current", f"{trend_df['Total_Quantity'].iloc[-1]:,.0f}")
            else:
                st.info("No trend data available for this SKU")
        
        # RIGHT COLUMN: Warehouse Breakdown
        with col_right:
            st.markdown("##### 🏭 Warehouse Breakdown")
            with st.spinner("Loading warehouse data..."):
                try:
                    # Fetch warehouse data
                    warehouse_df = fetch_warehouse_summary(drill_sku)
                    
                    # Fetch blocked inventory
                    blocked_df_all = fetch_blocked_inventory_clean()
                    blocked_filtered = pd.DataFrame()
                    if not blocked_df_all.empty:
                        blocked_filtered = blocked_df_all[blocked_df_all["SKU"] == drill_sku]
                    
                    # Merge data
                    if not warehouse_df.empty or not blocked_filtered.empty:
                        merged = pd.merge(
                            warehouse_df,
                            blocked_filtered[["Location", "Total_Blocked_Inventory"]],
                            left_on="Company_Name",
                            right_on="Location",
                            how="outer"
                        ).fillna({"Total_Inventory": 0, "Available_Inventory": 0, "Total_Blocked_Inventory": 0})
                        
                        merged.drop(columns=["Location"], inplace=True, errors="ignore")
                        merged.rename(columns={"Total_Blocked_Inventory": "Blocked"}, inplace=True)
                        merged.rename(columns={"Company_Name": "Warehouse", "Available_Inventory": "Available"}, inplace=True)
                        
                        merged["Available"] = merged["Available"].astype(float)
                        merged["Blocked"] = merged["Blocked"].astype(float)
                        merged["Total"] = merged["Available"] + merged["Blocked"]
                        
                        display_cols_drill = ["Warehouse", "Available", "Blocked", "Total"]
                        
                        st.dataframe(
                            merged[display_cols_drill].style.format({
                                "Available": "{:,.0f}",
                                "Blocked": "{:,.0f}",
                                "Total": "{:,.0f}"
                            }),
                            use_container_width=True,
                            height=300
                        )
                        
                        # Show summary
                        total_available = merged["Available"].sum()
                        total_blocked = merged["Blocked"].sum()
                        st.metric("Total Available", f"{total_available:,.0f}")
                        st.metric("Total Blocked", f"{total_blocked:,.0f}")
                    else:
                        st.info("No warehouse data available")
                        
                except Exception as e:
                    st.error(f"Error loading warehouse data: {e}")

    # --- 🥧 PIE CHART FOR BUSINESS LOSS (uses full report, not filtered) ---
    st.markdown("### 🥧 Contribution to Total Business Loss")
    total_loss_full = report["business_loss"].sum()
    if total_loss_full > 0:
        pie_df = report[report["business_loss"] > 0.03 * total_loss_full].copy()
        if not pie_df.empty:
            fig = px.pie(
                pie_df,
                values="business_loss",
                title="Contribution to Total Business Loss (All SKUs)",
                color_discrete_sequence=px.colors.sequential.RdBu
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No significant contributors (>3% of total) to business loss.")
    else:
        st.info("No business loss data available.")

    # --- 📈 INVENTORY TREND CHART (Separate Section) ---
    st.markdown("---")
    st.markdown("### 📈 Inventory Trend Explorer (from Google Sheets)")
    st.markdown("Use this section to explore trends for any product independently")
    
    # Get available options for trend analysis
    trend_skus = ["None"] + sorted(report["sku"].dropna().unique().tolist())
    trend_products = ["None"] + sorted(report["product_title_clean"].dropna().unique().tolist())
    
    col_trend1, col_trend2 = st.columns(2)
    with col_trend1:
        selected_trend_sku = st.selectbox("Select SKU for trend:", options=trend_skus, key="trend_sku_explorer")
    with col_trend2:
        selected_trend_product = st.selectbox("Select Product for trend:", options=trend_products, key="trend_product_explorer")
    
    # Determine which selection to use
    trend_sku_to_use = None
    if selected_trend_sku != "None":
        trend_sku_to_use = selected_trend_sku
    elif selected_trend_product != "None":
        # Get SKU(s) for the selected product
        product_skus = report[report["product_title_clean"] == selected_trend_product]["sku"].unique()
        if len(product_skus) > 0:
            trend_sku_to_use = product_skus[0]  # Use first SKU if multiple
    
    if trend_sku_to_use:
        with st.spinner(f"Fetching inventory trend for {trend_sku_to_use}..."):
            trend_df = get_inventory_trend_from_sheet(INVENTORY_URL, trend_sku_to_use, start_date, end_date)
        
        if not trend_df.empty and len(trend_df) > 0:
            # Get product title for the SKU
            sku_info = report[report["sku"] == trend_sku_to_use].iloc[0]
            display_title = sku_info.get("product_title_clean", "Unknown Product")
            
            # Create line chart
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=trend_df["Date"],
                y=trend_df["Total_Quantity"],
                mode='lines+markers',
                name='Total Inventory',
                line=dict(color='#1f77b4', width=2),
                marker=dict(size=6)
            ))
            
            fig.update_layout(
                title=f"Inventory Trend: {display_title} (SKU: {trend_sku_to_use})",
                xaxis_title="Date",
                yaxis_title="Total Quantity",
                hovermode='x unified',
                template='plotly_white'
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Show summary stats
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Avg Inventory", f"{trend_df['Total_Quantity'].mean():,.0f}")
            col2.metric("Max Inventory", f"{trend_df['Total_Quantity'].max():,.0f}")
            col3.metric("Min Inventory", f"{trend_df['Total_Quantity'].min():,.0f}")
            col4.metric("Current Inventory", f"{trend_df['Total_Quantity'].iloc[-1]:,.0f}")
        else:
            st.warning(f"No inventory trend data found for the selected item in the date range.")

    # --- 🏭 UNIFIED WAREHOUSE EXPLORER (Separate Section) ---
    st.markdown("---")
    st.subheader("🏭 Warehouse Explorer: Live + Blocked Inventory (from BigQuery)")
    st.markdown("Use this section to explore warehouse inventory for any product independently")

    # Fetch blocked inventory data
    try:
        blocked_df_all = fetch_blocked_inventory_clean()
        blocked_skus = blocked_df_all["SKU"].unique().tolist() if not blocked_df_all.empty else []
        product_titles_blocked = sorted(blocked_df_all["Product_Name"].dropna().unique().tolist()) if not blocked_df_all.empty else []
        live_skus = report["sku"].unique().tolist()
        all_skus = sorted(list(set(live_skus + blocked_skus)))
    except Exception as e:
        st.error(f"Error fetching blocked SKUs: {e}")
        all_skus, product_titles_blocked = [], []

    # --- Filter Controls for Warehouse Data ---
    colsku, coltitle = st.columns(2)
    with colsku:
        selected_wh_sku = st.selectbox("Select SKU (optional):", options=["None"] + all_skus, key="wh_sku_explorer")
    with coltitle:
        selected_wh_title = st.selectbox("Select Product Title (optional):", options=["None"] + product_titles_blocked, key="wh_title_explorer")

    # --- Logic for Fetching Warehouse Data ---
    if selected_wh_sku != "None" or selected_wh_title != "None":
        # Determine what to display in the title
        display_sku = selected_wh_sku if selected_wh_sku != "None" else None
        display_product = selected_wh_title if selected_wh_title != "None" else None
        
        # If only SKU selected, try to get product name
        if display_sku and not display_product:
            if not blocked_df_all.empty:
                matching_product = blocked_df_all[blocked_df_all["SKU"] == display_sku]["Product_Name"].unique()
                if len(matching_product) > 0:
                    display_product = matching_product[0]
            # Also check in report
            if not display_product or display_product == "":
                matching_in_report = report[report["sku"] == display_sku]["product_title_clean"].unique()
                if len(matching_in_report) > 0:
                    display_product = matching_in_report[0]
        
        # If only product selected, try to get SKU
        if display_product and not display_sku:
            if not blocked_df_all.empty:
                matching_sku = blocked_df_all[blocked_df_all["Product_Name"] == display_product]["SKU"].unique()
                if len(matching_sku) > 0:
                    display_sku = matching_sku[0]
        
        # Display title with both SKU and Product
        st.markdown("---")
        title_parts = []
        if display_product:
            title_parts.append(f"**Product:** {display_product}")
        if display_sku:
            title_parts.append(f"**SKU:** {display_sku}")
        
        if title_parts:
            st.markdown(f"#### 📦 {' | '.join(title_parts)}")
        
        st.info("Fetching live warehouse and blocked data...")

        try:
            warehouse_df = pd.DataFrame()
            blocked_filtered = pd.DataFrame()

            # CASE 1: SKU selected
            if selected_wh_sku != "None":
                warehouse_df = fetch_warehouse_summary(selected_wh_sku)
                if not blocked_df_all.empty:
                    blocked_filtered = blocked_df_all[blocked_df_all["SKU"] == selected_wh_sku]

            # CASE 2: Product Title selected
            elif selected_wh_title != "None":
                if not blocked_df_all.empty:
                    blocked_filtered = blocked_df_all[blocked_df_all["Product_Name"] == selected_wh_title]
                    skus_for_title = blocked_filtered["SKU"].unique().tolist()
                    all_wh = []
                    for sku in skus_for_title:
                        temp_df = fetch_warehouse_summary(sku)
                        if not temp_df.empty:
                            temp_df["SKU"] = sku
                            all_wh.append(temp_df)
                    if all_wh:
                        warehouse_df = pd.concat(all_wh, ignore_index=True)

            # Merge warehouse and blocked data
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

                # Add Total = Available + Blocked
                merged["Available_Inventory"] = merged["Available_Inventory"].astype(float)
                merged["Blocked_Inventory"] = merged["Blocked_Inventory"].astype(float)
                merged["Total"] = merged["Available_Inventory"] + merged["Blocked_Inventory"]
                merged["Company_Name"] = merged["Company_Name"].astype(str).str.strip()

                display_cols_wh = ["Company_Name", "Available_Inventory", "Blocked_Inventory", "Total"]

                st.dataframe(
                    merged[display_cols_wh].style.format({
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
    st.info("👆 Please calculate business loss first using the 🚀 button above.")






