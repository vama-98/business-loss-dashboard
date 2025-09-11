# streamlit_business_loss_app.py
# -----------------------------------------------------------
# Business Loss Dashboard (Gemini-enabled)
# - Upload 3 CSVs:
#   1) Products: Title | Variant ID | Status
#   2) Days OOS: Product title | Product variant ID | Days out of stock (at location)
#   3) Rates: Variant ID | DRR | optional ASP
# - Merge by Variant ID, keep Status == Active, exclude Days == 0
# - Compute Business Loss per variant, plus Total
# - Sort alphabetically (Product title or Variant ID)
# - Download: variant-wise CSV, summary CSV, Excel (2 sheets)
# - Natural language Q&A powered by Gemini (Google AI Studio)
#
# Run:
#   pip install --upgrade pip
#   pip install streamlit pandas google-genai
#   streamlit run streamlit_business_loss_app.py
#
# Provide your Gemini key via:
# - Sidebar field (Gemini API Key), or
# - Environment var: setx GEMINI_API_KEY "YOUR_KEY"  (Windows, then restart terminal)
#                    export GEMINI_API_KEY="YOUR_KEY" (macOS/Linux)

import os
import io
import json
import pandas as pd
import numpy as np
import streamlit as st
from google import genai

# ---------------------- Config ----------------------
st.set_page_config(page_title="Business Loss Dashboard", layout="wide")
PRIMARY_COLOR = "#7C3AED"

REQ_PRODUCTS = {"Title", "Variant ID", "Status"}
REQ_DAYS = {"Product title", "Product variant ID", "Days out of stock (at location)"}
REQ_RATES = {"Variant ID", "DRR"}  # ASP optional
DAYS_COL = "Days out of stock (at location)"

# ---------------------- Helpers ----------------------
def read_csv_safely(uploaded_file, dtype=None):
    """Read a CSV from Streamlit's UploadedFile with optional dtype mapping."""
    if uploaded_file is None:
        return None
    try:
        return pd.read_csv(uploaded_file, dtype=dtype)
    except Exception:
        uploaded_file.seek(0)
        return pd.read_csv(uploaded_file, encoding="utf-8", dtype=dtype)

def normalize_id_series(s: pd.Series) -> pd.Series:
    """Ensure IDs are strings, strip, drop trailing .0 to avoid sci-notation issues."""
    if s is None:
        return s
    s = s.astype(str).str.strip()
    s = s.str.replace(r"\.0$", "", regex=True)
    return s

def validate_columns(df: pd.DataFrame, required: set, label: str) -> list:
    if df is None:
        return [f"{label}: file missing."]
    cols = set(df.columns.str.strip())
    missing = list(required - cols)
    return [f"{label}: missing column '{m}'." for m in missing]

def rupee(n: float) -> str:
    try:
        return f"₹{n:,.0f}"
    except Exception:
        return str(n)

def dedupe_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicate header names, keep the first occurrence."""
    if df is None:
        return df
    df.columns = df.columns.str.strip()
    return df.loc[:, ~df.columns.duplicated()]

# ---------------------- Sidebar ----------------------
st.sidebar.markdown(f"<h2 style='color:{PRIMARY_COLOR}'>Settings</h2>", unsafe_allow_html=True)

uploaded_products = st.sidebar.file_uploader("1) Upload Products CSV", type=["csv"], key="products")
uploaded_days = st.sidebar.file_uploader("2) Upload Days OOS CSV", type=["csv"], key="days")
uploaded_rates = st.sidebar.file_uploader("3) Upload Rates CSV", type=["csv"], key="rates")

st.sidebar.divider()
default_asp = st.sidebar.number_input("Default ASP (used if ASP missing)", min_value=0, value=250, step=10)
default_drr = st.sidebar.number_input("Default DRR (used if DRR missing)", min_value=0, value=5, step=1)
sort_by = st.sidebar.selectbox("Sort alphabetically by", ["Product title", "Variant ID"], index=0)

st.sidebar.divider()
# Resolve API key (env vars or secrets), but don't hard-code anything
api_key_env = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or ""
try:
    api_key_secret = st.secrets["GEMINI_API_KEY"]
except Exception:
    api_key_secret = ""
api_key = api_key_env or api_key_secret
api_key = st.sidebar.text_input("Gemini API Key (AI Studio)", type="password", value=api_key)
model_name = st.sidebar.text_input("Model", value="gemini-2.5-flash")
max_rows_for_context = st.sidebar.slider("Max rows to include in NL context", min_value=50, max_value=2000, value=300, step=50)

# ---------------------- Main Header ----------------------
st.markdown(
    f"""
<div style='display:flex;align-items:center;gap:10px;'>
  <div style='font-size:28px;font-weight:700;'>Business Loss Dashboard</div>
  <div style='background:{PRIMARY_COLOR};color:white;padding:4px 10px;border-radius:999px;font-size:12px;'>Gemini-enabled</div>
</div>
""",
    unsafe_allow_html=True,
)
st.caption("Upload your three CSVs, and this app will compute variant-wise and total business loss. Then ask questions with Gemini.")

# ---------------------- Read Files ----------------------
products = read_csv_safely(uploaded_products, dtype={"Variant ID": str}) if uploaded_products else None
days = read_csv_safely(uploaded_days, dtype={"Product variant ID": str}) if uploaded_days else None
rates = read_csv_safely(uploaded_rates, dtype={"Variant ID": str}) if uploaded_rates else None

# Deduplicate any duplicate headers
products = dedupe_headers(products)
days = dedupe_headers(days)
rates = dedupe_headers(rates)

# Validate
errors = []
errors += validate_columns(products, REQ_PRODUCTS, "Products")
errors += validate_columns(days, REQ_DAYS, "Days OOS")
errors += validate_columns(rates, REQ_RATES, "Rates")

if errors:
    with st.expander("Validation & required columns", expanded=True):
        for e in errors:
            st.error(e)
    st.stop()

# ---------------------- Transform & Merge ----------------------
# Normalize IDs (strings) and numeric days
products["Variant ID"] = normalize_id_series(products["Variant ID"])
rates["Variant ID"] = normalize_id_series(rates["Variant ID"])

try:
    days[DAYS_COL] = pd.to_numeric(days[DAYS_COL], errors="coerce").fillna(0)
except Exception:
    days[DAYS_COL] = 0

# Build a SINGLE 'Variant ID' column for Days OOS without renaming into a duplicate
vid_series = normalize_id_series(days["Product variant ID"])
# Drop any pre-existing 'Variant ID' col to avoid duplicate header names
days = days.drop(columns=[c for c in days.columns if c.strip().lower() == "variant id"], errors="ignore")
days["Variant ID"] = vid_series

# Join Days -> Products (get Status / Title), keep Active, exclude 0 days
merged = days.merge(
    products[["Variant ID", "Status", "Title"]],
    on="Variant ID",
    how="inner",
)

merged["Status"] = merged["Status"].astype(str)
merged = merged[merged["Status"].str.strip().str.lower() == "active"]
merged = merged[merged[DAYS_COL] > 0]

# Prefer Product title from Days; fallback to Products Title if missing
if "Product title" in merged.columns:
    merged["Product title"] = merged["Product title"].fillna(merged["Title"])
else:
    merged["Product title"] = merged["Title"]

# Add Rates (DRR, optional ASP)
merged = merged.merge(rates, on="Variant ID", how="left")

# Fill DRR/ASP fallbacks
merged["DRR"] = pd.to_numeric(merged.get("DRR"), errors="coerce").fillna(default_drr)
if "ASP" in merged.columns:
    merged["ASP"] = pd.to_numeric(merged.get("ASP"), errors="coerce").fillna(default_asp)
else:
    merged["ASP"] = default_asp

# Compute Business Loss
merged["Business Loss"] = merged[DAYS_COL] * merged["DRR"] * merged["ASP"]

# Arrange alphabetically
if sort_by == "Product title":
    merged = merged.sort_values(["Product title", "Variant ID"], ascending=[True, True])
else:
    merged = merged.sort_values(["Variant ID", "Product title"], ascending=[True, True])

final_cols = ["Product title", "Variant ID", "Status", DAYS_COL, "DRR", "ASP", "Business Loss"]
final_df = merged[final_cols].reset_index(drop=True)

# ---------------------- KPIs ----------------------
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Unique Variants", int(final_df["Variant ID"].nunique()))
with col2:
    st.metric("Total OOS Days", int(final_df[DAYS_COL].sum()))
with col3:
    st.metric("Avg DRR", round(float(final_df["DRR"].mean()), 2))
with col4:
    st.metric("Total Business Loss", rupee(float(final_df["Business Loss"].sum())))

st.divider()

# ---------------------- Tables & Downloads ----------------------
st.subheader("Variant-wise Business Loss (sorted)")
st.dataframe(final_df, use_container_width=True)

# CSV downloads
csv_buffer = io.StringIO()
final_df.to_csv(csv_buffer, index=False)
st.download_button(
    label="↓ Download variant-wise CSV",
    data=csv_buffer.getvalue(),
    file_name="business_loss_variant_wise.csv",
    mime="text/csv",
)

summary_df = pd.DataFrame({
    "Total Business Loss": [final_df["Business Loss"].sum()],
    "Total OOS Days": [final_df[DAYS_COL].sum()],
    "Unique Variants": [final_df["Variant ID"].nunique()],
})
sum_buffer = io.StringIO()
summary_df.to_csv(sum_buffer, index=False)
st.download_button(
    label="↓ Download summary CSV",
    data=sum_buffer.getvalue(),
    file_name="business_loss_summary.csv",
    mime="text/csv",
)

# Excel (two sheets) — optional, if engine available
excel_bytes = None
try:
    excel_bytes = io.BytesIO()
    with pd.ExcelWriter(excel_bytes) as writer:  # engine auto-select
        final_df.to_excel(writer, index=False, sheet_name="Variant-wise")
        summary_df.to_excel(writer, index=False, sheet_name="Summary")
    excel_bytes.seek(0)
except Exception:
    excel_bytes = None

if excel_bytes:
    st.download_button(
        label="↓ Download Excel (variant + summary)",
        data=excel_bytes,
        file_name="business_loss_report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

st.divider()

# ---------------------- Chart ----------------------
st.subheader("Top N SKUs by Business Loss")
N = st.slider("Choose N", 5, 50, 15)
chart_df = (
    final_df[["Product title", "Business Loss"]]
    .groupby("Product title", as_index=False)
    .sum()
    .sort_values("Business Loss", ascending=False)
    .head(N)
)
st.bar_chart(chart_df.set_index("Product title"))

st.divider()

# ---------------------- Natural Language Q&A (Gemini) ----------------------
st.subheader("Ask questions about this data (Gemini API)")
question = st.text_area("Type your question (e.g., 'Which product has the highest loss?')", height=80)

if st.button("Ask"):
    # Resolve API key: sidebar > env vars
    gemini_key = api_key or os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or ""

    if not gemini_key:
        st.warning("Please provide a Gemini API key in the sidebar or set GEMINI_API_KEY.")
    elif final_df.empty:
        st.info("No data to analyze. Please upload files above.")
    else:
        try:
            client = genai.Client(api_key=gemini_key)

            # Compact context for token control
            sample_df = final_df.copy()
            if len(sample_df) > max_rows_for_context:
                sample_df = (
                    sample_df.sort_values("Business Loss", ascending=False)
                    .head(max_rows_for_context)
                )
            context_csv = sample_df.to_csv(index=False)
            totals = {
                "total_business_loss": float(final_df["Business Loss"].sum()),
                "total_oos_days": int(final_df[DAYS_COL].sum()),
                "unique_variants": int(final_df["Variant ID"].nunique()),
            }

            sys_msg = (
                "You are a careful retail/ops analyst. Answer ONLY using the provided CSV context. "
                "Show computed numbers clearly. If not answerable from the data, say so."
            )

            # Build prompt without multiline f-strings
            cols_str = ", ".join([str(c) for c in final_df.columns])
            totals_str = json.dumps(totals)
            user_prompt = "\n".join([
                f"Columns: {cols_str}",
                f"Totals JSON: {totals_str}",
                f"CSV Context (sample up to {len(sample_df)} rows):",
                context_csv,
                "",
                f"Question: {question}",
            ])

            resp = client.models.generate_content(
                model=model_name,
                contents=f"SYSTEM: {sys_msg}\n\n{user_prompt}"
            )
            answer = getattr(resp, "text", None) or str(resp)

            st.success("Answer:")
            st.write(answer)
        except Exception as e:
            st.error(f"Gemini error: {e}")

# ---------------------- Footer ----------------------
st.caption("© Business Loss Dashboard — Upload, merge, compute, and ask.")
