# 🟢 ARR_DRR LOAD
arr_drr = pd.read_csv(arr_drr_url)

# Clean header names aggressively
arr_drr.columns = (
    arr_drr.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
    .str.replace("₹", "", regex=False)
    .str.replace("(", "", regex=False)
    .str.replace(")", "", regex=False)
)

# Map alternate names
arr_drr.rename(columns={
    "sku_code": "SKU Code",
    "product_name": "product_title",
    "product": "product_title",
    "asp_inr": "asp",
    "asp_₹": "asp",
    "asp_rs": "asp",
    "avg_selling_price": "asp",
    "daily_run_rate": "drr",
    "drr_units_day": "drr"
}, inplace=True)

# Normalize SKU
arr_drr["SKU Code"] = arr_drr["SKU Code"].astype(str).str.strip().str.upper().str.replace("-", "", regex=False)

# ✅ Ensure required columns exist
required_cols = {"variant_id", "product_title", "drr", "asp", "SKU Code"}
missing = required_cols - set(arr_drr.columns)
if missing:
    raise ValueError(f"❌ Missing columns in ARR/DRR sheet: {missing}")

arr_drr["variant_id"] = arr_drr["variant_id"].astype(str)
report["variant_id"] = report["variant_id"].astype(str)

report = pd.merge(
    report,
    arr_drr[["variant_id", "product_title", "drr", "asp", "SKU Code"]],
    on="variant_id",
    how="left"
)
