arr_drr = pd.read_csv(arr_drr_url)
arr_drr.columns = arr_drr.columns.str.strip().str.lower().str.replace(" ", "_")
arr_drr.rename(columns={"sku_code": "sku"}, inplace=True)

st.write("🔍 ARR_DRR Columns:", list(arr_drr.columns))
st.write("📄 ARR_DRR Sample:", arr_drr.head(10))

# Clean variant IDs
arr_drr["variant_id"] = arr_drr["variant_id"].astype(str).str.strip()
report["variant_id"] = report["variant_id"].astype(str).str.strip()

# Debug mismatched IDs
sample_inventory_ids = report["variant_id"].head(10).tolist()
sample_arrdrr_ids = arr_drr["variant_id"].head(10).tolist()
common_ids = set(report["variant_id"]).intersection(set(arr_drr["variant_id"]))

st.write("🧾 Inventory sample variant_ids:", sample_inventory_ids)
st.write("🧾 ARR_DRR sample variant_ids:", sample_arrdrr_ids)
st.write(f"✅ Common variant_id count: {len(common_ids)}")

# Now merge
report = pd.merge(
    report,
    arr_drr[["variant_id", "product_title", "drr", "asp", "sku"]],
    on="variant_id",
    how="left"
)
