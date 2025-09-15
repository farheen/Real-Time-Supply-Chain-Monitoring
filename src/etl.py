import pandas as pd

# 1. Load dataset
df = pd.read_csv("data/smart_logistics_dataset.csv")

# 2. Inspect dataset (columns + first few rows)
print("\n🔎 Columns in dataset:")
print(df.columns.tolist())
print("\n📊 First 5 rows:")
print(df.head())
print("\n📈 Data types:")
print(df.dtypes)

# 3. Basic cleaning
df = df.dropna()

# Try to standardize column names (lowercase, no spaces)
df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

# --- Example: Adjust below based on actual columns in your dataset ---
# Assumptions: dataset has shipment_date, delivery_date, supplier, product, order_quantity, price, cost

# Parse dates (if columns exist)
if "shipment_date" in df.columns and "delivery_date" in df.columns:
    df["shipment_date"] = pd.to_datetime(df["shipment_date"], errors="coerce")
    df["delivery_date"] = pd.to_datetime(df["delivery_date"], errors="coerce")
    df["lead_time_days"] = (df["delivery_date"] - df["shipment_date"]).dt.days

# Compute revenue and profit if columns exist
if "order_quantity" in df.columns and "price" in df.columns:
    df["revenue"] = df["order_quantity"] * df["price"]

if "cost" in df.columns:
    df["profit"] = df["revenue"] - df["cost"]

# Flag on-time delivery if lead_time_days and expected_lead_time exist
if "expected_lead_time" in df.columns and "lead_time_days" in df.columns:
    df["on_time_delivery"] = df["lead_time_days"] <= df["expected_lead_time"]

# 4. Aggregate by supplier and product (if columns exist)
group_cols = []
for col in ["supplier", "product"]:
    if col in df.columns:
        group_cols.append(col)

if group_cols:
    summary = df.groupby(group_cols).agg(
        total_orders=("order_quantity", "sum") if "order_quantity" in df.columns else ("shipment_date", "count"),
        avg_lead_time=("lead_time_days", "mean") if "lead_time_days" in df.columns else ("shipment_date", "count"),
        on_time_rate=("on_time_delivery", "mean") if "on_time_delivery" in df.columns else ("shipment_date", "count"),
        total_revenue=("revenue", "sum") if "revenue" in df.columns else ("shipment_date", "count"),
        total_profit=("profit", "sum") if "profit" in df.columns else ("shipment_date", "count")
    ).reset_index()
else:
    summary = df.copy()

# 5. Save processed summary
summary.to_csv("data/supplychain_summary.csv", index=False)
print("\n✅ Processed supply chain dataset saved at data/processed/supplychain_summary.csv")

