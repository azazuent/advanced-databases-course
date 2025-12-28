import pandas as pd

df = pd.read_parquet("../data/SnapShotForMongoDB/ozon_inference_2025_10_17_offers_2025_10_17.pq")

unique_partners = df["Partner_Name"].nunique()

unique_categories = df["Category_ID"].nunique()
unique_category_paths = df["Category_FullPathName"].nunique()

unique_offer_ids = df["Offer_ID"].nunique()
unique_offer_names = df["Offer_Name"].nunique()
unique_offer_types = df["Offer_Type"].nunique()

# 2. Максимальная глубина категорий
df["cat_depth"] = df["Category_FullPathName"].str.split("\\\\").apply(len)
max_depth = df["cat_depth"].max()

# 3. Категории с наибольшим числом товаров
top_categories = (
    df.groupby(["Partner_Name", "Category_ID"])
    .size()
    .sort_values(ascending=False)
)

# считаем товары в категории
cat_counts = (
    df.groupby(["Partner_Name", "Category_ID"])
    .size()
    .reset_index(name="product_count")
)

# максимум
max_count = cat_counts["product_count"].max()

# сколько категорий имеют этот максимум
num_max_categories = (
    cat_counts[cat_counts["product_count"] == max_count]
    .shape[0]
)

# 4. Offer_ID у разных партнеров
duplicate_offers = (
    df.groupby("Offer_ID")["Partner_Name"]
    .nunique()
    .reset_index()
    .query("Partner_Name > 1")
)

pass
