import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# Загружаем parquet
df = pd.read_parquet("../data/SnapShotForMongoDB/ozon_inference_2025_10_17_offers_2025_10_17.pq")

# Разбиваем путь категории
def process_path(path_str):
    parts = path_str.split("\\")
    breadcrumbs = []
    now = datetime.utcnow()
    for i, name in enumerate(parts, start=1):
        breadcrumbs.append({
            "level": i,
            "name": name,
            "created_at": now,
            "updated_at": now
        })
    return "/".join(parts), breadcrumbs, parts[-1]

# Добавляем колонки для категории
df[['full_path', 'breadcrumbs', 'cat_name']] = df['Category_FullPathName'].apply(
    lambda x: pd.Series(process_path(x))
)

# Формируем документы
docs = []
for idx, row in df.iterrows():
    doc_id = f"{row['Partner_Name']}_{row['Offer_ID']}"  # PK
    docs.append({
        "_id": doc_id,
        "partner": row['Partner_Name'],
        "offer_id": str(row['Offer_ID']),
        "name": row['Offer_Name'],
        "type": row['Offer_Type'],
        "category": {
            "id": str(row['Category_ID']),
            "name": row['cat_name'],
            "full_path": row['full_path'],
            "breadcrumbs": row['breadcrumbs']
        },
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow()
    })

# Вставка в Mongo
client = MongoClient("mongodb://root:root@localhost:27017")
db = client['ecom_catalog']
db.products.drop()  # если нужно перезаписать
db.products.insert_many(docs)

# Статистика
total_products = len(docs)
top5_types = df['Offer_Type'].value_counts().head(5)
per_partner = df['Partner_Name'].value_counts()

print(f"Общее количество товаров: {total_products}")
print("Топ-5 типов товаров:")
print(top5_types)
print("Распределение товаров по партнерам:")
print(per_partner)
