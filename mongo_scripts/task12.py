import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# Загружаем parquet
df = pd.read_parquet("../data/SnapShotForMongoDB/ozon_inference_2025_10_17_offers_2025_10_17.pq")

# Функция для разбивки пути
def process_path(path_str):
    parts = path_str.split("\\")
    level = len(parts)
    name = parts[-1]
    parent_path = "/".join(parts[:-1]) if level > 1 else None
    path = "/".join(parts)
    return name, path, parts, level, parent_path

# Добавляем колонки для категорий
df[['cat_name', 'path', 'path_array', 'level', 'parent_path']] = df['Category_FullPathName'].apply(
    lambda x: pd.Series(process_path(x))
)

# Группируем по категории
categories = (
    df.groupby(['Partner_Name', 'Category_ID'])
    .agg(
        partner=('Partner_Name', 'first'),
        category_id=('Category_ID', 'first'),
        name=('cat_name', 'first'),
        path=('path', 'first'),
        path_array=('path_array', 'first'),
        level=('level', 'first'),
        parent_path=('parent_path', 'first'),
        total_products=('Offer_ID', 'count')
    )
    .reset_index(drop=True)
)

# Формируем документы Mongo
docs = []
for _, row in categories.iterrows():
    docs.append({
        "_id": f"{row['partner']}_{row['category_id']}",
        "partner": row['partner'],
        "category_id": str(row['category_id']),
        "name": row['name'],
        "path": row['path'],
        "path_array": row['path_array'],
        "level": int(row['level']),
        "parent_path": row['parent_path'],
        "metadata": {
            "total_products": int(row['total_products']),
            "last_updated": datetime.utcnow()
        }
    })

# Подключение к MongoDB и вставка
client = MongoClient("mongodb://root:root@localhost:27017")
db = client['ecom_catalog']
db.categories.drop()  # опционально, если хотим перезаписать
db.categories.insert_many(docs)

# Статистика по уровням
level_stats = categories['level'].value_counts().sort_index()
print(f"Общее количество категорий: {len(categories)}")
print("Распределение по уровням:")
print(level_stats)
