import json
from urllib.parse import urlparse, urlunparse

DATA_PATH = "crawled_data/data.json"


def normalize_url(url: str) -> str:
    """
    將 URL 正規化：
    - 去掉 query (?xxx)
    - 去掉 fragment (#xxx)
    - 去除尾端 /
    """
    if not url:
        return ""

    p = urlparse(url)
    clean = urlunparse((
        p.scheme,
        p.netloc,
        p.path.rstrip("/"),
        "",  # query
        ""   # fragment
    ))
    return clean


with open(DATA_PATH, "r", encoding="utf-8") as f:
    data = json.load(f)

products = data.get("products", [])
original_count = len(products)

print(f"原始商品數量: {original_count}")

seen_urls = set()
unique_products = []
removed = []

for product in products:
    raw_url = product.get("url", "")
    norm_url = normalize_url(raw_url)

    # URL 為空：保留，但不參與去重
    if not norm_url:
        unique_products.append(product)
        continue

    if norm_url not in seen_urls:
        seen_urls.add(norm_url)
        product["url"] = norm_url   # 順便回寫乾淨 URL
        unique_products.append(product)
    else:
        removed.append(product)
        print(f"🗑️ 移除重複商品: {product.get('title', 'Unknown')}")

data["products"] = unique_products

with open(DATA_PATH, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("\n" + "=" * 50)
print(f"去重後商品數量: {len(unique_products)}")
print(f"實際移除重複商品: {len(removed)}")
print("=" * 50)

print("\n📋 去重後商品列表：")
for i, product in enumerate(unique_products, 1):
    title = product.get("title", "Unknown")
    category = product.get("category", "未分類")
    print(f"{i}. {title} ({category})")
