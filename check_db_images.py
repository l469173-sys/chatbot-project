import sqlite3

DB_PATH = "company_data.db"

def scalar(conn, sql):
    cur = conn.execute(sql)
    return cur.fetchone()[0]

with sqlite3.connect(DB_PATH) as conn:
    # 1) products 表有沒有 URL 圖片
    c1 = scalar(conn, "SELECT COUNT(*) FROM products WHERE image LIKE 'http%';")
    c2 = scalar(conn, "SELECT COUNT(*) FROM products WHERE images LIKE '%http%';")

    print("products.image 以 http 開頭的筆數 =", c1)
    print("products.images 含有 http 的筆數   =", c2)

    # 2) 隨機抓幾筆看看 image / images 長什麼樣
    print("\n--- sample rows (最多 10 筆) ---")
    cur = conn.execute("""
        SELECT name, category, image, images
        FROM products
        WHERE (image IS NOT NULL AND image != '')
           OR (images IS NOT NULL AND images != '')
        LIMIT 10;
    """)
    for name, category, image, images in cur.fetchall():
        print("name:", name)
        print("category:", category)
        print("image:", image)
        print("images:", (images[:120] + "…") if images and len(images) > 120 else images)
        print("-" * 40)
