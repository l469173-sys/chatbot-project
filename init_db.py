import sqlite3
import json
import os
import re
from typing import Any, Dict


DATA_PATH = "crawled_data/data.json"
DB_PATH = "company_data.db"


def clean_text(s: Any) -> str:
    s = (s or "")
    if not isinstance(s, str):
        s = str(s)
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s


def normalize_category(cat: Any) -> str:
    cat = clean_text(cat)
    return cat if cat else "未分類"


def normalize_images(images: Any) -> str:
    """
    DB 欄位 images 存 JSON 字串，內容是 ['xxx.jpg', 'yyy.png'] 這種檔名。
    若傳入是路徑 'crawled_data/images/xxx.jpg' 也會轉成 'xxx.jpg'
    """
    if not images:
        return "[]"

    if isinstance(images, str):
        try:
            images = json.loads(images)
        except Exception:
            images = [images]

    if not isinstance(images, list):
        images = [images]

    cleaned = []
    for x in images:
        x = clean_text(x)
        if not x:
            continue
        x = x.replace("\\", "/")
        if "/" in x:
            x = x.split("/")[-1]
        cleaned.append(x)

    seen = set()
    uniq = []
    for x in cleaned:
        if x in seen:
            continue
        seen.add(x)
        uniq.append(x)

    return json.dumps(uniq, ensure_ascii=False)


def load_json(path: str) -> Dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ 找不到資料檔案: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def init_database():
    data = load_json(DATA_PATH)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("🗑️  刪除舊表格結構...")
    cursor.execute("DROP TABLE IF EXISTS company_pages")
    cursor.execute("DROP TABLE IF EXISTS news")
    cursor.execute("DROP TABLE IF EXISTS products")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS company_pages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        page_type TEXT NOT NULL,
        url TEXT,
        content TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS news (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        url TEXT,
        content TEXT,
        date TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        url TEXT UNIQUE,
        category TEXT,
        description TEXT,
        specifications TEXT,
        images TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # ===== 匯入公司頁面 + 新聞 =====
    print("📄 匯入公司頁面資料...")
    company_info = data.get("company_info", {}) or {}

    # ✅ 同時支援兩種結構：
    # A) 舊：company_info = {url, content}
    # B) 新：company_info = {home:{}, about:{}, contact:{}, news:{}}
    if isinstance(company_info, dict) and ("home" in company_info or "about" in company_info or "contact" in company_info or "news" in company_info):
        # --- 新結構 ---
        for page_type in ["home", "about", "contact"]:
            page = company_info.get(page_type) or {}
            url = clean_text(page.get("url", ""))
            content = clean_text(page.get("content", ""))
            cursor.execute(
                "INSERT INTO company_pages (page_type, url, content) VALUES (?, ?, ?)",
                (page_type, url, content)
            )

        # --- 新聞 ---
        print("📰 匯入新聞資料...")
        news_block = company_info.get("news") or {}
        if isinstance(news_block, dict) and "items" in news_block:
            items = news_block.get("items", []) or []
            for item in items:
                title = clean_text(item.get("title", ""))
                url = clean_text(item.get("url", news_block.get("url", "")))
                content = clean_text(item.get("content", ""))
                date = clean_text(item.get("date", ""))
                if title and title != "+更多":
                    cursor.execute(
                        "INSERT INTO news (title, url, content, date) VALUES (?, ?, ?, ?)",
                        (title, url, content, date)
                    )
        else:
            # 若 news 只有全文 content（沒 items），就存成一則摘要新聞
            content = clean_text(news_block.get("content", ""))
            url = clean_text(news_block.get("url", ""))
            if content:
                cursor.execute(
                    "INSERT INTO news (title, url, content, date) VALUES (?, ?, ?, ?)",
                    ("新聞頁摘要", url, content, "")
                )

    else:
        # --- 舊結構 fallback ---
        url = clean_text(company_info.get("url", ""))
        content = clean_text(company_info.get("content", ""))
        cursor.execute(
            "INSERT INTO company_pages (page_type, url, content) VALUES (?, ?, ?)",
            ("about", url, content)
        )
        print("📰 匯入新聞資料...（舊 company_info 結構通常沒有 news，略過）")

    # ===== 匯入產品 =====
    print("📦 匯入產品資料...")
    products = data.get("products", []) or []

    inserted = 0
    skipped = 0

    for p in products:
        title = clean_text(p.get("title", ""))
        url = clean_text(p.get("url", ""))
        category = normalize_category(p.get("category", ""))
        description = clean_text(p.get("description", ""))
        specifications = (p.get("specifications", "") or "").strip()
        images_json = normalize_images(p.get("images", []))

        if not url:
            skipped += 1
            continue
        if not title:
            title = url

        try:
            cursor.execute(
                """
                INSERT INTO products (title, url, category, description, specifications, images)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (title, url, category, description, specifications, images_json)
            )
            inserted += 1
        except sqlite3.IntegrityError:
            skipped += 1

    conn.commit()

    # ===== 統計 =====
    cursor.execute("SELECT COUNT(*) FROM company_pages")
    pages_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM news")
    news_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM products")
    products_count = cursor.fetchone()[0]

    cursor.execute("""
        SELECT
          CASE
            WHEN category IS NULL OR TRIM(category) = '' THEN '未分類'
            ELSE TRIM(category)
          END AS cat,
          COUNT(*)
        FROM products
        GROUP BY cat
        ORDER BY COUNT(*) DESC
    """)
    category_counts = cursor.fetchall()

    conn.close()

    print("\n" + "=" * 50)
    print("✅ 資料庫初始化完成!")
    print("=" * 50)
    print(f"📄 公司頁面: {pages_count} 筆")
    print(f"📰 新聞資料: {news_count} 筆")
    print(f"📦 產品資料: {products_count} 筆")
    print("📊 產品分類統計（加總應等於產品總數）:")
    total_check = 0
    for cat, cnt in category_counts:
        print(f"  - {cat}: {cnt} 個")
        total_check += cnt
    print(f"✅ 分類加總: {total_check}（應等於 {products_count}）")
    print(f"🧾 products 匯入：inserted={inserted}, skipped={skipped}")
    print("=" * 50)


if __name__ == "__main__":
    init_database()
