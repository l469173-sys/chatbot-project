import os
import sqlite3
import re

DB_PATH = "company_data.db"
OUTPUT_DIR = "data/product_structured"

os.makedirs(OUTPUT_DIR, exist_ok=True)


def safe_filename(name: str) -> str:
    name = name.strip()
    name = re.sub(r"[\\/:*?\"<>|]", "_", name)
    return name


TEMPLATE = """產品名稱：{title}
產品類型：{category}
主要用途：（請填寫）
可量測項目：（請填寫）
適用光源：（請填寫）
量測範圍：（請填寫）
精度/解析度：（請填寫）
典型應用情境：（研發 / 品保 / 產線 / 實驗室）
是否支援軟體/自動化：（是 / 否）
不適用情境：（非常重要，請填寫）
"""


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT title, category
        FROM products
        WHERE title IS NOT NULL AND title != ''
        ORDER BY title
    """)

    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("❌ 找不到任何產品")
        return

    created = 0

    for title, category in rows:
        fname = safe_filename(title) + ".txt"
        path = os.path.join(OUTPUT_DIR, fname)

        if os.path.exists(path):
            continue  # 已存在就不覆蓋（避免你填過的被洗掉）

        content = TEMPLATE.format(
            title=title.strip(),
            category=(category or "（請填寫）").strip()
        )

        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

        created += 1

    print(f"✅ 已產生 {created} 個產品結構化模板")
    print(f"📂 位置：{OUTPUT_DIR}")


if __name__ == "__main__":
    main()
