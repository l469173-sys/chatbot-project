import os
import re
import sqlite3
import json
import argparse
import shutil
from datetime import datetime


def normalize(text: str) -> str:
    """標準化名稱，用來模糊比對"""
    if not text:
        return ""

    text = text.lower()

    # 移除中文、符號、空白
    text = re.sub(r"[^\w]", "", text)

    return text


def backup_db(db_path):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = f"{db_path}.bak_{ts}"
    shutil.copy(db_path, backup)
    print(f"✔ DB backup: {backup}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--images_dir", required=True)

    args = parser.parse_args()

    db_path = args.db
    img_dir = args.images_dir

    if not os.path.exists(db_path):
        raise FileNotFoundError(db_path)

    if not os.path.isdir(img_dir):
        raise FileNotFoundError(img_dir)

    backup_db(db_path)

    # 讀圖片
    images = [
        f for f in os.listdir(img_dir)
        if f.lower().endswith("_main.jpg")
    ]

    print("found images:", len(images))

    img_map = {}

    for img in images:
        key = normalize(img.replace("_main.jpg", ""))
        img_map[key] = img

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT id, title, images FROM products")
    rows = cur.fetchall()

    updated = 0
    missed = []

    for pid, title, images_json in rows:

        key = normalize(title)

        matched = None

        # 嘗試模糊匹配
        for k, v in img_map.items():
            if k in key or key in k:
                matched = v
                break

        if not matched:
            missed.append(title)
            continue

        new_images = json.dumps([matched], ensure_ascii=False)

        cur.execute("""
            UPDATE products
            SET images=?
            WHERE id=?
        """, (new_images, pid))

        updated += 1

    conn.commit()
    conn.close()

    print("\n==== RESULT ====")
    print("updated:", updated)
    print("missed:", len(missed))

    if missed:
        print("\n---- still missed ----")
        for t in missed:
            print("-", t)


if __name__ == "__main__":
    main()
