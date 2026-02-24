import os
import re
import sqlite3
import json
import argparse
import shutil
from datetime import datetime


def backup(db):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bk = f"{db}.bak_{ts}"
    shutil.copy(db, bk)
    print("✔ backup:", bk)


def extract_model(title: str) -> str | None:
    """
    從產品名稱抓型號
    ex: 光譜色彩照度計 SRI-2000 -> SRI-2000
    """

    if not title:
        return None

    # 抓 英文+數字+-
    m = re.findall(r"[A-Z]{2,}[-_]?\d+[A-Z]*", title.upper())

    if not m:
        return None

    return m[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--images_dir", required=True)

    args = parser.parse_args()

    db = args.db
    img_dir = args.images_dir

    if not os.path.exists(db):
        raise FileNotFoundError(db)

    if not os.path.isdir(img_dir):
        raise FileNotFoundError(img_dir)

    backup(db)

    images = [
        f for f in os.listdir(img_dir)
        if f.lower().endswith("_main.jpg")
    ]

    print("found images:", len(images))

    image_map = {}

    for img in images:
        key = img.replace("_main.jpg", "").replace("_", "-").upper()
        image_map[key] = img

    conn = sqlite3.connect(db)
    cur = conn.cursor()

    cur.execute("SELECT id, title, images FROM products")
    rows = cur.fetchall()

    updated = 0
    missed = []

    for pid, title, old_images in rows:

        model = extract_model(title)

        if not model:
            missed.append(title)
            continue

        key = model.replace("_", "-").upper()

        matched = None

        for k, v in image_map.items():
            if key in k or k in key:
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
        for m in missed:
            print("-", m)


if __name__ == "__main__":
    main()
