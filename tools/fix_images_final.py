import os
import re
import sqlite3
import json
import argparse
import shutil
from datetime import datetime


# ⭐ 人工補齊表
MANUAL_MAP = {
    "積分球 ISP-XXXX": "ISP_XXXX_main.jpg",
    "鍍金積分球 ISP-XXXGL": "ISP_XXXGL_main.jpg",
    "IS治具": "IS_fixture_main.jpg",
    "光通量(流明)量測系統 LM-ISP-XXXX": "LM_ISP_XXXX_main.jpg",
    "CMOS影像偵測器量測系統": "CMOS_main.jpg",
    "廣角鏡頭量測-積分球均勻光源": "integrating_sphere_uniform_light_source_main.jpg",
    "VCSEL量測儀": "VCSEL_main.jpg"
}


def backup(db):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bk = f"{db}.bak_{ts}"
    shutil.copy(db, bk)
    print("✔ backup:", bk)


def extract_model(title):

    if not title:
        return None

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

    backup(db)

    images = [
        f for f in os.listdir(img_dir)
        if f.lower().endswith("_main.jpg")
    ]

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

        # ✅ 先吃人工表
        if title in MANUAL_MAP:

            img = MANUAL_MAP[title]

            cur.execute("""
                UPDATE products
                SET images=?
                WHERE id=?
            """, (json.dumps([img]), pid))

            updated += 1
            continue


        # ✅ 再跑自動
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

        cur.execute("""
            UPDATE products
            SET images=?
            WHERE id=?
        """, (json.dumps([matched]), pid))

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
