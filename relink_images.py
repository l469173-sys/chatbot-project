import os
import re
import json
import sqlite3
import difflib

DB_PATH = "company_data.db"
IMG_DIR = "crawled_data/images"


def normalize(s: str) -> str:
    s = s.lower()
    s = s.replace("-", "_").replace(" ", "_")
    s = re.sub(r"[^a-z0-9_]+", "", s)
    return s


def main():
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(DB_PATH)
    if not os.path.isdir(IMG_DIR):
        raise FileNotFoundError(IMG_DIR)

    images = [f for f in os.listdir(IMG_DIR)
              if f.lower().endswith((".jpg", ".jpeg", ".png", ".webp"))]

    norm_images = {normalize(f): f for f in images}

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("SELECT id, title, images FROM products").fetchall()

    updated = 0

    for r in rows:
        pid = r["id"]
        title = r["title"] or ""

        key = normalize(title)

        best = difflib.get_close_matches(key, norm_images.keys(), n=1, cutoff=0.4)

        if not best:
            continue

        fname = norm_images[best[0]]

        conn.execute(
            "UPDATE products SET images=? WHERE id=?",
            (json.dumps([fname], ensure_ascii=False), pid)
        )

        print(f"[LINK] {title} -> {fname}")
        updated += 1

    conn.commit()
    conn.close()

    print(f"\nDone. Updated rows = {updated}")


if __name__ == "__main__":
    main()
