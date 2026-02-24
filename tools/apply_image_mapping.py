# apply_image_mapping.py
# 目的：
# - 讀 mapping_final.json
# - 把 products.images 更新成 ["<filename>"]（JSON list）
# - 只改 images 欄位，不碰 image
# - 執行前自動備份 DB：company_data.db.bak_YYYYmmdd_HHMMSS
#
# 用法：
#   python tools/apply_image_mapping.py
#   python tools/apply_image_mapping.py --db company_data.db --mapping mapping_final.json
#   python tools/apply_image_mapping.py --dry-run
#
import os
import json
import shutil
import argparse
import sqlite3
from datetime import datetime
from typing import Dict, Any, List

ALLOWED_EXT = (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tif", ".tiff")


def now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def backup_db(db_path: str) -> str:
    bak = f"{db_path}.bak_{now_tag()}"
    shutil.copy2(db_path, bak)
    return bak


def load_mapping(path: str) -> Dict[str, str]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    mapping = payload.get("mapping")
    if not isinstance(mapping, dict):
        raise ValueError("mapping_final.json 格式錯誤：找不到 mapping dict")
    out: Dict[str, str] = {}
    for k, v in mapping.items():
        k2 = (k or "").strip()
        v2 = (v or "").strip().replace("\\", "/")
        if not k2 or not v2:
            continue
        out[k2] = v2
    return out


def normalize_filename(fn: str) -> str:
    fn = (fn or "").strip().replace("\\", "/")
    fn = fn.replace("crawled_data/images/", "").lstrip("/")
    return fn


def is_allowed(fn: str) -> bool:
    low = fn.lower()
    return low.endswith(ALLOWED_EXT)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.getenv("DB_PATH", "company_data.db"))
    ap.add_argument("--mapping", default="mapping_final.json")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only-when-empty", action="store_true", help="只更新 images 為空的產品（保守模式）")
    args = ap.parse_args()

    if not os.path.exists(args.db):
        raise SystemExit(f"DB 不存在：{args.db}")
    if not os.path.exists(args.mapping):
        raise SystemExit(f"mapping 檔不存在：{args.mapping}")

    mapping = load_mapping(args.mapping)
    if not mapping:
        raise SystemExit("mapping_final.json 內 mapping 是空的")

    bak_path = ""
    if not args.dry_run:
        bak_path = backup_db(args.db)

    updated = 0
    not_found = 0
    skipped = 0
    bad_ext = 0

    with sqlite3.connect(args.db, timeout=30) as conn:
        conn.row_factory = sqlite3.Row

        # 建 title->id
        cur = conn.execute("SELECT id, title, images FROM products")
        rows = cur.fetchall()

        title_to_row = {}
        for r in rows:
            t = (r["title"] or "").strip()
            if t:
                title_to_row[t] = r

        for title, fn in mapping.items():
            r = title_to_row.get(title)
            if not r:
                not_found += 1
                continue

            fn2 = normalize_filename(fn)
            if not is_allowed(fn2):
                bad_ext += 1
                continue

            if args.only_when_empty:
                old_images = r["images"]
                try:
                    j = json.loads(old_images) if isinstance(old_images, str) and old_images.strip() else []
                except Exception:
                    j = []
                if isinstance(j, list) and len(j) > 0:
                    skipped += 1
                    continue

            new_val = json.dumps([fn2], ensure_ascii=False)

            if args.dry_run:
                updated += 1
                continue

            conn.execute("UPDATE products SET images=? WHERE id=?", (new_val, r["id"]))
            updated += 1

        if not args.dry_run:
            conn.commit()

    print("✅ apply_image_mapping 完成")
    if bak_path:
        print(f" - DB backup: {bak_path}")
    print(f" - updated: {updated}")
    print(f" - skipped(existing images): {skipped}")
    print(f" - not_found(title not in DB): {not_found}")
    print(f" - bad_ext(not image ext): {bad_ext}")
    if args.dry_run:
        print(" - dry-run: 沒有寫入 DB")


if __name__ == "__main__":
    main()
