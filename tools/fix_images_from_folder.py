# fix_images_from_folder.py
# 功能：從 crawled_data/images 讀全部 jpg（優先 *_main.jpg），用更嚴格規則把 products.images 補齊
# - 只更新 images 為空的產品
# - 自動備份 DB
# - 只動 products.images，不碰 image
#
# 用法：
#   python fix_images_from_folder.py

import os
import re
import json
import shutil
import sqlite3
from pathlib import Path
from datetime import datetime
from difflib import get_close_matches
from typing import List, Dict, Optional

DB_PATH = os.getenv("DB_PATH", "company_data.db")
IMAGE_DIR = Path(os.getenv("IMAGE_DIR", r".\crawled_data\images"))
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"

MANUAL_MAPPING: Dict[str, str] = {
    "積分球 ISP-XXXX": "ISP_XXXX_main.jpg",
    "鍍金積分球 ISP-XXXGL": "ISP_XXXGL_main.jpg",
    "IS治具": "IS_fixture_main.jpg",
    "光通量(流明)量測系統 LM-ISP-XXXX": "LM_ISP_XXXX_main.jpg",
    "CMOS影像(感測器)量測系統": "CMOS_main.jpg",
    "廣角鏡頭量測-積分球均勻光源": "integrating_sphere_uniform_light_source_main.jpg",
    "VCSEL量測儀": "VCSEL_main.jpg",
}

def now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def backup_db(db_path: str) -> str:
    src = Path(db_path)
    if not src.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")
    dst = src.with_suffix(src.suffix + f".bak_{now_ts()}")
    shutil.copy2(src, dst)
    return str(dst)

def safe_json_list(s: Optional[str]) -> List[str]:
    if not s:
        return []
    if isinstance(s, list):
        return [str(x) for x in s if x]
    try:
        v = json.loads(s)
        return v if isinstance(v, list) else []
    except Exception:
        s = str(s).strip()
        if s.startswith("[") and s.endswith("]") and "'" in s and '"' not in s:
            try:
                v2 = json.loads(s.replace("'", '"'))
                return v2 if isinstance(v2, list) else []
            except Exception:
                return []
        return []

def normalize(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("-", "_")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9\u4e00-\u9fff_]+", "", s)
    return s

def pick_best_image_for_key(key_norm: str, candidates_norm: List[str], norm_to_filename: Dict[str, str]) -> Optional[str]:
    if not key_norm:
        return None
    matches = get_close_matches(key_norm, candidates_norm, n=8, cutoff=0.55)
    if not matches:
        return None
    # 優先 *_main
    matches_sorted = sorted(matches, key=lambda x: (0 if x.endswith("_main") else 1, -len(x)))
    return norm_to_filename.get(matches_sorted[0])

def main():
    print(f"[INFO] DB_PATH: {DB_PATH}")
    print(f"[INFO] IMAGE_DIR: {IMAGE_DIR}")
    print(f"[INFO] DRY_RUN: {DRY_RUN}")

    backup_path = backup_db(DB_PATH)
    print(f"[OK] DB backup created: {backup_path}")

    if not IMAGE_DIR.exists():
        raise FileNotFoundError(f"Image dir not found: {IMAGE_DIR}")

    all_jpg = list(IMAGE_DIR.glob("*.jpg"))
    main_jpg = [p for p in all_jpg if p.name.lower().endswith("_main.jpg")]
    preferred = main_jpg + [p for p in all_jpg if p not in main_jpg]

    norm_to_filename: Dict[str, str] = {}
    candidates_norm: List[str] = []
    for p in preferred:
        stem_norm = normalize(p.stem)
        if stem_norm not in norm_to_filename:
            norm_to_filename[stem_norm] = p.name
            candidates_norm.append(stem_norm)

    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, COALESCE(title,''), COALESCE(model,''), COALESCE(images,'') FROM products")
        rows = cur.fetchall()

        updated = 0
        missed = []

        for pid, title, model, images in rows:
            if safe_json_list(images):
                continue

            # 1) manual
            hit = None
            for k, fn in MANUAL_MAPPING.items():
                if k and (k in title or k in model):
                    if (IMAGE_DIR / fn).exists():
                        hit = fn
                        break
            if hit:
                print(f"[MANUAL] id={pid} title={title} -> {hit}")
                if not DRY_RUN:
                    cur.execute("UPDATE products SET images=? WHERE id=?",
                                (json.dumps([hit], ensure_ascii=False), pid))
                updated += 1
                continue

            # 2) strict fuzzy：model 優先
            key1 = normalize(model)
            key2 = normalize(title)

            fn = None
            if key1:
                fn = pick_best_image_for_key(key1 + "_main", candidates_norm, norm_to_filename) or \
                     pick_best_image_for_key(key1, candidates_norm, norm_to_filename)
            if not fn and key2:
                fn = pick_best_image_for_key(key2 + "_main", candidates_norm, norm_to_filename) or \
                     pick_best_image_for_key(key2, candidates_norm, norm_to_filename)

            if fn:
                print(f"[STRICT] id={pid} model={model} title={title} -> {fn}")
                if not DRY_RUN:
                    cur.execute("UPDATE products SET images=? WHERE id=?",
                                (json.dumps([fn], ensure_ascii=False), pid))
                updated += 1
            else:
                missed.append((pid, title, model))

        if not DRY_RUN:
            conn.commit()

        print("\n========== SUMMARY ==========")
        print(f"updated: {updated}")
        print(f"missed: {len(missed)}")
        if missed:
            print("\n[Missed Top 30]")
            for pid, title, model in missed[:30]:
                print(f"- id={pid} title={title} model={model}")
        print("================================\n")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
