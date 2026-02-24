# fix_images_to_main.py
# 功能：掃描 crawled_data/images 內的 *_main.jpg，依「型號/關鍵字/alias/fuzzy」回填到 SQLite 的 products.images (JSON list)
# 特色：自動備份 DB、只改 images 欄位、不碰 image 欄位
#
# 用法：
#   python fix_images_to_main.py

import os
import re
import json
import shutil
import sqlite3
from pathlib import Path
from datetime import datetime
from difflib import SequenceMatcher
from typing import Dict, List, Tuple, Optional

DB_PATH = os.getenv("DB_PATH", "company_data.db")
IMAGE_DIR = Path(os.getenv("IMAGE_DIR", r".\crawled_data\images"))
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"  # 1=只顯示不寫入

# 你列的 miss（人工 mapping，確保一次補齊）
MANUAL_MAPPING: Dict[str, str] = {
    "積分球 ISP-XXXX": "ISP_XXXX_main.jpg",
    "鍍金積分球 ISP-XXXGL": "ISP_XXXGL_main.jpg",
    "IS治具": "IS_fixture_main.jpg",
    "光通量(流明)量測系統 LM-ISP-XXXX": "LM_ISP_XXXX_main.jpg",
    "CMOS影像(感測器)量測系統": "CMOS_main.jpg",
    "廣角鏡頭量測-積分球均勻光源": "integrating_sphere_uniform_light_source_main.jpg",
    "VCSEL量測儀": "VCSEL_main.jpg",
}

ALIAS_MAP: Dict[str, List[str]] = {
    "isp-xxxx": ["isp_xxxx", "integrating_sphere", "lm_isp_xxxx"],
    "isp-xxxgl": ["isp_xxxgl", "integrating_sphere"],
    "lm-isp-xxxx": ["lm_isp_xxxx", "isp_xxxx"],
    "is治具": ["is_fixture", "fixture_is", "is_jig"],
    "cmos": ["cmos", "sensor", "imager"],
    "vcsel": ["vcsel"],
    "廣角鏡頭": ["wide_angle", "wideangle", "lens"],
    "均勻光源": ["uniform_light_source", "integrating_sphere_uniform_light_source"],
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
        # 舊格式：['x.jpg']
        s = str(s).strip()
        if s.startswith("[") and s.endswith("]") and "'" in s and '"' not in s:
            try:
                v2 = json.loads(s.replace("'", '"'))
                return v2 if isinstance(v2, list) else []
            except Exception:
                return []
        return []

def normalize_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("（", "(").replace("）", ")")
    s = re.sub(r"[\s/]+", " ", s)
    s = s.replace("-", "_")
    s = re.sub(r"[^a-z0-9\u4e00-\u9fff_() ]+", "", s)
    return s

def tokens_from_text(s: str) -> List[str]:
    s = normalize_text(s)
    parts = re.split(r"[ _()]+", s)
    return [p for p in parts if p]

def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()

def load_products(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("SELECT id, COALESCE(title,''), COALESCE(model,''), COALESCE(images,'') FROM products")
    out = []
    for pid, title, model, images in cur.fetchall():
        out.append({
            "id": pid,
            "title": title,
            "model": model,
            "images": safe_json_list(images)
        })
    return out

def collect_main_images(image_dir: Path):
    items = []
    for p in image_dir.glob("*_main.jpg"):
        stem_norm = normalize_text(p.stem)
        items.append({
            "filename": p.name,
            "stem_norm": stem_norm,
            "tokens": tokens_from_text(stem_norm),
        })
    return items

def apply_manual_mapping(products, image_set: set) -> Dict[int, str]:
    result = {}
    for pr in products:
        for k, fn in MANUAL_MAPPING.items():
            if k and (k in pr["title"] or k in pr["model"]):
                if fn in image_set:
                    result[pr["id"]] = fn
    return result

def best_match_for_product(pr, images) -> Tuple[Optional[str], float]:
    base_text = f"{pr['title']} {pr['model']}"
    base_norm = normalize_text(base_text)
    prod_tokens = set(tokens_from_text(base_norm))

    for key, alias_tokens in ALIAS_MAP.items():
        if key in base_norm:
            for t in alias_tokens:
                prod_tokens.update(tokens_from_text(t))

    if not prod_tokens:
        return None, 0.0

    best_fn = None
    best_score = 0.0

    for im in images:
        hits = sum(1 for t in im["tokens"] if t in prod_tokens)
        token_score = hits / max(1, len(im["tokens"]))
        sim_score = max(similarity(base_norm, im["stem_norm"]),
                        similarity(normalize_text(pr["model"]), im["stem_norm"]) if pr["model"] else 0.0)
        score = 0.65 * token_score + 0.35 * sim_score
        if score > best_score:
            best_score = score
            best_fn = im["filename"]

    return best_fn, best_score

def update_product_image(conn: sqlite3.Connection, pid: int, filename: str):
    cur = conn.cursor()
    cur.execute("UPDATE products SET images=? WHERE id=?",
                (json.dumps([filename], ensure_ascii=False), pid))

def main():
    print(f"[INFO] DB_PATH: {DB_PATH}")
    print(f"[INFO] IMAGE_DIR: {IMAGE_DIR}")
    print(f"[INFO] DRY_RUN: {DRY_RUN}")

    backup_path = backup_db(DB_PATH)
    print(f"[OK] DB backup created: {backup_path}")

    conn = sqlite3.connect(DB_PATH)
    try:
        products = load_products(conn)
        main_images = collect_main_images(IMAGE_DIR)
        image_set = {im["filename"] for im in main_images}

        print(f"[INFO] products: {len(products)}")
        print(f"[INFO] found main images: {len(main_images)}")

        manual_hits = apply_manual_mapping(products, image_set)

        updated = 0
        skipped_has_value = 0
        missed = []

        # 先人工 mapping
        for pr in products:
            if pr["images"]:
                skipped_has_value += 1
                continue
            if pr["id"] in manual_hits:
                fn = manual_hits[pr["id"]]
                print(f"[MANUAL] id={pr['id']} title={pr['title']} -> {fn}")
                if not DRY_RUN:
                    update_product_image(conn, pr["id"], fn)
                updated += 1

        # 再自動匹配
        for pr in products:
            if pr["images"] or pr["id"] in manual_hits:
                continue

            fn, score = best_match_for_product(pr, main_images)
            if fn and score >= 0.58:
                print(f"[AUTO] id={pr['id']} score={score:.3f} title={pr['title']} model={pr['model']} -> {fn}")
                if not DRY_RUN:
                    update_product_image(conn, pr["id"], fn)
                updated += 1
            else:
                missed.append((pr["id"], pr["title"], pr["model"], score, fn))

        if not DRY_RUN:
            conn.commit()

        print("\n========== SUMMARY ==========")
        print(f"skipped(has images already): {skipped_has_value}")
        print(f"updated: {updated}")
        print(f"missed: {len(missed)}")
        if missed:
            print("\n[Missed Top 30]")
            for pid, title, model, score, fn in missed[:30]:
                print(f"- id={pid} score={score:.3f} best={fn} title={title} model={model}")
        print("================================\n")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
