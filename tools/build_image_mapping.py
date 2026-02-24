# build_image_mapping.py
# 目的：
# - 掃描 crawled_data/images 下的 *_main.(jpg/png/webp/...)
# - 從 DB 讀 products(id,title,images)
# - 已有 images 就跳過（可用 --force 覆蓋）
# - 對不到的用 fuzzy 候選（topK）產生 mapping_suggest.json
#
# 產出：
# - mapping_suggest.json
# - mapping_final.json（先用 seed + 自動高分命中先填好；你可手動再修）
#
# 用法：
#   python tools/build_image_mapping.py
#   python tools/build_image_mapping.py --db company_data.db --images crawled_data/images --topk 8
#   python tools/build_image_mapping.py --force   (即使已有 images 也產生建議)
#
import os
import re
import json
import argparse
import sqlite3
from difflib import SequenceMatcher
from typing import Dict, List, Any, Tuple, Optional

ALLOWED_EXT = (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tif", ".tiff")


# -----------------------------
# JSON helpers
# -----------------------------
def safe_json_list(v: Any) -> List[str]:
    if not v:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []
        try:
            j = json.loads(s)
            if isinstance(j, list):
                return [str(x).strip() for x in j if str(x).strip()]
        except Exception:
            pass
        return [s]
    return []


def norm_local_filename(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    s = s.replace("\\", "/")
    s = re.sub(r"^crawled_data/images/", "", s, flags=re.IGNORECASE)
    s = s.lstrip("/")
    return s


# -----------------------------
# normalization / scoring
# -----------------------------
STOP_WORDS = {
    "system", "measurement", "measure", "measuring", "meter", "device",
    "optical", "opto", "light", "source",
    "cmos", "image", "sensor", "camera",  # 這些有時是關鍵詞，先不強移除，只不加權
}

def normalize_key(s: str) -> str:
    """
    把 title / filename 轉成可比對 key
    - 大寫
    - 把空白與特殊符號轉底線
    - 連續底線壓縮
    """
    s = (s or "").strip()
    if not s:
        return ""
    s = s.replace("\\", "/")
    s = s.split("/")[-1]  # 只留最後段
    s = s.rsplit(".", 1)[0]  # 去 ext
    s = s.upper()
    s = s.replace("－", "-").replace("—", "-")
    s = re.sub(r"[^\w]+", "_", s, flags=re.UNICODE)  # 非英數底線都變底線
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def tokens(s: str) -> List[str]:
    k = normalize_key(s)
    if not k:
        return []
    toks = [t for t in k.split("_") if t]
    return toks


def ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def token_overlap(a: str, b: str) -> float:
    ta = set(tokens(a))
    tb = set(tokens(b))
    if not ta or not tb:
        return 0.0
    inter = ta & tb
    return len(inter) / max(len(ta), len(tb))


def score_pair(title: str, filename: str) -> float:
    """
    綜合分數：
    - key ratio（0~1）
    - token overlap（0~1）
    - 若出現型號-like token (含數字/連字號) → 小加權
    """
    tkey = normalize_key(title)
    fkey = normalize_key(filename)

    base = ratio(tkey, fkey)
    ov = token_overlap(title, filename)

    # 型號 token 加權
    model_bonus = 0.0
    for tk in tokens(title):
        if any(ch.isdigit() for ch in tk) and (len(tk) >= 3):
            if tk in tokens(filename):
                model_bonus += 0.05

    return (base * 0.65) + (ov * 0.30) + min(model_bonus, 0.15)


# -----------------------------
# seed mapping（你目前已知會 miss 的）
# key 用「產品 title（DB 裡的 title）」為主，值是本機檔名（不含 crawled_data/images/）
# 你之後可以直接改 mapping_final.json，不用動這裡
# -----------------------------
SEED_MAPPING: Dict[str, str] = {
    # 依你資料夾截圖中已存在的檔名
    "積分球 ISP-XXXX": "ISP_XXXX_main.jpg",
    "鍍金積分球 ISP-XXXGL": "ISP_XXXGL_main.jpg",
    "IS治具": "IS_fixture_main.jpg",
    "CMOS影像(感測器)量測系統": "CMOS_main.jpg",
    "廣角鏡頭量測-積分球均勻光源": "integrating_sphere_uniform_light_source_main.jpg",
    "VCSEL量測儀": "VCSEL_main.jpg",
    "光通量(流明)量測系統 LM-ISP-XXXX": "LM_ISP_XXXX_main.jpg",
}


def list_main_images(images_dir: str) -> List[str]:
    imgs: List[str] = []
    if not os.path.isdir(images_dir):
        return imgs
    for fn in os.listdir(images_dir):
        low = fn.lower()
        if not low.endswith(ALLOWED_EXT):
            continue
        # 主圖規則：*_main.*
        if "_main." not in low:
            continue
        imgs.append(fn)
    return sorted(imgs)


def load_products(db_path: str) -> List[Dict[str, Any]]:
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute("SELECT id, title, images FROM products")
        rows = cur.fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "id": r["id"],
            "title": (r["title"] or "").strip(),
            "images": safe_json_list(r["images"]),
        })
    return out


def best_candidates(title: str, main_images: List[str], topk: int) -> List[Dict[str, Any]]:
    scored: List[Tuple[float, str]] = []
    for fn in main_images:
        s = score_pair(title, fn)
        scored.append((s, fn))
    scored.sort(key=lambda x: x[0], reverse=True)
    out = []
    for s, fn in scored[:max(1, topk)]:
        out.append({"filename": fn, "score": round(float(s), 4)})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.getenv("DB_PATH", "company_data.db"))
    ap.add_argument("--images", default=os.getenv("IMAGE_DIR", "crawled_data/images"))
    ap.add_argument("--topk", type=int, default=8)
    ap.add_argument("--auto_threshold", type=float, default=0.92, help="自動直接採用的分數門檻（0~1）")
    ap.add_argument("--force", action="store_true", help="已有 images 也產生建議")
    ap.add_argument("--out_suggest", default="mapping_suggest.json")
    ap.add_argument("--out_final", default="mapping_final.json")
    args = ap.parse_args()

    main_images = list_main_images(args.images)
    if not main_images:
        raise SystemExit(f"找不到 *_main.* 圖片：{args.images}")

    products = load_products(args.db)
    if not products:
        raise SystemExit("DB products 無資料")

    suggest_payload: Dict[str, Any] = {
        "meta": {
            "db": args.db,
            "images_dir": args.images,
            "topk": args.topk,
            "auto_threshold": args.auto_threshold,
        },
        "items": [],
    }

    final_payload: Dict[str, Any] = {
        "meta": {
            "db": args.db,
            "images_dir": args.images,
            "note": "這份檔案是要拿去 apply_image_mapping.py 寫回 DB 的。value 必須是本機檔名（例如 LI_100_main.jpg）",
        },
        "mapping": {},
    }

    # 先塞 seed（若該檔案真的存在才塞）
    seed_ok = 0
    for k, v in SEED_MAPPING.items():
        if v in main_images:
            final_payload["mapping"][k] = v
            seed_ok += 1

    auto_ok = 0
    need_review = 0
    skipped_has_images = 0

    for p in products:
        pid = p["id"]
        title = p["title"]
        existing = [norm_local_filename(x) for x in (p["images"] or []) if norm_local_filename(x)]

        if (not args.force) and existing:
            skipped_has_images += 1
            continue

        # seed 已經填了就跳過（但仍可放到 suggest 讓你知道候選）
        candidates = best_candidates(title, main_images, args.topk)

        chosen = None
        if candidates and candidates[0]["score"] >= args.auto_threshold:
            chosen = candidates[0]["filename"]

        item = {
            "id": pid,
            "title": title,
            "existing_images": existing,
            "seed": final_payload["mapping"].get(title, ""),
            "auto_pick": chosen or "",
            "candidates": candidates,
        }
        suggest_payload["items"].append(item)

        # 若 seed 沒有，且 auto_pick 有 → 直接先寫入 final（你可再改）
        if title not in final_payload["mapping"]:
            if chosen:
                final_payload["mapping"][title] = chosen
                auto_ok += 1
            else:
                need_review += 1

    with open(args.out_suggest, "w", encoding="utf-8") as f:
        json.dump(suggest_payload, f, ensure_ascii=False, indent=2)

    with open(args.out_final, "w", encoding="utf-8") as f:
        json.dump(final_payload, f, ensure_ascii=False, indent=2)

    print("✅ build_image_mapping 完成")
    print(f" - images(main): {len(main_images)}")
    print(f" - products: {len(products)}")
    print(f" - skipped(has images & not --force): {skipped_has_images}")
    print(f" - seed filled: {seed_ok}")
    print(f" - auto filled (>= {args.auto_threshold}): {auto_ok}")
    print(f" - need manual review: {need_review}")
    print(f" - wrote: {args.out_suggest}")
    print(f" - wrote: {args.out_final}")
    print("\n下一步：打開 mapping_final.json，把 need_review 的那些 title 手動指定檔名，然後跑 apply_image_mapping.py")


if __name__ == "__main__":
    main()
