import os
import re
import json
import csv
import hashlib
import sqlite3
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional, Set, Tuple

import requests

logger = logging.getLogger("image-cache")

# -----------------------------
# Config (env overridable)
# -----------------------------
IMAGE_CACHE_DIR = os.getenv("IMAGE_CACHE_DIR", "crawled_data/images")
IMAGE_DOWNLOAD_TIMEOUT = int(os.getenv("IMAGE_DOWNLOAD_TIMEOUT", "15"))
IMAGE_MAX_BYTES = int(os.getenv("IMAGE_MAX_BYTES", str(15 * 1024 * 1024)))  # 15MB
IMAGE_USER_AGENT = os.getenv(
    "IMAGE_USER_AGENT",
    "Mozilla/5.0 (compatible; OptimumQAImageCache/1.0)"
)

# 下載失敗是否保留原 URL（預設不保留，避免前端拿 URL 當本機路徑）
KEEP_ORIGINAL_URL_ON_FAIL = os.getenv("KEEP_ORIGINAL_URL_ON_FAIL", "0") == "1"
# 已成功快取過就跳過（預設開啟）
SKIP_ALREADY_CACHED_OK = os.getenv("SKIP_ALREADY_CACHED_OK", "1") == "1"


# -----------------------------
# Utilities
# -----------------------------
def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def is_http_url(s: str) -> bool:
    return isinstance(s, str) and (s.startswith("http://") or s.startswith("https://"))


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _guess_ext_from_url(url: str) -> str:
    try:
        path = url.split("?", 1)[0].split("#", 1)[0]
        _, ext = os.path.splitext(path)
        ext = (ext or "").lower()
        if ext in [".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tif", ".tiff"]:
            return ext
    except Exception:
        pass
    return ""


def _guess_ext_from_content_type(ct: str) -> str:
    ct = (ct or "").lower().split(";")[0].strip()
    mapping = {
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "image/gif": ".gif",
        "image/bmp": ".bmp",
        "image/tiff": ".tif",
    }
    return mapping.get(ct, "")


def _safe_json_list(v: Any) -> List[str]:
    """
    products.images 可能是：
    - JSON list 字串: '["a.jpg","b.jpg"]'
    - 單一網址/檔名字串
    - None
    """
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


def _normalize_local_filename(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip()
    if not s:
        return ""
    s = s.replace("\\", "/")
    s = s.replace("crawled_data/images/", "")
    s = s.lstrip("/")
    return s


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


# -----------------------------
# SQLite image_cache table
# -----------------------------
def ensure_image_cache_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS image_cache (
            url TEXT PRIMARY KEY,
            filename TEXT,
            status TEXT,           -- ok / failed
            http_status INTEGER,
            content_type TEXT,
            size_bytes INTEGER,
            etag TEXT,
            last_modified TEXT,
            error TEXT,
            updated_at TEXT
        )
        """
    )
    conn.commit()


def upsert_cache_row(
    conn: sqlite3.Connection,
    url: str,
    filename: Optional[str],
    status: str,
    http_status: Optional[int],
    content_type: Optional[str],
    size_bytes: Optional[int],
    etag: Optional[str],
    last_modified: Optional[str],
    error: Optional[str],
) -> None:
    conn.execute(
        """
        INSERT INTO image_cache(url, filename, status, http_status, content_type, size_bytes, etag, last_modified, error, updated_at)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(url) DO UPDATE SET
            filename=excluded.filename,
            status=excluded.status,
            http_status=excluded.http_status,
            content_type=excluded.content_type,
            size_bytes=excluded.size_bytes,
            etag=excluded.etag,
            last_modified=excluded.last_modified,
            error=excluded.error,
            updated_at=excluded.updated_at
        """,
        (
            url,
            filename or "",
            status,
            int(http_status) if http_status is not None else None,
            content_type or "",
            int(size_bytes) if size_bytes is not None else None,
            etag or "",
            last_modified or "",
            (error or "")[:500],
            _now_iso(),
        ),
    )
    conn.commit()


def get_cached_filename_if_ok(conn: sqlite3.Connection, url: str) -> Optional[str]:
    cur = conn.execute("SELECT filename, status FROM image_cache WHERE url = ?", (url,))
    row = cur.fetchone()
    if not row:
        return None
    filename, status = (row[0] or "").strip(), row[1]
    if status == "ok" and filename:
        fpath = os.path.join(IMAGE_CACHE_DIR, filename)
        if os.path.exists(fpath) and os.path.getsize(fpath) > 0:
            return filename
    return None


# -----------------------------
# Core downloader
# -----------------------------
def download_image_if_url(conn: sqlite3.Connection, url: str) -> Optional[str]:
    """
    - 若 url 不是 http(s)，視為本機檔名 → normalize 後回傳
    - 若是 http(s)，會：
        * 查 image_cache (ok) → 直接回傳 filename
        * 否則下載到 crawled_data/images/<sha1>.<ext>
        * 成功寫入 image_cache
    """
    url = (url or "").strip()
    if not is_http_url(url):
        return _normalize_local_filename(url) or None

    _ensure_dir(IMAGE_CACHE_DIR)
    ensure_image_cache_table(conn)

    if SKIP_ALREADY_CACHED_OK:
        cached = get_cached_filename_if_ok(conn, url)
        if cached:
            return cached

    base = _sha1(url)
    ext = _guess_ext_from_url(url)
    filename = base + (ext if ext else "")

    headers = {"User-Agent": IMAGE_USER_AGENT}

    try:
        with requests.get(url, headers=headers, stream=True, timeout=IMAGE_DOWNLOAD_TIMEOUT) as resp:
            http_status = resp.status_code
            if http_status != 200:
                upsert_cache_row(
                    conn, url, None, "failed", http_status, None, None,
                    resp.headers.get("ETag"), resp.headers.get("Last-Modified"),
                    f"HTTP {http_status}"
                )
                return None

            content_type = resp.headers.get("Content-Type", "")
            if not ext:
                ext = _guess_ext_from_content_type(content_type) or ".jpg"
                filename = base + ext

            tmp_path = os.path.join(IMAGE_CACHE_DIR, filename + ".tmp")
            final_path = os.path.join(IMAGE_CACHE_DIR, filename)

            # 已存在就直接記錄 ok 並回傳
            if os.path.exists(final_path) and os.path.getsize(final_path) > 0:
                upsert_cache_row(
                    conn, url, filename, "ok", http_status, content_type, os.path.getsize(final_path),
                    resp.headers.get("ETag"), resp.headers.get("Last-Modified"),
                    ""
                )
                return filename

            size = 0
            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=64 * 1024):
                    if not chunk:
                        continue
                    size += len(chunk)
                    if size > IMAGE_MAX_BYTES:
                        raise ValueError(f"Image too large: > {IMAGE_MAX_BYTES} bytes")
                    f.write(chunk)

            if size <= 0:
                raise ValueError("Downloaded empty file")

            os.replace(tmp_path, final_path)

            upsert_cache_row(
                conn, url, filename, "ok", http_status, content_type, size,
                resp.headers.get("ETag"), resp.headers.get("Last-Modified"),
                ""
            )
            return filename

    except Exception as e:
        # 清掉殘留 tmp
        try:
            tmp_path = os.path.join(IMAGE_CACHE_DIR, base + (ext if ext else "") + ".tmp")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass

        upsert_cache_row(conn, url, None, "failed", None, None, None, None, None, str(e))
        logger.warning(f"download failed: {url} err={e}")
        return None


# -----------------------------
# Apply cache to DB (only products.images)
# -----------------------------
@dataclass
class CacheRunStats:
    total_rows: int = 0
    updated_rows: int = 0
    downloaded_ok: int = 0
    downloaded_failed: int = 0
    images_removed: int = 0
    started_at: str = ""
    finished_at: str = ""


def cache_product_images_in_db(
    db_path: str,
    products_table: str = "products",
    col_id: str = "id",
    col_images: str = "images",
    images_dir: str = IMAGE_CACHE_DIR,  # ✅ 新增：可由外部指定快取資料夾（相容 app.py 可能傳 images_dir）
) -> CacheRunStats:
    """
    你的 DB 結構：
    products(id, title, url, category, description, specifications, images, created_at)

    - 僅處理 products.images（TEXT）
    - 若含 URL：下載→改成本機檔名
    - 若下載失敗：移除該 URL（或保留，看 env KEEP_ORIGINAL_URL_ON_FAIL）
    - 寫回 JSON list 字串
    """
    # ✅ 讓整個流程使用指定 images_dir（兼容舊程式用全域 IMAGE_CACHE_DIR）
    global IMAGE_CACHE_DIR
    old_dir = IMAGE_CACHE_DIR
    if images_dir:
        IMAGE_CACHE_DIR = images_dir

    stats = CacheRunStats(started_at=_now_iso())

    try:
        with sqlite3.connect(db_path, timeout=30) as conn:
            conn.row_factory = sqlite3.Row
            ensure_image_cache_table(conn)

            # ✅ 不用 rowid，直接用 id（修正你遇到的 No item with that key）
            cur = conn.execute(f"SELECT {col_id} as pid, {col_images} as images FROM {products_table}")
            rows = cur.fetchall()
            stats.total_rows = len(rows)

            for r in rows:
                pid = r["pid"]
                images_raw = r["images"]
                images_list = _safe_json_list(images_raw)

                new_list: List[str] = []
                changed = False

                for it in images_list:
                    s = (it or "").strip()
                    if not s:
                        changed = True
                        continue

                    if is_http_url(s):
                        fname = download_image_if_url(conn, s)
                        if fname:
                            new_list.append(fname)
                            stats.downloaded_ok += 1
                        else:
                            stats.downloaded_failed += 1
                            if KEEP_ORIGINAL_URL_ON_FAIL:
                                new_list.append(s)
                            else:
                                stats.images_removed += 1
                        changed = True
                    else:
                        norm = _normalize_local_filename(s)
                        new_list.append(norm)
                        if norm != s:
                            changed = True

                # 若原本不是 JSON list（例如單一字串），寫回 JSON list 視為 changed
                if isinstance(images_raw, str):
                    try:
                        j = json.loads(images_raw.strip())
                        if not isinstance(j, list):
                            changed = True
                    except Exception:
                        if images_raw.strip():
                            changed = True

                if changed:
                    conn.execute(
                        f"UPDATE {products_table} SET {col_images}=? WHERE {col_id}=?",
                        (json.dumps(new_list, ensure_ascii=False), pid),
                    )
                    stats.updated_rows += 1

            conn.commit()

        stats.finished_at = _now_iso()
        return stats

    finally:
        # ✅ 還原全域（避免其他流程意外吃到不同 dir）
        IMAGE_CACHE_DIR = old_dir


# -----------------------------
# GC report
# -----------------------------
def collect_referenced_filenames_from_db(
    db_path: str,
    products_table: str = "products",
    col_images: str = "images",
) -> Set[str]:
    refs: Set[str] = set()
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(f"SELECT {col_images} as images FROM {products_table}")
        for r in cur.fetchall():
            imgs = _safe_json_list(r["images"])
            for it in imgs:
                if isinstance(it, str):
                    s = _normalize_local_filename(it.strip())
                    if s and not is_http_url(s):
                        refs.add(s)
    return refs


def collect_referenced_filenames_from_markdown_dirs(dirs: List[str]) -> Set[str]:
    refs: Set[str] = set()
    pat = re.compile(r'([a-zA-Z0-9_\-./]+?\.(?:png|jpg|jpeg|webp|gif|bmp|tif|tiff))', re.IGNORECASE)

    for d in dirs:
        if not d or not os.path.isdir(d):
            continue
        for root, _, files in os.walk(d):
            for fn in files:
                if not fn.lower().endswith((".md", ".txt")):
                    continue
                path = os.path.join(root, fn)
                try:
                    with open(path, "r", encoding="utf-8", errors="ignore") as f:
                        text = f.read()
                    for m in pat.findall(text):
                        s = _normalize_local_filename(m)
                        if s and not is_http_url(s):
                            refs.add(s)
                except Exception:
                    continue
    return refs


def generate_unused_images_report(
    db_path: str,
    images_dir: str = IMAGE_CACHE_DIR,
    extra_scan_dirs: Optional[List[str]] = None,
    report_dir: str = IMAGE_CACHE_DIR,
) -> Tuple[str, str]:
    """
    產出：
    - crawled_data/images/_gc_report_YYYYmmdd_HHMMSS.json
    - crawled_data/images/_gc_report_YYYYmmdd_HHMMSS.csv
    """
    _ensure_dir(report_dir)

    referenced = set()
    referenced |= collect_referenced_filenames_from_db(db_path)

    if extra_scan_dirs:
        referenced |= collect_referenced_filenames_from_markdown_dirs(extra_scan_dirs)

    all_files: List[str] = []
    if os.path.isdir(images_dir):
        for fn in os.listdir(images_dir):
            if fn.startswith("_gc_report_") or fn.endswith(".tmp"):
                continue
            fpath = os.path.join(images_dir, fn)
            if os.path.isfile(fpath):
                all_files.append(fn)

    unused = sorted([fn for fn in all_files if fn not in referenced])

    payload = {
        "generated_at": _now_iso(),
        "images_dir": images_dir,
        "total_files": len(all_files),
        "referenced_files": len(referenced),
        "unused_files": len(unused),
        "unused_list": unused,
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = os.path.join(report_dir, f"_gc_report_{ts}.json")
    csv_path = os.path.join(report_dir, f"_gc_report_{ts}.csv")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["filename"])
        for fn in unused:
            w.writerow([fn])

    return json_path, csv_path
