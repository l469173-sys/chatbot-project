# app.py (智慧推薦版 Ultimate + Anti-Hallucination Allowlist for Decision + Auto Image + Hit-rate Booster
#        + ✅ BM25 Fallback for product_structured 命中率大幅提升
#        + ✅ 融合推薦(RRF) 產生「幾乎不為空」的動態 Allowlist
#        + ✅ BM25 納入 system_docs/ + company_info.md（當向量庫片段不足時補位）
#
# ✅ 本版已套用你指定的 1~3 項優化：
# 1) /api/admin/log-tail 支援 ?rid= 後端過濾
# 2) Session JSON：atomic write + per-session lock（避免多人同時寫壞檔/互蓋）
# 3) SQLite：加 PRAGMA（WAL / cache / temp_store）提升讀取效能與減少鎖競爭

import os
import re
import json
import time
import uuid
import sqlite3
import logging
import threading
import math
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory, abort
from flask_cors import CORS
from werkzeug.utils import safe_join, secure_filename

from vector_db import VectorDatabase
import decision  # decision.py

os.environ["TOKENIZERS_PARALLELISM"] = "false"
load_dotenv()

# -----------------------------
# Logging (console + file)
# -----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_DIR = os.getenv("LOG_DIR", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, os.getenv("LOG_FILE", "app.log"))

logger = logging.getLogger("company-qa")
logger.setLevel(LOG_LEVEL)

_fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

if not logger.handlers:
    _ch = logging.StreamHandler()
    _ch.setLevel(LOG_LEVEL)
    _ch.setFormatter(_fmt)
    logger.addHandler(_ch)

    _fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    _fh.setLevel(LOG_LEVEL)
    _fh.setFormatter(_fmt)
    logger.addHandler(_fh)


class Config:
    DEBUG = os.getenv("API_DEBUG", "0") == "1"
    PORT = int(os.getenv("PORT", "5001"))
    HOST = os.getenv("HOST", "0.0.0.0")

    PROMPT_VERSION = os.getenv("PROMPT_VERSION", "2026-02-20-bm25-fallback+fusion+systemdocs")

    DB_PATH = os.getenv("DB_PATH", "company_data.db")

    # Ollama
    OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434").rstrip("/")
    OLLAMA_MODEL_INTENT = os.getenv("OLLAMA_MODEL_INTENT", os.getenv("OLLAMA_MODEL", "qwen2.5:7b"))
    OLLAMA_MODEL_ANSWER = os.getenv("OLLAMA_MODEL_ANSWER", os.getenv("OLLAMA_MODEL", "qwen2.5:14b"))
    OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "180"))
    OLLAMA_KEEP_ALIVE = os.getenv("OLLAMA_KEEP_ALIVE", "5m")
    OLLAMA_MAX_CONCURRENT = int(os.getenv("OLLAMA_MAX_CONCURRENT", "2"))
    OLLAMA_QUEUE_TIMEOUT = float(os.getenv("OLLAMA_QUEUE_TIMEOUT", "6.0"))

    # Static & images
    STATIC_DIR = os.getenv("STATIC_DIR", "static")
    INDEX_FILE = os.getenv("INDEX_FILE", "index.html")
    ADMIN_FILE = os.getenv("ADMIN_FILE", "admin.html")
    IMAGE_DIR = os.getenv("IMAGE_CACHE_DIR", "crawled_data/images")

    # Structured product md
    PRODUCT_STRUCTURED_DIR = os.getenv("PRODUCT_STRUCTURED_DIR", "data/product_structured")
    SYSTEM_DOCS_DIR = os.getenv("SYSTEM_DOCS_DIR", "data/system_docs")
    COMPANY_INFO_PATH = os.getenv("COMPANY_INFO_PATH", "data/company_info.md")
    COMPANY_CARD_IMAGE = os.getenv("COMPANY_CARD_IMAGE", "")

    # Alias
    ALIAS_PATH = os.getenv("ALIAS_PATH", "data/product_alias.json")

    # RAG
    COLLECTION_NAME = os.getenv("CHROMA_COLLECTION", "company_knowledge")
    TOPK_NORMAL = int(os.getenv("TOPK_NORMAL", "6"))
    TOPK_FAST = int(os.getenv("TOPK_FAST", "3"))
    TOPK_CARD = int(os.getenv("TOPK_CARD", "8"))

    RAG_CHUNK_MAX_CHARS = int(os.getenv("RAG_CHUNK_MAX_CHARS", "900"))
    RAG_MAX_BLOCKS = int(os.getenv("RAG_MAX_BLOCKS", "6"))
    RAG_MAX_BLOCKS_WHEN_MD = int(os.getenv("RAG_MAX_BLOCKS_WHEN_MD", "3"))
    RAG_PER_SOURCE_MAX = int(os.getenv("RAG_PER_SOURCE_MAX", "2"))
    RAG_DEDUPE_TEXT = os.getenv("RAG_DEDUPE_TEXT", "1") == "1"

    # Prompt guard
    MAX_PROMPT_CHARS = int(os.getenv("MAX_PROMPT_CHARS", "14000"))

    # Sessions
    SESSIONS_DIR = os.getenv("SESSIONS_DIR", "data/sessions")
    SESSION_MAX_TURNS = int(os.getenv("SESSION_MAX_TURNS", "10"))
    DEFAULT_ANSWER_MODE = os.getenv("DEFAULT_ANSWER_MODE", "NORMAL").upper()

    # Debounce
    MIN_REQUEST_INTERVAL_MS = int(os.getenv("MIN_REQUEST_INTERVAL_MS", "650"))

    # Cache
    ENABLE_SESSION_CACHE = os.getenv("ENABLE_SESSION_CACHE", "1") == "1"

    # Unanswerable
    APPEND_CONTACT_ON_UNANSWERABLE = os.getenv("APPEND_CONTACT_ON_UNANSWERABLE", "1") == "1"

    # Admin / Reload Token
    ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()
    RELOAD_TOKEN = os.getenv("RELOAD_TOKEN", "").strip()
    MAX_UPLOAD_MB = int(os.getenv("MAX_UPLOAD_MB", "8"))

    # Admin log tail
    ADMIN_LOG_TAIL_DEFAULT = int(os.getenv("ADMIN_LOG_TAIL_DEFAULT", "250"))
    ADMIN_LOG_TAIL_MAX = int(os.getenv("ADMIN_LOG_TAIL_MAX", "2000"))

    # ✅ Anti-hallucination for decision/recommend
    STRICT_DECISION_ALLOWLIST = os.getenv("STRICT_DECISION_ALLOWLIST", "1") == "1"
    ALLOWLIST_MAX_ITEMS = int(os.getenv("ALLOWLIST_MAX_ITEMS", "24"))

    # ✅ BM25 fallback tuning (product_structured)
    BM25_ENABLED = os.getenv("BM25_ENABLED", "1") == "1"
    BM25_K1 = float(os.getenv("BM25_K1", "1.5"))
    BM25_B = float(os.getenv("BM25_B", "0.75"))
    BM25_TEXT_MAX_CHARS = int(os.getenv("BM25_TEXT_MAX_CHARS", "3500"))
    BM25_TOPN = int(os.getenv("BM25_TOPN", "8"))

    # ✅ BM25 for system_docs + company_info.md
    SYSTEM_BM25_ENABLED = os.getenv("SYSTEM_BM25_ENABLED", "1") == "1"
    SYSTEM_BM25_TEXT_MAX_CHARS = int(os.getenv("SYSTEM_BM25_TEXT_MAX_CHARS", "6500"))
    SYSTEM_BM25_TOPN = int(os.getenv("SYSTEM_BM25_TOPN", "4"))

    # ✅ Fusion (RRF) allowlist booster
    FUSION_ENABLED = os.getenv("FUSION_ENABLED", "1") == "1"
    FUSION_RRF_K = int(os.getenv("FUSION_RRF_K", "60"))
    FUSION_VEC_TOPN = int(os.getenv("FUSION_VEC_TOPN", "10"))
    FUSION_BM25_TOPN = int(os.getenv("FUSION_BM25_TOPN", "10"))
    FUSION_FORCE_NONEMPTY = os.getenv("FUSION_FORCE_NONEMPTY", "1") == "1"
    # when everything weak -> allow full set (low confidence)
    FUSION_LOWCONF_ALLOW_ALL = os.getenv("FUSION_LOWCONF_ALLOW_ALL", "1") == "1"
    FUSION_LOWCONF_MIN_ALLOW = int(os.getenv("FUSION_LOWCONF_MIN_ALLOW", "12"))


# -----------------------------
# Global locks & sessions
# -----------------------------
REBUILD_LOCK = threading.Lock()

CANCEL_EVENTS: Dict[str, threading.Event] = {}
CANCEL_LOCK = threading.Lock()

OLLAMA_SEM = threading.Semaphore(max(1, Config.OLLAMA_MAX_CONCURRENT))

HTTP = requests.Session()
HTTP.headers.update({"User-Agent": "company-qa/1.0"})

# ✅ (2) Session atomic write + per-session lock（避免多人同時寫壞 session json）
SESSION_LOCKS: Dict[str, threading.Lock] = {}
SESSION_LOCKS_GUARD = threading.Lock()


def _get_session_lock(session_id: str) -> threading.Lock:
    sid = (session_id or "default").strip() or "default"
    with SESSION_LOCKS_GUARD:
        if sid not in SESSION_LOCKS:
            SESSION_LOCKS[sid] = threading.Lock()
        return SESSION_LOCKS[sid]


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def read_text_file(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()


def safe_json_loads(s: str, default: Any) -> Any:
    try:
        return json.loads(s)
    except Exception:
        return default


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


def is_http_url(s: str) -> bool:
    return isinstance(s, str) and (s.startswith("http://") or s.startswith("https://"))


def normalize_filename(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip()
    if not s:
        return ""
    s = s.replace("\\", "/")
    s = s.replace("crawled_data/images/", "")
    s = s.lstrip("/")
    return s


def image_to_web_path(x: str) -> str:
    if not x:
        return ""
    s = str(x).strip()
    if not s:
        return ""
    if is_http_url(s):
        return s
    if s.startswith("/images/"):
        return s
    fn = normalize_filename(s)
    parts = [requests.utils.quote(p) for p in fn.split("/")]
    return "/images/" + "/".join(parts)


def clean_placeholder(text: str) -> str:
    if text is None:
        return ""
    t = str(text).strip()
    if not t:
        return ""
    bad = {"(資料中未提供)", "資料中未提供", "未提供", "N/A", "NA", "null", "None"}
    if t in bad:
        return ""
    if len(t) <= 24 and ("未提供" in t or "資料中" in t):
        return ""
    return t


def truncate_text(s: str, max_chars: int) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    if len(s) <= max_chars:
        return s
    return s[: max_chars - 1].rstrip() + "…"


def normalize_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]", "", s)
    return s


def normalize_question_for_cache(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


# -----------------------------
# Token checks
# -----------------------------
def _extract_token() -> str:
    tok = (
        request.headers.get("X-Reload-Token")
        or request.headers.get("X-Admin-Token")
        or request.args.get("token")
        or (request.get_json(silent=True) or {}).get("token")
        or ""
    )
    return (tok or "").strip()


def require_reload_token() -> None:
    if not Config.RELOAD_TOKEN:
        return
    tok = _extract_token()
    if tok != Config.RELOAD_TOKEN:
        abort(401)


def require_admin_or_reload() -> None:
    if not Config.ADMIN_TOKEN and not Config.RELOAD_TOKEN:
        return
    tok = _extract_token()
    if Config.ADMIN_TOKEN and tok == Config.ADMIN_TOKEN:
        return
    if Config.RELOAD_TOKEN and tok == Config.RELOAD_TOKEN:
        return
    abort(401)


# -----------------------------
# Alias
# -----------------------------
def load_alias_map() -> Dict[str, List[str]]:
    p = Config.ALIAS_PATH
    if not p or (not os.path.isfile(p)):
        return {}
    try:
        data = json.load(open(p, "r", encoding="utf-8"))
        out: Dict[str, List[str]] = {}
        if isinstance(data, dict):
            for k, v in data.items():
                key = normalize_key(k)
                arr: List[str] = []
                if isinstance(v, list):
                    arr = [str(x).strip() for x in v if str(x).strip()]
                elif isinstance(v, str):
                    arr = [v.strip()]
                if key:
                    out[key] = arr
        return out
    except Exception as e:
        logger.warning(f"load_alias_map failed: {e}")
        return {}


ALIAS_MAP: Dict[str, List[str]] = load_alias_map()


def expand_by_alias(user_text: str) -> List[str]:
    q = (user_text or "").strip()
    if not q:
        return []
    nq = normalize_key(q)
    terms = {q}

    for base, arr in ALIAS_MAP.items():
        if base and base in nq:
            terms.add(base)
            for a in arr:
                terms.add(a)

    for base, arr in ALIAS_MAP.items():
        for a in arr:
            if a and (a in q):
                terms.add(base)
                terms.add(a)

    out = [t for t in terms if t]
    return out[:12]


# -----------------------------
# Company info loader + cache
# -----------------------------
_company_info_cache: Dict[str, Any] = {"mtime": None, "text": "", "parsed": {}}


def _normalize_phone(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    s = re.sub(r"[^\d\-\+\(\)\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    m = re.search(r"\b(0\d)\s*(\d{3})\s*(\d{4})\b", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}-{m.group(4)}"
    s = s.replace(" ", "-")
    s = re.sub(r"-{2,}", "-", s)
    return s


def _normalize_hours(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    s = re.sub(r"\r\n|\r", "\n", s)
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s).strip()
    return s


def _normalize_address(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _parse_company_info(text: str) -> Dict[str, Any]:
    raw = (text or "").strip()
    parsed: Dict[str, Any] = {"raw": raw}

    m = re.search(r"(公司名稱|名稱|Company\s*Name)\s*[:：]\s*(.+)", raw, re.IGNORECASE)
    if m:
        parsed["name"] = m.group(2).strip()

    m = re.search(r"(地址|公司地址|Address)\s*[:：]\s*(.+)", raw, re.IGNORECASE)
    if m:
        parsed["address"] = _normalize_address(m.group(2).strip())

    m = re.search(r"(電話|電話號碼|聯絡電話|Tel|Phone)\s*[:：]\s*(.+)", raw, re.IGNORECASE)
    if m:
        parsed["phone"] = _normalize_phone(m.group(2).strip())

    m = re.search(r"(Email|E-mail|信箱|電子郵件)\s*[:：]\s*(.+)", raw, re.IGNORECASE)
    if m:
        parsed["email"] = m.group(2).strip()

    m = re.search(r"(官網|網站|Website|URL)\s*[:：]\s*(.+)", raw, re.IGNORECASE)
    if m:
        parsed["website"] = m.group(2).strip()

    m = re.search(r"(營業時間|Opening\s*Hours)\s*[:：]?\s*\n([\s\S]{0,800})", raw, re.IGNORECASE)
    if m:
        block = m.group(2)
        block = re.split(r"\n\s*\n|^#{1,6}\s+", block, maxsplit=1, flags=re.MULTILINE)[0].strip()
        if block:
            parsed["hours"] = _normalize_hours(block)

    return parsed


def load_company_info(force: bool = False) -> Tuple[str, Dict[str, Any]]:
    path = Config.COMPANY_INFO_PATH
    if not path:
        return "", {}
    if not os.path.isfile(path):
        _company_info_cache.update({"mtime": None, "text": "", "parsed": {}})
        return "", {}

    try:
        mtime = os.path.getmtime(path)
    except Exception:
        mtime = None

    if (not force) and _company_info_cache.get("mtime") == mtime and _company_info_cache.get("text"):
        return _company_info_cache["text"], _company_info_cache.get("parsed") or {}

    try:
        text = read_text_file(path).strip()
        parsed = _parse_company_info(text)
        _company_info_cache.update({"mtime": mtime, "text": text, "parsed": parsed})
        return text, parsed
    except Exception as e:
        logger.warning(f"load_company_info failed: {e}")
        return "", {}


def build_company_info_context(user_text: str) -> List[str]:
    text, parsed = load_company_info(force=False)
    if not text:
        return ["（系統尚未建立或找不到 data/company_info.md，公司資料未提供。）"]

    q = (user_text or "").lower()
    pieces: List[str] = []

    def add(k: str, label: str) -> None:
        v = (parsed.get(k) or "").strip() if isinstance(parsed.get(k), str) else ""
        if v:
            pieces.append(f"{label}：{v}")

    wants_phone = any(x in q for x in ["電話", "tel", "phone", "聯絡電話"])
    wants_addr = any(x in q for x in ["地址", "location", "在哪", "怎麼去"])
    wants_hours = any(x in q for x in ["營業", "時間", "幾點", "opening", "hours"])
    wants_web = any(x in q for x in ["官網", "網站", "website", "url"])
    wants_email = any(x in q for x in ["信箱", "email", "e-mail", "郵件"])

    if wants_phone:
        add("phone", "電話")
    if wants_addr:
        add("address", "地址")
    if wants_hours:
        add("hours", "營業時間")
    if wants_web:
        add("website", "官網")
    if wants_email:
        add("email", "Email")

    if not any([wants_phone, wants_addr, wants_hours, wants_web, wants_email]):
        add("name", "公司名稱")
        add("address", "地址")
        add("phone", "電話")
        add("hours", "營業時間")
        add("website", "官網")
        add("email", "Email")

    raw_short = truncate_text(parsed.get("raw") or text, 900)
    if raw_short:
        pieces.append("\n---\n（原文節錄）\n" + raw_short)

    return ["【公司資訊（company_info.md）】\n" + "\n".join(pieces)]


def build_company_card() -> Optional[Dict[str, Any]]:
    _, parsed = load_company_info(force=False)
    if not parsed:
        return None

    name = (parsed.get("name") or "尚澤光電股份有限公司").strip()
    phone = (parsed.get("phone") or "").strip()
    addr = (parsed.get("address") or "").strip()
    hours = (parsed.get("hours") or "").strip()
    website = (parsed.get("website") or "").strip()

    lines = []
    if phone:
        lines.append(f"電話：{phone}")
    if addr:
        lines.append(f"地址：{addr}")
    if hours:
        lines.append("營業時間：\n" + hours)

    img = (Config.COMPANY_CARD_IMAGE or "").strip()
    if img and (not is_http_url(img)) and (not img.startswith("/images/")):
        img = image_to_web_path(img)

    return {
        "title": name,
        "url": website,
        "category": "公司資訊",
        "product_category": "公司資訊",
        "description": "\n".join(lines).strip(),
        "specifications": "",
        "images": [img] if img else [],
        "image": img or "",
    }


# -----------------------------
# Product MD parsing / cards
# -----------------------------
def extract_product_highlights_from_md(md: str, max_chars: int = 1200) -> str:
    if not md:
        return ""
    text = re.sub(r"\n{3,}", "\n\n", md.strip())
    if len(text) <= max_chars:
        return text
    keep = [text[:600]]
    keywords = ["產品定位", "量測能力", "量測性能", "適用", "不適用", "選型", "提醒", "應用", "光源", "波段", "規格", "系統"]
    for kw in keywords:
        m = re.search(rf"(#+\s*.*{re.escape(kw)}.*\n[\s\S]{{0,650}})", text, re.IGNORECASE)
        if m:
            keep.append(m.group(1))
    merged = re.sub(r"\n{3,}", "\n\n", "\n\n".join(keep)).strip()
    return merged[:max_chars]


def parse_md_card(md_text: str, fallback_title: str = "") -> Dict[str, Any]:
    raw = md_text or ""
    title = (fallback_title or "").strip()

    m = re.search(r"(產品名稱|名稱)\s*[:：]\s*(.+)", raw)
    if m:
        title = m.group(2).strip()

    model = ""
    m = re.search(r"(型號|Model)\s*[:：]\s*(.+)", raw, re.IGNORECASE)
    if m:
        model = m.group(2).strip()

    url = ""
    m = re.search(r"(產品頁面連結|產品連結|Product\s*Page)\s*[:：]?\s*\n\s*(https?://\S+)", raw, re.IGNORECASE)
    if m:
        url = m.group(2).strip()

    images: List[str] = []
    for im in re.findall(r"^\s*-\s*([^\s\)]+?\.(?:jpg|jpeg|png|webp))\b", raw, flags=re.IGNORECASE | re.MULTILINE):
        images.append(im.strip())

    images2 = [image_to_web_path(x) for x in images if x]
    return {
        "title": title or "產品",
        "url": url,
        "category": "產品資料",
        "product_category": "產品資料",
        "description": (f"型號：{model}\n（可點右側卡片查看原始頁面）" if model else "（可點右側卡片查看原始頁面）").strip(),
        "specifications": "",
        "images": images2,
        "image": (images2[0] if images2 else ""),
        "_model": model.strip(),
    }


MODEL_REGEX = re.compile(r"\b([A-Za-z]{1,}\-?\d{2,}[A-Za-z0-9\-]*)\b")
BAD_ABBREVS = {"DNA", "LED", "UV", "UVA", "UVB", "VIS", "NIR", "IR"}
SALES_PATTERNS = [r"有賣嗎", r"有賣", r"有沒有賣", r"販售", r"賣不賣", r"有沒有", r"可以買", r"價格", r"報價", r"多少錢"]


def has_model_token(text: str) -> bool:
    return bool(text and MODEL_REGEX.search(text))


def extract_model_key(user_text: str) -> str:
    q = (user_text or "").strip()
    if not q:
        return ""
    m = MODEL_REGEX.search(q)
    return (m.group(1) if m else "").strip()


# -----------------------------
# Auto image resolver (prefer *_main.jpg in IMAGE_DIR)
# -----------------------------
def _norm_model_for_filename(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    s = s.replace("-", "_")
    s = re.sub(r"[^A-Za-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def guess_main_image_for_model(model_or_title: str) -> str:
    base = _norm_model_for_filename(model_or_title)
    if not base:
        return ""
    if not os.path.isdir(Config.IMAGE_DIR):
        return ""

    exts = [".jpg", ".jpeg", ".png", ".webp"]
    candidates = [f"{base}_main{e}" for e in exts] + [f"{base}_main".lower() + e for e in exts]
    for fn in candidates:
        p = os.path.join(Config.IMAGE_DIR, fn)
        if os.path.isfile(p):
            return image_to_web_path(fn)

    try:
        for fn in os.listdir(Config.IMAGE_DIR):
            fl = fn.lower()
            if fl.endswith((".jpg", ".jpeg", ".png", ".webp")) and ("_main" in fl):
                if base.lower() in fl:
                    p = os.path.join(Config.IMAGE_DIR, fn)
                    if os.path.isfile(p):
                        return image_to_web_path(fn)
    except Exception:
        pass

    return ""


def ensure_card_images(card: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(card, dict):
        return card
    images = safe_json_list(card.get("images"))
    image = (card.get("image") or "").strip()

    if images:
        images2 = [image_to_web_path(x) for x in images if x]
        card["images"] = images2
        card["image"] = image_to_web_path(image) if image else (images2[0] if images2 else "")
        return card

    model = (card.get("_model") or "").strip()
    title = (card.get("title") or "").strip()
    key = model or title
    im = guess_main_image_for_model(key)
    if im:
        card["images"] = [im]
        card["image"] = im
    else:
        card["images"] = []
        card["image"] = ""
    return card


# -----------------------------
# ✅ BM25 tokenizer (中英混合 + CJK bigram)
# -----------------------------
RE_LATIN_WORD = re.compile(r"[a-zA-Z0-9]+")
RE_CJK_SEQ = re.compile(r"[\u4e00-\u9fff]{2,}")


def bm25_tokenize(text: str, max_tokens: int = 220) -> List[str]:
    """
    - 英數詞：lower
    - 中文：取連續片段 + bigram（提高匹配）
    """
    if not text:
        return []
    t = text.lower()
    t = t.replace("-", " ").replace("_", " ")
    tokens: List[str] = []

    for w in RE_LATIN_WORD.findall(t):
        if len(w) >= 2:
            tokens.append(w)

    for seg in RE_CJK_SEQ.findall(text):
        seg = seg.strip()
        if len(seg) < 2:
            continue
        tokens.append(seg)
        for i in range(len(seg) - 1):
            tokens.append(seg[i : i + 2])

    if len(tokens) > max_tokens:
        tokens = tokens[:max_tokens]
    return tokens


# -----------------------------
# ✅ Generic BM25 index for arbitrary docs (system_docs + company_info fallback)
# -----------------------------
class BM25CorpusIndex:
    def __init__(self, k1: float = 1.5, b: float = 0.75):
        self.k1 = float(k1)
        self.b = float(b)
        self._ready = False

        self.doc_ids: List[str] = []
        self.doc_sources: List[str] = []
        self.doc_text_snippets: List[str] = []
        self.tf: List[Counter] = []
        self.df: Dict[str, int] = {}
        self.dl: List[int] = []
        self.avgdl: float = 0.0

    def build(self, docs: List[Tuple[str, str, str]], max_chars: int = 6500) -> None:
        """
        docs: [(doc_id, source_label, raw_text)]
        """
        self._ready = False
        self.doc_ids, self.doc_sources, self.doc_text_snippets = [], [], []
        self.tf, self.df, self.dl = [], {}, []
        self.avgdl = 0.0

        if not docs:
            return

        df = defaultdict(int)
        tf_list: List[Counter] = []
        dl_list: List[int] = []
        ids: List[str] = []
        sources: List[str] = []
        snippets: List[str] = []

        for doc_id, src, text in docs:
            raw = (text or "").strip()
            raw = raw[:max_chars] if raw else ""
            toks = bm25_tokenize(raw)
            tf = Counter(toks)
            dl = sum(tf.values())
            if dl > 0:
                for term in tf.keys():
                    df[term] += 1
            ids.append(doc_id)
            sources.append(src)
            snippets.append(raw)
            tf_list.append(tf)
            dl_list.append(dl)

        self.doc_ids = ids
        self.doc_sources = sources
        self.doc_text_snippets = snippets
        self.tf = tf_list
        self.df = dict(df)
        self.dl = dl_list
        nonzero = [d for d in dl_list if d > 0]
        self.avgdl = float(sum(nonzero) / max(1, len(nonzero))) if nonzero else 0.0
        self._ready = True

    def search(self, query: str, topn: int = 4) -> List[Dict[str, Any]]:
        if not self._ready or not self.doc_ids:
            return []
        q = (query or "").strip()
        if not q:
            return []
        q_tokens = bm25_tokenize(q, max_tokens=140)
        if not q_tokens:
            return []

        N = len(self.doc_ids)
        avgdl = self.avgdl if self.avgdl > 0 else 1.0
        k1 = self.k1
        b = self.b
        q_tf = Counter(q_tokens)

        scored: List[Tuple[float, int]] = []
        for i in range(N):
            tf = self.tf[i]
            dl = self.dl[i]
            if dl <= 0 or not tf:
                continue
            score = 0.0
            for term, qf in q_tf.items():
                df = self.df.get(term, 0)
                if df <= 0:
                    continue
                f = tf.get(term, 0)
                if f <= 0:
                    continue
                idf = math.log(1.0 + (N - df + 0.5) / (df + 0.5))
                denom = f + k1 * (1.0 - b + b * (dl / avgdl))
                score += idf * (f * (k1 + 1.0) / denom) * (1.0 + 0.15 * min(4, qf - 1))
            if score > 0:
                scored.append((score, i))

        if not scored:
            return []
        scored.sort(key=lambda x: x[0], reverse=True)

        out: List[Dict[str, Any]] = []
        for score, idx in scored[: max(10, topn * 2)]:
            out.append(
                {
                    "doc_id": self.doc_ids[idx],
                    "source": self.doc_sources[idx],
                    "score": float(score),
                    "text": self.doc_text_snippets[idx],
                }
            )
            if len(out) >= topn:
                break
        return out


# -----------------------------
# Product structured index + known-model allowlist source + ✅ BM25 index (product)
# -----------------------------
class ProductMDIndex:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.files: List[str] = []
        self.path_by_norm_stem: Dict[str, str] = {}
        self.path_by_norm_name: Dict[str, str] = {}
        self.path_by_norm_model: Dict[str, str] = {}
        self.display_name_by_path: Dict[str, str] = {}
        self.model_by_path: Dict[str, str] = {}
        self.known_models: List[str] = []

        # ✅ BM25 structures (product only)
        self._bm25_ready = False
        self._bm25_doc_paths: List[str] = []
        self._bm25_doc_stems: List[str] = []
        self._bm25_tf: List[Counter] = []
        self._bm25_df: Dict[str, int] = {}
        self._bm25_dl: List[int] = []
        self._bm25_avgdl: float = 0.0
        self._build()

    def _build(self) -> None:
        self.files = []
        self.path_by_norm_stem.clear()
        self.path_by_norm_name.clear()
        self.path_by_norm_model.clear()
        self.display_name_by_path.clear()
        self.model_by_path.clear()
        self.known_models = []

        self._bm25_ready = False
        self._bm25_doc_paths = []
        self._bm25_doc_stems = []
        self._bm25_tf = []
        self._bm25_df = {}
        self._bm25_dl = []
        self._bm25_avgdl = 0.0

        if not self.base_dir or not os.path.isdir(self.base_dir):
            logger.warning(f"PRODUCT_STRUCTURED_DIR not found: {self.base_dir}")
            return

        models_set = set()

        self.files = [fn for fn in os.listdir(self.base_dir) if fn.startswith("product_") and fn.lower().endswith(".md")]
        self.files.sort()

        df = defaultdict(int)
        tf_list: List[Counter] = []
        dl_list: List[int] = []
        doc_paths: List[str] = []
        doc_stems: List[str] = []

        for fn in self.files:
            full = os.path.join(self.base_dir, fn)

            stem = fn[len("product_") :].replace(".md", "")
            if stem:
                models_set.add(stem.strip())

            norm_stem = re.sub(r"[^a-z0-9]", "", stem.lower())
            if norm_stem:
                self.path_by_norm_stem[norm_stem] = full

            try:
                raw = read_text_file(full)
                head = raw[:7000]
            except Exception:
                raw = ""
                head = ""

            m = re.search(r"(產品名稱|名稱)\s*[:：]\s*(.+)", head)
            if m:
                name = m.group(2).strip()
                self.display_name_by_path[full] = name
                nk = normalize_key(name)
                if nk:
                    self.path_by_norm_name[nk] = full

            m = re.search(r"(型號|Model)\s*[:：]\s*(.+)", head, re.IGNORECASE)
            if m:
                model = m.group(2).strip()
                self.model_by_path[full] = model
                models_set.add(model)
                mk = normalize_key(model)
                if mk:
                    self.path_by_norm_model[mk] = full

            if Config.BM25_ENABLED:
                name = (self.display_name_by_path.get(full) or "")
                model = (self.model_by_path.get(full) or "")
                snippet = raw[: Config.BM25_TEXT_MAX_CHARS] if raw else head[: Config.BM25_TEXT_MAX_CHARS]
                doc_text = f"{stem}\n{name}\n{model}\n{snippet}"
                toks = bm25_tokenize(doc_text)
                tf = Counter(toks)
                dl = sum(tf.values())
                if dl <= 0:
                    tf = Counter()
                    dl = 0
                else:
                    for term in tf.keys():
                        df[term] += 1

                doc_paths.append(full)
                doc_stems.append(stem)
                tf_list.append(tf)
                dl_list.append(dl)

        self.known_models = sorted({m for m in models_set if m and len(m) >= 3}, key=lambda x: x.lower())
        logger.info(f"Loaded product_structured index: {len(self.files)} files, known_models={len(self.known_models)}")

        if Config.BM25_ENABLED and doc_paths:
            self._bm25_doc_paths = doc_paths
            self._bm25_doc_stems = doc_stems
            self._bm25_tf = tf_list
            self._bm25_df = dict(df)
            self._bm25_dl = dl_list
            nonzero = [d for d in dl_list if d > 0]
            self._bm25_avgdl = float(sum(nonzero) / max(1, len(nonzero))) if nonzero else 0.0
            self._bm25_ready = True
            logger.info(
                f"BM25(product) ready: docs={len(self._bm25_doc_paths)}, avgdl={self._bm25_avgdl:.1f}, vocab={len(self._bm25_df)}"
            )

    def rebuild(self) -> None:
        self._build()

    def guess_path(self, query: str) -> Optional[str]:
        if not query:
            return None
        q = query.strip()
        if not q:
            return None

        direct = os.path.join(self.base_dir, f"product_{q}.md")
        if os.path.isfile(direct):
            return direct

        nq = normalize_key(q)
        p = self.path_by_norm_model.get(nq) or self.path_by_norm_name.get(nq) or self.path_by_norm_stem.get(nq)
        if p and os.path.isfile(p):
            return p

        ql = q.lower()
        for fn in self.files:
            stem = fn.lower().replace("product_", "").replace(".md", "")
            tokens = [t for t in re.split(r"[_\-]+", stem) if len(t) >= 4]
            if any(t in ql for t in tokens):
                full = os.path.join(self.base_dir, fn)
                if os.path.isfile(full):
                    return full
        return None

    def guess_from_user_text(self, user_text: str) -> Optional[str]:
        mk = extract_model_key(user_text)
        if mk:
            p = self.guess_path(mk)
            if p:
                return p
        for t in expand_by_alias(user_text):
            p = self.guess_path(t)
            if p:
                return p
        return self.guess_path(user_text)

    def display_name(self, path: str, fallback: str = "") -> str:
        return (self.display_name_by_path.get(path) or fallback or "").strip()

    def model_of(self, path: str) -> str:
        return (self.model_by_path.get(path) or "").strip()

    def bm25_rank_models(self, query_text: str, extra_terms: Optional[List[str]] = None, topn: int = None) -> List[str]:
        if not Config.BM25_ENABLED or (not self._bm25_ready) or (not self._bm25_doc_paths):
            return []
        if topn is None:
            topn = Config.BM25_TOPN

        q = (query_text or "").strip()
        if extra_terms:
            q = q + "\n" + " ".join([t for t in extra_terms if t])

        q_tokens = bm25_tokenize(q, max_tokens=120)
        if not q_tokens:
            return []

        N = len(self._bm25_doc_paths)
        avgdl = self._bm25_avgdl if self._bm25_avgdl > 0 else 1.0
        k1 = Config.BM25_K1
        b = Config.BM25_B

        q_tf = Counter(q_tokens)

        scored: List[Tuple[float, int]] = []
        for i in range(N):
            tf = self._bm25_tf[i]
            dl = self._bm25_dl[i]
            if dl <= 0 or not tf:
                continue

            score = 0.0
            for term, qf in q_tf.items():
                df = self._bm25_df.get(term, 0)
                if df <= 0:
                    continue
                f = tf.get(term, 0)
                if f <= 0:
                    continue
                idf = math.log(1.0 + (N - df + 0.5) / (df + 0.5))
                denom = f + k1 * (1.0 - b + b * (dl / avgdl))
                score += idf * (f * (k1 + 1.0) / denom) * (1.0 + 0.15 * min(4, qf - 1))

            if score > 0:
                scored.append((score, i))

        if not scored:
            return []

        scored.sort(key=lambda x: x[0], reverse=True)
        out: List[str] = []
        seen = set()
        for _, idx in scored[: max(12, topn * 2)]:
            stem = (self._bm25_doc_stems[idx] or "").strip()
            if not stem:
                continue
            k = stem.lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(stem)
            if len(out) >= topn:
                break
        return out


# -----------------------------
# Smart query expansion (hit-rate booster)
# -----------------------------
KEYWORD_EXPANSION = {
    "ppfd": ["PPFD", "PPF", "植物燈", "光子通量", "光合光子", "PAR", "植物照明"],
    "ppf": ["PPFD", "PPF", "植物燈", "光子通量", "PAR", "植物照明"],
    "par": ["PPFD", "PPF", "PAR", "植物燈", "植物照明", "光子通量"],
    "uvc": ["UVC", "消毒", "輻照度", "辐照度", "radiometer", "uvc led", "紫外線消毒", "254nm"],
    "輝度": ["輝度", "luminance", "亮度", "nit", "cd/m2", "cd/m²"],
    "照度": ["照度", "lux", "illuminance"],
    "光譜": ["光譜", "spectrum", "波長", "spectrometer", "光譜儀"],
    "穿透": ["穿透率", "透過率", "transmittance", "玻璃穿透"],
    "反射": ["反射率", "reflectance", "鏡面反射"],
}


def build_search_queries(
    user_text: str,
    alias_terms: List[str],
    extra_terms: Optional[List[str]] = None,
    model_key: str = "",
) -> List[str]:
    base = (user_text or "").strip()
    qs: List[str] = []
    seen = set()

    def add(q: str):
        q = (q or "").strip()
        if not q:
            return
        k = q.lower()
        if k in seen:
            return
        seen.add(k)
        qs.append(q)

    add(base)
    if model_key:
        add(model_key)

    for t in alias_terms or []:
        add(t)

    for t in (extra_terms or []):
        add(t)

    low = base.lower()
    for k, arr in KEYWORD_EXPANSION.items():
        if k in low or k in base:
            for x in arr:
                add(x)

    key_tokens = []
    if model_key:
        key_tokens.append(model_key)
    key_tokens += [t for t in (extra_terms or []) if t]
    key_tokens += [t for t in (alias_terms[:4] if alias_terms else []) if t]
    if key_tokens:
        add(" ".join(key_tokens))

    return qs[:10]


def vdb_multi_search(vdb: VectorDatabase, queries: List[str], top_k_each: int, cap: int) -> List[Dict[str, Any]]:
    if not queries:
        return []

    merged: Dict[str, Dict[str, Any]] = {}
    for q in queries:
        try:
            hits = vdb.search(q, top_k=top_k_each) if hasattr(vdb, "search") else []
        except Exception:
            hits = []
        for d in hits or []:
            txt = (d.get("text") or "").strip()
            if not txt:
                continue
            key = re.sub(r"\s+", " ", txt).lower()[:260]
            dist = float(d.get("distance") or 9999.0)
            if key not in merged or dist < float(merged[key].get("distance") or 9999.0):
                merged[key] = d

    out = list(merged.values())
    out.sort(key=lambda x: float(x.get("distance") or 9999.0))
    return out[:cap]


# -----------------------------
# Cancel support
# -----------------------------
def _register_cancel(rid: str) -> threading.Event:
    ev = threading.Event()
    with CANCEL_LOCK:
        CANCEL_EVENTS[rid] = ev
    return ev


def _pop_cancel(rid: str) -> None:
    with CANCEL_LOCK:
        CANCEL_EVENTS.pop(rid, None)


def _cancel(rid: str) -> bool:
    with CANCEL_LOCK:
        ev = CANCEL_EVENTS.get(rid)
        if not ev:
            return False
        ev.set()
        return True


# -----------------------------
# Ollama (Cancelable streaming + keep_alive + queue timeout)
# -----------------------------
def ollama_generate_cancelable(
    prompt: str,
    model: str,
    cancel_event: threading.Event,
    temperature: float = 0.2,
    num_predict: int = 512,
) -> str:
    url = Config.OLLAMA_URL + "/api/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": True,
        "keep_alive": Config.OLLAMA_KEEP_ALIVE,
        "options": {"temperature": float(temperature), "num_predict": int(num_predict)},
    }

    buf: List[str] = []
    resp = None

    try:
        resp = HTTP.post(url, json=payload, timeout=Config.OLLAMA_TIMEOUT, stream=True)
        resp.raise_for_status()

        for line in resp.iter_lines(decode_unicode=True):
            if cancel_event.is_set():
                try:
                    resp.close()
                except Exception:
                    pass
                raise RuntimeError("CANCELLED")

            if not line:
                continue

            try:
                j = json.loads(line)
            except Exception:
                continue

            chunk = j.get("response") or ""
            if chunk:
                buf.append(chunk)

            if j.get("done") is True:
                break

        return "".join(buf).strip()
    finally:
        try:
            if resp is not None:
                resp.close()
        except Exception:
            pass


# -----------------------------
# Intent classify (heuristic + fallback LLM)
# -----------------------------
def classify_intent(user_text: str, md_index: Optional[ProductMDIndex] = None) -> str:
    t = (user_text or "").strip()
    tl = t.lower()

    if has_model_token(t):
        return "PRODUCT_SPEC"

    abbrev = re.findall(r"\b[A-Z]{2,6}\b", t)
    if abbrev and any(a in BAD_ABBREVS for a in abbrev) and (("量測" in t) or ("測量" in t) or ("測" in t)):
        return "OTHER"

    if any(k in tl for k in ["地址", "電話", "聯絡", "公司", "官網", "email", "信箱", "營業", "幾點"]):
        return "COMPANY_INFO"
    if any(k in tl for k in ["怎麼安裝", "怎麼設定", "錯誤", "無法", "故障", "支援", "校正", "校準"]):
        return "TECH_SUPPORT"

    if any(re.search(p, t) for p in SALES_PATTERNS):
        if md_index and md_index.guess_from_user_text(t):
            return "PRODUCT_SPEC"
        return "OTHER"

    alias_terms = expand_by_alias(t)
    alias_hit = False
    if md_index and alias_terms:
        for at in alias_terms:
            if md_index.guess_path(at):
                alias_hit = True
                break

    if alias_hit or any(
        k in tl
        for k in [
            "規格",
            "型號",
            "差異",
            "推薦",
            "列出",
            "是什麼",
            "介紹",
            "適用",
            "不適用",
            "量測系統",
            "量測",
            "光譜",
            "輝度",
            "照度",
            "積分球",
            "ppfd",
            "ppf",
            "par",
            "uvc",
        ]
    ):
        return "PRODUCT_SPEC"

    prompt = (
        "你是意圖分類器，只能輸出下列其中一個：\n"
        "PRODUCT_SPEC, COMPANY_INFO, TECH_SUPPORT, OTHER\n\n"
        f"使用者問題：{t}\n"
        "只輸出標籤："
    )
    try:
        ans = ollama_generate_cancelable(
            prompt=prompt,
            model=Config.OLLAMA_MODEL_INTENT,
            cancel_event=threading.Event(),
            temperature=0.0,
            num_predict=16,
        ).upper()
        for lab in ["PRODUCT_SPEC", "COMPANY_INFO", "TECH_SUPPORT", "OTHER"]:
            if lab in ans:
                return lab
    except Exception as e:
        logger.warning(f"classify_intent fallback failed: {e}")

    return "OTHER"


# -----------------------------
# DB for cards
# -----------------------------
def db_get_products_by_titles(conn: sqlite3.Connection, titles: List[str], limit: int = 8) -> List[Dict[str, Any]]:
    if not titles:
        return []
    titles = [t for t in titles if t]
    if not titles:
        return []

    placeholders = ",".join(["?"] * len(titles))
    sql = f"""
        SELECT id, title, url, category, description, specifications, images, created_at
        FROM products
        WHERE title IN ({placeholders})
        LIMIT ?
    """
    rows = conn.execute(sql, (*titles, int(limit))).fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        imgs = safe_json_list(r["images"])
        imgs2 = [image_to_web_path(it) for it in imgs if it]
        card = {
            "title": r["title"] or "",
            "url": r["url"] or "",
            "category": r["category"] or "",
            "product_category": r["category"] or "",
            "description": (r["description"] or "").strip(),
            "specifications": (r["specifications"] or "").strip(),
            "images": imgs2,
            "image": (imgs2[0] if imgs2 else ""),
            "_model": (r["title"] or "").strip(),
        }
        out.append(ensure_card_images(card))
    return out


def db_search_products_by_keyword(conn: sqlite3.Connection, keyword: str, limit: int = 8) -> List[Dict[str, Any]]:
    kw = (keyword or "").strip()
    if not kw:
        return []

    sql = """
        SELECT id, title, url, category, description, specifications, images, created_at
        FROM products
        WHERE title LIKE ?
        ORDER BY id ASC
        LIMIT ?
    """
    rows = conn.execute(sql, (f"%{kw}%", int(limit))).fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        imgs = safe_json_list(r["images"])
        imgs2 = [image_to_web_path(it) for it in imgs if it]
        card = {
            "title": r["title"] or "",
            "url": r["url"] or "",
            "category": r["category"] or "",
            "product_category": r["category"] or "",
            "description": (r["description"] or "").strip(),
            "specifications": (r["specifications"] or "").strip(),
            "images": imgs2,
            "image": (imgs2[0] if imgs2 else ""),
            "_model": (r["title"] or "").strip(),
        }
        out.append(ensure_card_images(card))
    return out


# -----------------------------
# Session store
# -----------------------------
def session_path(session_id: str) -> str:
    ensure_dir(Config.SESSIONS_DIR)
    return os.path.join(Config.SESSIONS_DIR, f"{session_id}.json")


def load_session(session_id: str) -> Dict[str, Any]:
    p = session_path(session_id)
    if not os.path.isfile(p):
        return {"messages": [], "decision": None, "meta": {}}
    return safe_json_loads(read_text_file(p), {"messages": [], "decision": None, "meta": {}})


def save_session(session_id: str, data: Dict[str, Any]) -> None:
    # ✅ (2) atomic write + per-session lock
    p = session_path(session_id)
    ensure_dir(os.path.dirname(p))
    lock = _get_session_lock(session_id)
    with lock:
        tmp = p + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, p)  # atomic replace


# -----------------------------
# Answer modes / prompts
# -----------------------------
def pick_answer_mode(payload: Dict[str, Any]) -> str:
    mode = (payload.get("answer_mode") or payload.get("mode") or Config.DEFAULT_ANSWER_MODE).upper()
    if mode not in ("FAST", "NORMAL"):
        mode = "NORMAL"
    return mode


def build_answer_prompt(
    user_text: str,
    intent: str,
    context_blocks: List[str],
    answer_mode: str,
    session_messages: Optional[List[Dict[str, Any]]] = None,
    allowlist_picks: Optional[List[str]] = None,
) -> str:
    sys = (
        "你是尚澤光電的企業客服與產品選型助理。\n"
        "規則：\n"
        "1) 優先使用「提供的資料內容」回答，不要編造不存在的規格數字/不存在的產品型號。\n"
        "2) 若資料不足，請用：『資料中尚未包含：XXX』並提出最多 2 個關鍵補充問題。\n"
        "3) 回答用繁體中文，條列清楚。\n"
        "4) 一般模式：8~14 行；快速模式：4~8 行。\n"
    )

    if intent in ("PRODUCT_SPEC", "DECISION_RESULT"):
        sys += (
            "5) 選型/產品問題請用顧問式：\n"
            "   - 先列候選型號（2~3）\n"
            "   - 再比較差異與適用情境\n"
            "   - 再列不適用/風險\n"
            "   - 最後給明確建議與下一步\n"
        )
        if Config.STRICT_DECISION_ALLOWLIST and allowlist_picks:
            sys += (
                "6) 【嚴格限制】若需要列出候選型號：只能使用「候選清單」裡的型號；\n"
                "   絕對禁止編造/猜測/自創任何不在清單內的型號。\n"
                "   如果清單不足以選出 2~3 台：請明確說『資料庫目前沒有對應型號』，並提出最多 2 個補充問題。\n"
            )

    ctx = "\n\n---\n\n".join([c for c in context_blocks if c.strip()])
    mode_hint = "（快速模式：精簡回答）" if answer_mode == "FAST" else "（一般模式：完整回答）"

    history = ""
    if session_messages:
        msgs = session_messages[-(Config.SESSION_MAX_TURNS * 2) :]
        lines = []
        for m in msgs:
            role = m.get("role", "")
            content = (m.get("content") or "").strip()
            if not content:
                continue
            if role == "user":
                lines.append(f"使用者：{content}")
            elif role == "assistant":
                lines.append(f"助理：{content}")
        if lines:
            history = "【對話歷史】\n" + "\n".join(lines) + "\n\n"

    allowlist_block = ""
    if Config.STRICT_DECISION_ALLOWLIST and allowlist_picks:
        picks = allowlist_picks[: Config.ALLOWLIST_MAX_ITEMS]
        allowlist_block = "【候選清單（只能從這裡挑型號）】\n" + "、".join(picks) + "\n\n"

    return (
        f"{sys}\n{mode_hint}\n"
        f"PromptVersion={Config.PROMPT_VERSION}\n\n"
        f"{history}"
        f"{allowlist_block}"
        f"【資料內容】\n{ctx}\n\n"
        f"【使用者問題】\n{user_text}\n\n"
        "請直接給出答案："
    )


def prompt_guard_trim(context_blocks: List[str], base_prompt_builder, max_chars: int) -> Tuple[List[str], str]:
    blocks = list(context_blocks)

    def build(blocks_now: List[str]) -> str:
        return base_prompt_builder(blocks_now)

    p = build(blocks)
    if len(p) <= max_chars:
        return blocks, p

    def is_rag(b: str) -> bool:
        return b.startswith("【向量庫片段：") or b.startswith("【資料片段：")

    def is_db(b: str) -> bool:
        return b.startswith("【DB 產品摘要：")

    while len(p) > max_chars:
        rag_idxs = [i for i, b in enumerate(blocks) if is_rag(b)]
        if not rag_idxs:
            break
        blocks.pop(rag_idxs[-1])
        p = build(blocks)

    if len(p) <= max_chars:
        return blocks, p

    while len(p) > max_chars:
        db_idxs = [i for i, b in enumerate(blocks) if is_db(b)]
        if not db_idxs:
            break
        blocks.pop(db_idxs[-1])
        p = build(blocks)

    if len(p) <= max_chars:
        return blocks, p

    new_blocks = []
    for b in blocks:
        if b.startswith("【產品文件："):
            parts = b.split("\n", 1)
            if len(parts) == 2:
                head, body = parts[0], parts[1]
                body2 = truncate_text(body, 700)
                new_blocks.append(head + "\n" + body2)
            else:
                new_blocks.append(b)
        else:
            new_blocks.append(b)

    blocks = new_blocks
    p = build(blocks)

    while len(p) > max_chars and len(blocks) > 2:
        blocks.pop()
        p = build(blocks)

    return blocks, p


# -----------------------------
# RAG helpers (dedupe + per-source cap)
# -----------------------------
def vdb_search(vdb: VectorDatabase, query: str, top_k: int) -> List[Dict[str, Any]]:
    return vdb.search(query, top_k=top_k) if hasattr(vdb, "search") else []


def rag_dedupe_and_cap(docs: List[Dict[str, Any]], per_source_max: int) -> List[Dict[str, Any]]:
    if not docs:
        return []
    docs2 = sorted(docs, key=lambda x: float(x.get("distance") or 9999.0))

    per_source_count: Dict[str, int] = {}
    seen_text = set()
    out = []

    for d in docs2:
        meta = d.get("metadata") or {}
        src = (meta.get("source") or meta.get("file") or meta.get("id") or "").strip()
        src_key = src or "_"

        if per_source_count.get(src_key, 0) >= per_source_max:
            continue

        txt = (d.get("text") or "").strip()
        if not txt:
            continue

        if Config.RAG_DEDUPE_TEXT:
            key = re.sub(r"\s+", " ", txt)[:260].lower()
            if key in seen_text:
                continue
            seen_text.add(key)

        per_source_count[src_key] = per_source_count.get(src_key, 0) + 1
        out.append(d)

    return out


# -----------------------------
# ✅ Fusion allowlist (BM25 + Vector via RRF) — keeps allowlist non-empty
# -----------------------------
def _extract_product_stem_from_source(src: str) -> str:
    s = (src or "").strip()
    if not s:
        return ""
    base = s.replace("\\", "/").split("/")[-1]
    if base.startswith("product_") and base.lower().endswith(".md"):
        return base[len("product_") : -len(".md")].strip()
    return ""


def _is_known_model(md_index: ProductMDIndex, x: str) -> bool:
    if not x:
        return False
    nx = normalize_key(x)
    if not nx:
        return False
    for m in md_index.known_models:
        if normalize_key(m) == nx:
            return True
    return False


def extract_candidate_models_from_rag(md_index: ProductMDIndex, rag_docs: List[Dict[str, Any]], limit: int = 12) -> List[str]:
    out: List[str] = []
    seen = set()

    def add(x: str):
        x = (x or "").strip()
        if not x:
            return
        k = x.lower()
        if k in seen:
            return
        seen.add(k)
        out.append(x)

    for d in rag_docs or []:
        meta = d.get("metadata") or {}
        src = (meta.get("source") or meta.get("file") or "").strip()
        stem = _extract_product_stem_from_source(src)
        if stem:
            add(stem)

        title = (meta.get("title") or "").strip()
        if title:
            p = md_index.guess_path(title)
            if p:
                add(os.path.basename(p)[len("product_") : -len(".md")])

        txt = (d.get("text") or "")
        for mm in MODEL_REGEX.findall(txt)[:8]:
            if _is_known_model(md_index, mm):
                add(mm)

        if len(out) >= limit:
            break

    return out[:limit]


def rrf_fuse(ranked_lists: List[List[str]], k: int = 60, topn: int = 24) -> List[str]:
    score = defaultdict(float)
    for lst in ranked_lists:
        for rank, item in enumerate(lst, start=1):
            if not item:
                continue
            score[item] += 1.0 / float(k + rank)
    if not score:
        return []
    items = sorted(score.items(), key=lambda x: x[1], reverse=True)
    return [x for x, _ in items[:topn]]


def build_fused_allowlist(
    md_index: ProductMDIndex,
    vdb: VectorDatabase,
    user_text: str,
    extra_terms: Optional[List[str]],
    search_queries: List[str],
    bm25_models: List[str],
    rag_docs: List[Dict[str, Any]],
) -> Tuple[List[str], Dict[str, Any]]:
    if not Config.FUSION_ENABLED:
        return [], {"enabled": False}

    vec_candidates = extract_candidate_models_from_rag(md_index, rag_docs, limit=max(12, Config.FUSION_VEC_TOPN))
    bm_candidates = (bm25_models or [])[: max(12, Config.FUSION_BM25_TOPN)]

    fused = rrf_fuse(
        ranked_lists=[bm_candidates, vec_candidates],
        k=max(10, int(Config.FUSION_RRF_K)),
        topn=max(8, int(Config.ALLOWLIST_MAX_ITEMS)),
    )

    overlap = len(set([x.lower() for x in bm_candidates]) & set([x.lower() for x in vec_candidates]))
    overlap_ratio = overlap / float(max(1, min(len(bm_candidates), len(vec_candidates), 8)))

    if overlap_ratio >= 0.25:
        k_allow = 8
    elif overlap_ratio > 0:
        k_allow = 12
    else:
        k_allow = max(Config.FUSION_LOWCONF_MIN_ALLOW, 12)

    allow = fused[: min(Config.ALLOWLIST_MAX_ITEMS, k_allow)]

    low_conf = False
    if (not allow) and Config.FUSION_FORCE_NONEMPTY:
        low_conf = True
        if Config.FUSION_LOWCONF_ALLOW_ALL and md_index.known_models:
            allow = md_index.known_models[: Config.ALLOWLIST_MAX_ITEMS]
        else:
            allow = bm_candidates[: Config.ALLOWLIST_MAX_ITEMS] or vec_candidates[: Config.ALLOWLIST_MAX_ITEMS]

    meta = {
        "enabled": True,
        "bm25_candidates": bm_candidates[:10],
        "vec_candidates": vec_candidates[:10],
        "overlap": overlap,
        "overlap_ratio": round(overlap_ratio, 3),
        "k_allow": k_allow,
        "low_confidence": low_conf,
    }
    return allow, meta


# -----------------------------
# Product context (MD > Smart RAG > DB + ✅ BM25 structured fallback)
# -----------------------------
def build_product_context_priority_md(
    user_text: str,
    vdb: VectorDatabase,
    conn: sqlite3.Connection,
    topk: int,
    md_index: ProductMDIndex,
    extra_terms: Optional[List[str]] = None,
) -> Tuple[List[str], List[Dict[str, Any]], List[str], List[str], List[str], List[Dict[str, Any]]]:
    used_md_files: List[str] = []
    query_terms: List[str] = []

    model_key = extract_model_key(user_text)
    alias_terms = expand_by_alias(user_text)

    if model_key:
        query_terms.append(model_key)
    for t in alias_terms:
        if t and t not in query_terms:
            query_terms.append(t)
    if extra_terms:
        for t in extra_terms:
            if t and t not in query_terms:
                query_terms.append(t)

    search_queries = build_search_queries(
        user_text=user_text,
        alias_terms=alias_terms,
        extra_terms=extra_terms,
        model_key=model_key,
    )

    candidates: List[Dict[str, Any]] = []
    if model_key:
        candidates = db_search_products_by_keyword(conn, model_key, limit=Config.TOPK_CARD)
    if not candidates:
        for t in alias_terms:
            c = db_search_products_by_keyword(conn, t, limit=Config.TOPK_CARD)
            if c:
                candidates = c
                break

    md_blocks: List[str] = []
    md_cards: List[Dict[str, Any]] = []
    used_md = set()

    def _add_md(md_path: Optional[str], fallback_title: str = ""):
        nonlocal md_blocks, md_cards
        if not md_path or (not os.path.isfile(md_path)) or (md_path in used_md):
            return
        used_md.add(md_path)
        used_md_files.append(os.path.basename(md_path))
        md_text = read_text_file(md_path)
        md_blocks.append(f"【產品文件：{os.path.basename(md_path)}】\n{extract_product_highlights_from_md(md_text)}")
        card = parse_md_card(md_text, fallback_title=fallback_title or md_index.display_name(md_path, os.path.basename(md_path)))
        md_cards.append(ensure_card_images(card))

    for c in candidates:
        title = (c.get("title") or "").strip()
        if not title:
            continue
        md_path = md_index.guess_path(title)
        _add_md(md_path, fallback_title=md_index.display_name(md_path, title) if md_path else title)

    if not md_blocks:
        md_path = md_index.guess_from_user_text(user_text)
        _add_md(md_path, fallback_title=md_index.display_name(md_path, os.path.basename(md_path) if md_path else ""))

    has_md = bool(md_blocks)

    rag_limit = min(Config.RAG_MAX_BLOCKS, topk)
    if has_md:
        rag_limit = min(Config.RAG_MAX_BLOCKS_WHEN_MD, max(1, topk // 2), Config.RAG_MAX_BLOCKS)

    rag_docs = vdb_multi_search(vdb, search_queries, top_k_each=max(4, topk), cap=max(topk, rag_limit * 3))
    rag_docs = rag_dedupe_and_cap(rag_docs, per_source_max=max(1, Config.RAG_PER_SOURCE_MAX))[: max(1, rag_limit)]

    rag_blocks: List[str] = []
    rag_titles: List[str] = []
    for d in rag_docs:
        txt = truncate_text((d.get("text") or "").strip(), Config.RAG_CHUNK_MAX_CHARS)
        meta = d.get("metadata") or {}
        src = meta.get("source") or meta.get("file") or meta.get("id") or ""
        if txt:
            rag_blocks.append(f"【向量庫片段：{src}】\n{txt}")
        t = (meta.get("title") or "").strip()
        if t:
            rag_titles.append(t)

    relevant_docs: List[Dict[str, Any]] = []
    if md_cards:
        relevant_docs = md_cards[: Config.TOPK_CARD]
    elif candidates:
        relevant_docs = [ensure_card_images(c) for c in candidates[: Config.TOPK_CARD]]
    elif rag_titles:
        relevant_docs = db_get_products_by_titles(conn, rag_titles[: Config.TOPK_CARD], limit=Config.TOPK_CARD)

    bm25_models = md_index.bm25_rank_models(user_text, extra_terms=extra_terms, topn=Config.BM25_TOPN)
    if not relevant_docs:
        for m in bm25_models[:3]:
            p = md_index.guess_path(m)
            if p and os.path.isfile(p):
                _add_md(p, fallback_title=m)
        if md_cards:
            relevant_docs = md_cards[: Config.TOPK_CARD]

    db_blocks: List[str] = []
    if relevant_docs and (not has_md):
        for c in relevant_docs[:3]:
            title = c.get("title", "")
            desc = clean_placeholder(c.get("description") or "")
            specs = clean_placeholder(c.get("specifications") or "")
            chunk = "\n".join([x for x in [desc, specs] if x.strip()]).strip()
            if chunk:
                db_blocks.append(f"【DB 產品摘要：{title}】\n{truncate_text(chunk, 700)}")

    context_blocks: List[str] = []
    context_blocks.extend(md_blocks)
    context_blocks.extend(rag_blocks)
    if not has_md:
        context_blocks.extend(db_blocks)

    relevant_docs = [ensure_card_images(c) for c in (relevant_docs or [])]
    return context_blocks, relevant_docs, query_terms, used_md_files, bm25_models, rag_docs


# -----------------------------
# Anti-hallucination allowlist (Decision/Recommend)
# -----------------------------
def build_allowlist_from_sources(
    relevant_docs: List[Dict[str, Any]],
    used_md_files: List[str],
) -> List[str]:
    allow = []
    seen = set()

    def add(x: str):
        x = (x or "").strip()
        if not x:
            return
        k = x.lower()
        if k in seen:
            return
        seen.add(k)
        allow.append(x)

    for c in (relevant_docs or []):
        m = (c.get("_model") or "").strip()
        t = (c.get("title") or "").strip()
        if m and has_model_token(m):
            add(m)
        elif t and has_model_token(t):
            add(t)

    for fn in used_md_files or []:
        if fn.startswith("product_") and fn.lower().endswith(".md"):
            stem = fn[len("product_") : -len(".md")]
            add(stem)

    allow = [a for a in allow if len(a) >= 3]
    return allow[: Config.ALLOWLIST_MAX_ITEMS]


def extract_model_mentions(text: str) -> List[str]:
    if not text:
        return []
    found = MODEL_REGEX.findall(text)
    out = []
    seen = set()
    for f in found:
        f = (f or "").strip()
        if not f:
            continue
        k = f.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(f)
    return out


def enforce_allowlist_or_block(answer: str, allowlist: List[str]) -> Tuple[str, bool, List[str]]:
    if not Config.STRICT_DECISION_ALLOWLIST:
        return answer, False, []

    allow_norm = {normalize_key(x) for x in (allowlist or []) if x}
    if not allow_norm:
        safe = (
            "我可以幫你選型，但目前資料庫/產品文件沒有足夠的候選型號可供推薦，"
            "因此我不會亂編型號。\n\n"
            "請你補 1~2 個關鍵資訊後我再幫你配對：\n"
            "1) 你要量測的項目（光譜/輝度/照度/光強度/反射率/穿透率/PPFD…）\n"
            "2) 主要波段或對象（UVC LED / UVA / VIS / 玻璃 / 鏡面…）\n"
        )
        return safe, True, []

    mentions = extract_model_mentions(answer)
    bad = []
    for m in mentions:
        if normalize_key(m) not in allow_norm:
            bad.append(m)

    if not bad:
        return answer, False, []

    safe = (
        "⚠️ 為避免選型亂編，我只能從資料庫/產品文件中「確實存在」的型號做推薦。\n"
        "但我剛剛那段回覆中出現了資料庫不存在的型號，因此已被系統擋下。\n\n"
        "目前可用的候選型號清單如下：\n"
        f"{'、'.join(allowlist)}\n\n"
        "請你回覆：你希望我從以上清單中，偏向「研發」還是「產線/品管」用途？"
    )
    return safe, True, bad


# -----------------------------
# Unanswerable handling
# -----------------------------
UNANSWERABLE_RE = re.compile(
    "|".join([r"資料中尚未包含", r"資料未提供", r"無法回答", r"我不知道", r"我無法", r"沒有相關資料", r"找不到", r"無法確認"])
)


def looks_unanswerable(answer: str, strong_source: bool = False) -> bool:
    a = (answer or "").strip()
    if not a:
        return True
    if UNANSWERABLE_RE.search(a):
        return True
    if (not strong_source) and len(a) < 40:
        return True
    has_bullets = ("-" in a) or ("•" in a) or re.search(r"\n\s*\d+\.", a) is not None
    has_keywords = any(k in a for k in ["定位", "適用", "不適用", "規格", "功能", "建議", "原因", "方式", "流程", "差異"])
    if (not has_bullets) and (not has_keywords) and (not strong_source) and len(a) < 120:
        return True
    return False


def append_contact_if_needed(answer: str, strong_source: bool = False) -> str:
    if not Config.APPEND_CONTACT_ON_UNANSWERABLE:
        return answer
    if not looks_unanswerable(answer, strong_source=strong_source):
        return answer
    _, parsed = load_company_info(force=False)
    phone = (parsed.get("phone") or "").strip()
    if not phone or phone in answer:
        return answer
    return answer.rstrip() + "\n\n---\n如果需要更精準的協助，建議直接來電洽詢：\n📞 尚澤光電電話：" + phone + "\n"


# -----------------------------
# Helpers: log tail
# -----------------------------
def tail_file(path: str, lines: int = 200, rid: str = "") -> str:
    # ✅ (1) 支援 rid 過濾
    if not os.path.isfile(path):
        return ""
    lines = max(1, min(lines, Config.ADMIN_LOG_TAIL_MAX))
    rid = (rid or "").strip()

    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        block = 8192
        data = b""
        # 多抓一些，因為等下可能要 filter，避免 filtered 後不足
        while size > 0 and data.count(b"\n") < (lines * 3 + 50):
            step = block if size >= block else size
            size -= step
            f.seek(size)
            data = f.read(step) + data

    text = data.decode("utf-8", errors="ignore")
    arr = text.splitlines()

    if rid:
        arr = [ln for ln in arr if rid in ln]

    return "\n".join(arr[-lines:])


# -----------------------------
# ✅ Build system_docs + company_info BM25 docs
# -----------------------------
def collect_system_docs_for_bm25() -> List[Tuple[str, str, str]]:
    docs: List[Tuple[str, str, str]] = []

    if Config.COMPANY_INFO_PATH and os.path.isfile(Config.COMPANY_INFO_PATH):
        try:
            t = read_text_file(Config.COMPANY_INFO_PATH)
            docs.append(("company_info", "company_info.md", t))
        except Exception:
            pass

    base = Config.SYSTEM_DOCS_DIR
    if base and os.path.isdir(base):
        for fn in sorted(os.listdir(base)):
            if not (fn.lower().endswith(".md") or fn.lower().endswith(".txt")):
                continue
            p = os.path.join(base, fn)
            if not os.path.isfile(p):
                continue
            try:
                t = read_text_file(p)
                docs.append((f"system:{fn}", fn, t))
            except Exception:
                continue

    return docs


def build_system_docs_context_fallback(
    system_bm25: BM25CorpusIndex,
    user_text: str,
    topn: int = 4,
) -> List[str]:
    if not Config.SYSTEM_BM25_ENABLED:
        return []
    hits = system_bm25.search(user_text, topn=topn)
    blocks: List[str] = []
    for h in hits:
        src = h.get("source") or h.get("doc_id") or "system_docs"
        text = truncate_text((h.get("text") or "").strip(), 1200)
        if text:
            blocks.append(f"【系統文件(BM25)：{src}】\n{text}")
    return blocks


# -----------------------------
# App factory
# -----------------------------
def create_app() -> Flask:
    app = Flask(__name__, static_folder=Config.STATIC_DIR, static_url_path="")
    app.config["MAX_CONTENT_LENGTH"] = Config.MAX_UPLOAD_MB * 1024 * 1024
    CORS(app)

    os.environ.setdefault("PRODUCT_STRUCTURED_DIR", Config.PRODUCT_STRUCTURED_DIR)
    os.environ.setdefault("SYSTEM_DOCS_DIR", Config.SYSTEM_DOCS_DIR)
    os.environ.setdefault("COMPANY_INFO_PATH", Config.COMPANY_INFO_PATH)
    os.environ.setdefault("CHROMA_COLLECTION", Config.COLLECTION_NAME)

    ensure_dir(Config.SESSIONS_DIR)
    ensure_dir(Config.PRODUCT_STRUCTURED_DIR)

    vdb = VectorDatabase(db_path=Config.DB_PATH, collection_name=Config.COLLECTION_NAME)

    vdb_startup: Dict[str, Any] = {"ok": False, "action": "unknown"}
    try:
        if hasattr(vdb, "ensure_fresh"):
            vdb_startup = vdb.ensure_fresh()
            logger.info(f"VDB ensure_fresh startup: {vdb_startup}")
        else:
            vdb_startup = {"ok": False, "warning": "VectorDatabase.ensure_fresh not found"}
            logger.warning(vdb_startup["warning"])
    except Exception as e:
        vdb_startup = {"ok": False, "error": str(e)}
        logger.warning(f"VDB ensure_fresh failed: {e}")

    md_index = ProductMDIndex(Config.PRODUCT_STRUCTURED_DIR)
    load_company_info(force=True)

    system_bm25 = BM25CorpusIndex(k1=Config.BM25_K1, b=Config.BM25_B)
    try:
        docs = collect_system_docs_for_bm25()
        if Config.SYSTEM_BM25_ENABLED and docs:
            system_bm25.build(docs, max_chars=Config.SYSTEM_BM25_TEXT_MAX_CHARS)
            logger.info(f"BM25(system) ready: docs={len(docs)}")
        else:
            logger.info("BM25(system) disabled or no docs")
    except Exception as e:
        logger.warning(f"BM25(system) build failed: {e}")

    # ---------- Pages ----------
    @app.get("/")
    def index():
        return send_from_directory(Config.STATIC_DIR, Config.INDEX_FILE)

    @app.get("/admin")
    def admin_page():
        return send_from_directory(Config.STATIC_DIR, Config.ADMIN_FILE)

    @app.get("/favicon.ico")
    def favicon():
        return ("", 204)

    @app.get("/images/<path:filename>")
    def images(filename: str):
        if not filename:
            abort(404)
        fn = unquote(filename.replace("\\", "/"))
        if fn.startswith("/") or fn.startswith("~") or ":" in fn:
            abort(400)
        parts = [p for p in fn.split("/") if p]
        if any(p == ".." for p in parts):
            abort(400)
        safe_path = safe_join(Config.IMAGE_DIR, fn)
        if not safe_path:
            abort(400)
        return send_from_directory(Config.IMAGE_DIR, fn)

    # ---------- Health / Info ----------
    @app.get("/api/health")
    def api_health():
        text, _ = load_company_info(force=False)
        fp = ""
        try:
            if hasattr(vdb, "get_manifest_fingerprint"):
                fp = vdb.get_manifest_fingerprint()
        except Exception:
            fp = ""
        return jsonify(
            {
                "ok": True,
                "timestamp": now_iso(),
                "prompt_version": Config.PROMPT_VERSION,
                "db_path": Config.DB_PATH,
                "images_dir": Config.IMAGE_DIR,
                "product_structured_dir": Config.PRODUCT_STRUCTURED_DIR,
                "system_docs_dir": Config.SYSTEM_DOCS_DIR,
                "company_info_path": Config.COMPANY_INFO_PATH,
                "company_info_loaded": bool(text),
                "ollama_url": Config.OLLAMA_URL,
                "models": {"intent": Config.OLLAMA_MODEL_INTENT, "answer": Config.OLLAMA_MODEL_ANSWER},
                "ollama_keep_alive": Config.OLLAMA_KEEP_ALIVE,
                "reload_token_enabled": bool(Config.RELOAD_TOKEN),
                "admin_token_enabled": bool(Config.ADMIN_TOKEN),
                "vdb_startup": vdb_startup,
                "vdb_fingerprint": fp,
                "ollama_max_concurrent": Config.OLLAMA_MAX_CONCURRENT,
                "ollama_queue_timeout": Config.OLLAMA_QUEUE_TIMEOUT,
                "strict_decision_allowlist": Config.STRICT_DECISION_ALLOWLIST,
                "known_models_count": len(md_index.known_models),
                "bm25_enabled": Config.BM25_ENABLED,
                "system_bm25_enabled": Config.SYSTEM_BM25_ENABLED,
                "fusion_enabled": Config.FUSION_ENABLED,
            }
        )

    @app.get("/api/health/deps")
    def api_health_deps():
        out = {
            "ok": True,
            "timestamp": now_iso(),
            "ollama": False,
            "chroma": False,
            "db": False,
            "errors": [],
        }

        try:
            r = HTTP.get(f"{Config.OLLAMA_URL}/api/tags", timeout=3)
            if r.ok:
                out["ollama"] = True
            else:
                out["errors"].append(f"ollama http={r.status_code}")
        except Exception as e:
            out["errors"].append(f"ollama: {e}")

        try:
            _ = vdb.collection.count()
            out["chroma"] = True
        except Exception as e:
            out["errors"].append(f"chroma: {e}")

        try:
            conn = sqlite3.connect(Config.DB_PATH, timeout=3)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            conn.close()
            out["db"] = True
        except Exception as e:
            out["errors"].append(f"db: {e}")

        if not (out["ollama"] and out["chroma"] and out["db"]):
            out["ok"] = False

        return jsonify(out)

    @app.get("/api/company-info")
    def api_company_info():
        text, parsed = load_company_info(force=False)
        return jsonify({"ok": True, "timestamp": now_iso(), "path": Config.COMPANY_INFO_PATH, "parsed": parsed, "raw": text})

    # ---------- Sessions ----------
    @app.post("/api/clear")
    def api_clear():
        payload = request.get_json(force=True, silent=True) or {}
        session_id = (payload.get("session_id") or "").strip() or "default"
        p = session_path(session_id)
        try:
            if os.path.isfile(p):
                os.remove(p)
        except Exception:
            pass
        return jsonify({"ok": True, "session_id": session_id})

    # ---------- Cancel ----------
    @app.post("/api/cancel")
    def api_cancel():
        payload = request.get_json(force=True, silent=True) or {}
        rid = (payload.get("rid") or "").strip()
        if not rid:
            return jsonify({"ok": False, "error": "missing rid"}), 400
        ok = _cancel(rid)
        return jsonify({"ok": True, "rid": rid, "cancelled": bool(ok)})

    # ---------- Reload / Rebuild ----------
    @app.post("/api/reload-data")
    def api_reload_data():
        require_reload_token()

        t0 = time.time()
        payload = request.get_json(force=True, silent=True) or {}
        full_rebuild = bool(payload.get("full_rebuild", True))

        with REBUILD_LOCK:
            try:
                if full_rebuild:
                    stats = vdb.rebuild_full()
                else:
                    stats = vdb.ensure_fresh()

                md_index.rebuild()
                load_company_info(force=True)

                try:
                    docs = collect_system_docs_for_bm25()
                    if Config.SYSTEM_BM25_ENABLED and docs:
                        system_bm25.build(docs, max_chars=Config.SYSTEM_BM25_TEXT_MAX_CHARS)
                except Exception as e:
                    logger.warning(f"reload: BM25(system) rebuild failed: {e}")

                elapsed = int((time.time() - t0) * 1000)
                return jsonify({"ok": True, "elapsed_ms": elapsed, "full_rebuild": full_rebuild, "stats": stats, "known_models": len(md_index.known_models)})
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)}), 500

    # ---------- Admin APIs ----------
    @app.get("/api/admin/info")
    def api_admin_info():
        require_admin_or_reload()

        md_files: List[Dict[str, Any]] = []
        if os.path.isdir(Config.PRODUCT_STRUCTURED_DIR):
            for fn in sorted(os.listdir(Config.PRODUCT_STRUCTURED_DIR)):
                if not fn.lower().endswith(".md"):
                    continue
                p = os.path.join(Config.PRODUCT_STRUCTURED_DIR, fn)
                try:
                    st = os.stat(p)
                    md_files.append({"name": fn, "size": int(st.st_size), "mtime": int(st.st_mtime)})
                except Exception:
                    md_files.append({"name": fn, "size": 0, "mtime": 0})

        files = [x["name"] for x in md_files]

        vdb_sources = []
        if hasattr(vdb, "list_sources"):
            try:
                vdb_sources = vdb.list_sources()
            except Exception as e:
                vdb_sources = [{"error": str(e)}]

        fp = ""
        try:
            fp = vdb.get_manifest_fingerprint()
        except Exception:
            fp = ""

        return jsonify(
            {
                "ok": True,
                "timestamp": now_iso(),
                "prompt_version": Config.PROMPT_VERSION,
                "files": files,
                "md_files": md_files,
                "vdb_sources": vdb_sources,
                "vdb_fingerprint": fp,
                "known_models_count": len(md_index.known_models),
                "bm25_enabled": Config.BM25_ENABLED,
                "system_bm25_enabled": Config.SYSTEM_BM25_ENABLED,
                "fusion_enabled": Config.FUSION_ENABLED,
            }
        )

    @app.get("/api/admin/status")
    def api_admin_status():
        require_admin_or_reload()
        fp = ""
        try:
            fp = vdb.get_manifest_fingerprint()
        except Exception:
            fp = ""
        return jsonify(
            {
                "ok": True,
                "timestamp": now_iso(),
                "collection": Config.COLLECTION_NAME,
                "db_path": Config.DB_PATH,
                "chroma_dir": os.getenv("CHROMA_DIR", "chroma_db"),
                "startup": vdb_startup,
                "vdb_fingerprint": fp,
                "models": {"intent": Config.OLLAMA_MODEL_INTENT, "answer": Config.OLLAMA_MODEL_ANSWER},
                "known_models_count": len(md_index.known_models),
                "bm25_enabled": Config.BM25_ENABLED,
                "system_bm25_enabled": Config.SYSTEM_BM25_ENABLED,
                "fusion_enabled": Config.FUSION_ENABLED,
            }
        )

    @app.get("/api/admin/log-tail")
    def api_admin_log_tail():
        require_admin_or_reload()
        n = request.args.get("n", "").strip()
        rid = (request.args.get("rid") or "").strip()  # ✅ (1) rid filter

        try:
            n2 = int(n) if n else Config.ADMIN_LOG_TAIL_DEFAULT
        except Exception:
            n2 = Config.ADMIN_LOG_TAIL_DEFAULT

        text = tail_file(LOG_FILE, lines=n2, rid=rid)
        return jsonify({"ok": True, "timestamp": now_iso(), "file": LOG_FILE, "lines": n2, "rid": rid, "text": text})

    @app.post("/api/admin/md-upload")
    def api_admin_md_upload():
        require_admin_or_reload()

        if "file" not in request.files:
            return jsonify({"ok": False, "error": "missing file"}), 400

        f = request.files["file"]
        if not f or not f.filename:
            return jsonify({"ok": False, "error": "bad file"}), 400

        override_name = (request.form.get("filename") or "").strip()
        raw_name = override_name if override_name else f.filename

        filename = secure_filename(raw_name)
        if not filename.lower().endswith(".md"):
            return jsonify({"ok": False, "error": "only .md allowed"}), 400
        if not filename.startswith("product_"):
            filename = "product_" + filename

        ensure_dir(Config.PRODUCT_STRUCTURED_DIR)
        dst = os.path.join(Config.PRODUCT_STRUCTURED_DIR, filename)

        content = f.read()
        if not content:
            return jsonify({"ok": False, "error": "empty file"}), 400

        with open(dst, "wb") as w:
            w.write(content)

        md_index.rebuild()

        auto = (request.args.get("auto_reindex") or "").strip()
        did_reindex = False
        reindex_result = None
        if auto in ("1", "true", "yes"):
            try:
                with REBUILD_LOCK:
                    reindex_result = vdb.ensure_fresh()
                    did_reindex = True
            except Exception as e:
                reindex_result = {"ok": False, "error": str(e)}

        return jsonify(
            {
                "ok": True,
                "saved_as": filename,
                "auto_reindex": did_reindex,
                "reindex_result": reindex_result,
                "known_models_count": len(md_index.known_models),
                "bm25_enabled": Config.BM25_ENABLED,
                "system_bm25_enabled": Config.SYSTEM_BM25_ENABLED,
                "fusion_enabled": Config.FUSION_ENABLED,
            }
        )

    # -----------------------------
    # Chat
    # -----------------------------
    @app.post("/api/chat")
    def api_chat():
        t0 = time.time()

        payload = request.get_json(force=True, silent=True) or {}
        user_text = (payload.get("message") or "").strip()
        session_id = (payload.get("session_id") or payload.get("sid") or "default").strip()
        rid = (payload.get("rid") or "").strip() or uuid.uuid4().hex[:10]
        if not user_text:
            return jsonify({"error": "missing message"}), 400

        answer_mode = pick_answer_mode(payload)
        topk = Config.TOPK_FAST if answer_mode == "FAST" else Config.TOPK_NORMAL

        sess = load_session(session_id)
        sess_msgs = sess.get("messages", [])
        sess_meta = sess.get("meta") or {}
        decision_state = sess.get("decision")

        now_ms = int(time.time() * 1000)
        last_ms = int(sess_meta.get("last_user_ms") or 0)
        if last_ms and (now_ms - last_ms) < Config.MIN_REQUEST_INTERVAL_MS:
            return (
                jsonify(
                    {
                        "response": "⏳ 你輸入得太快了，我正在處理上一個請求。請稍等 1 秒再送出。",
                        "intent": "RATE_LIMIT",
                        "answer_mode": answer_mode,
                        "elapsed_ms": int((time.time() - t0) * 1000),
                        "timestamp": now_iso(),
                        "session_id": session_id,
                        "rid": rid,
                        "prompt_version": Config.PROMPT_VERSION,
                    }
                ),
                429,
            )
        sess_meta["last_user_ms"] = now_ms

        vdb_fp = ""
        try:
            vdb_fp = vdb.get_manifest_fingerprint()
        except Exception:
            vdb_fp = ""

        norm_q = normalize_question_for_cache(user_text)
        if Config.ENABLE_SESSION_CACHE:
            cache_q = (sess_meta.get("cache_q") or "").strip()
            cache_fp = (sess_meta.get("cache_fp") or "").strip()
            cache_ans = (sess_meta.get("cache_ans") or "").strip()
            if cache_q and cache_ans and (cache_q == norm_q) and (cache_fp == vdb_fp):
                logger.info(f"[{rid}] cache hit session={session_id}")
                sess["meta"] = sess_meta
                save_session(session_id, sess)
                return jsonify(
                    {
                        "response": cache_ans,
                        "intent": "CACHED",
                        "answer_mode": answer_mode,
                        "elapsed_ms": int((time.time() - t0) * 1000),
                        "timestamp": now_iso(),
                        "session_id": session_id,
                        "rid": rid,
                        "prompt_version": Config.PROMPT_VERSION,
                        "vdb_fingerprint": vdb_fp,
                        "cached": True,
                        "relevant_docs": [],
                    }
                )

        if not decision_state and decision.should_enter_decision(user_text):
            decision_state = decision.new_state()

        if decision_state and decision_state.get("active"):
            decision_state, assistant_text = decision.advance(decision_state, user_text)
            sess["decision"] = decision_state

            sess_msgs.append({"role": "user", "content": user_text, "ts": now_iso()})
            sess_msgs.append({"role": "assistant", "content": assistant_text, "ts": now_iso()})
            sess["messages"] = sess_msgs[-(Config.SESSION_MAX_TURNS * 2) :]
            sess["meta"] = sess_meta
            save_session(session_id, sess)

            return jsonify(
                {
                    "response": assistant_text,
                    "intent": "DECISION",
                    "answer_mode": answer_mode,
                    "elapsed_ms": int((time.time() - t0) * 1000),
                    "timestamp": now_iso(),
                    "session_id": session_id,
                    "rid": rid,
                    "prompt_version": Config.PROMPT_VERSION,
                    "decision": decision_state,
                    "relevant_docs": [build_company_card()] if build_company_card() else [],
                }
            )

        extra_terms: List[str] = []
        if decision_state and decision_state.get("finished"):
            extra_terms = decision.build_recommendation_query_terms(decision_state)
            sess["decision"] = None

        intent = classify_intent(user_text, md_index=md_index)

        if extra_terms:
            intent = "DECISION_RESULT"
            user_text_for_search = user_text + "\n\n（需求摘要：" + " / ".join(extra_terms) + "）"
        else:
            user_text_for_search = user_text

        relevant_docs: List[Dict[str, Any]] = []
        context_blocks: List[str] = []
        query_terms: List[str] = []
        used_md_files: List[str] = []
        bm25_models: List[str] = []
        rag_docs_for_fusion: List[Dict[str, Any]] = []
        fusion_meta: Dict[str, Any] = {}

        logger.info(f"[{rid}] chat start intent={intent} mode={answer_mode} topk={topk} session={session_id}")

        with sqlite3.connect(Config.DB_PATH, timeout=30) as conn:
            conn.row_factory = sqlite3.Row

            # ✅ (3) SQLite PRAGMA：讀取效能 & 降低鎖競爭（讀多寫少很適合）
            try:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
                conn.execute("PRAGMA temp_store=MEMORY;")
                conn.execute("PRAGMA cache_size=-20000;")  # ~20MB
            except Exception:
                pass

            if intent in ("PRODUCT_SPEC", "DECISION_RESULT"):
                (
                    context_blocks,
                    relevant_docs,
                    query_terms,
                    used_md_files,
                    bm25_models,
                    rag_docs_for_fusion,
                ) = build_product_context_priority_md(
                    user_text=user_text_for_search,
                    vdb=vdb,
                    conn=conn,
                    topk=topk,
                    md_index=md_index,
                    extra_terms=extra_terms,
                )
            elif intent == "COMPANY_INFO":
                context_blocks = build_company_info_context(user_text)
                cc = build_company_card()
                relevant_docs = [ensure_card_images(cc)] if cc else []
            else:
                rag = vdb_search(vdb, user_text, top_k=topk)
                rag = rag_dedupe_and_cap(rag, per_source_max=max(1, Config.RAG_PER_SOURCE_MAX))[: min(Config.RAG_MAX_BLOCKS, topk)]
                for d in rag:
                    txt = truncate_text((d.get("text") or "").strip(), Config.RAG_CHUNK_MAX_CHARS)
                    meta = d.get("metadata") or {}
                    src = meta.get("source") or meta.get("file") or ""
                    if txt:
                        context_blocks.append(f"【資料片段：{src}】\n{txt}")

                if (not context_blocks) or len(context_blocks) < 2:
                    context_blocks.extend(build_system_docs_context_fallback(system_bm25, user_text, topn=Config.SYSTEM_BM25_TOPN))

        if not context_blocks:
            context_blocks = build_system_docs_context_fallback(system_bm25, user_text, topn=Config.SYSTEM_BM25_TOPN)
        if not context_blocks:
            context_blocks = ["（系統找不到可用的公司資料片段。請確認已匯入向量庫或產品資料。）"]

        rag_blocks_used = len([c for c in context_blocks if c.startswith("【向量庫片段：") or c.startswith("【資料片段：")])
        db_blocks_used = len([c for c in context_blocks if c.startswith("【DB 產品摘要：")])
        strong_source = bool(used_md_files) or (rag_blocks_used > 0) or (db_blocks_used > 0)

        allowlist_picks: List[str] = []
        if intent in ("PRODUCT_SPEC", "DECISION_RESULT") and Config.STRICT_DECISION_ALLOWLIST:
            base_allow = build_allowlist_from_sources(relevant_docs, used_md_files)

            alias_terms = expand_by_alias(user_text_for_search)
            model_key = extract_model_key(user_text_for_search)
            search_queries = build_search_queries(user_text_for_search, alias_terms=alias_terms, extra_terms=extra_terms, model_key=model_key)

            fused_allow, fusion_meta = build_fused_allowlist(
                md_index=md_index,
                vdb=vdb,
                user_text=user_text_for_search,
                extra_terms=extra_terms,
                search_queries=search_queries,
                bm25_models=bm25_models,
                rag_docs=rag_docs_for_fusion,
            )

            merged = []
            seen = set()
            for x in (base_allow + fused_allow):
                x = (x or "").strip()
                if not x:
                    continue
                k = x.lower()
                if k in seen:
                    continue
                seen.add(k)
                merged.append(x)

            if (not merged) and Config.FUSION_FORCE_NONEMPTY and md_index.known_models:
                merged = md_index.known_models[: Config.ALLOWLIST_MAX_ITEMS]
                fusion_meta = fusion_meta or {}
                fusion_meta["forced_allow_all"] = True

            allowlist_picks = merged[: Config.ALLOWLIST_MAX_ITEMS]

        def _builder(blocks_now: List[str]) -> str:
            return build_answer_prompt(
                user_text=user_text_for_search,
                intent=intent,
                context_blocks=blocks_now,
                answer_mode=answer_mode,
                session_messages=sess_msgs,
                allowlist_picks=allowlist_picks,
            )

        context_blocks2, prompt = prompt_guard_trim(context_blocks, _builder, Config.MAX_PROMPT_CHARS)

        cancel_event = _register_cancel(rid)

        acquired = OLLAMA_SEM.acquire(timeout=Config.OLLAMA_QUEUE_TIMEOUT)
        if not acquired:
            _pop_cancel(rid)
            logger.warning(f"[{rid}] busy: queue timeout")
            return (
                jsonify(
                    {
                        "response": "⚠️ 系統目前忙碌中（同時詢問人數較多）。請稍後 5~10 秒再試一次。",
                        "intent": "BUSY",
                        "answer_mode": answer_mode,
                        "elapsed_ms": int((time.time() - t0) * 1000),
                        "timestamp": now_iso(),
                        "session_id": session_id,
                        "rid": rid,
                        "prompt_version": Config.PROMPT_VERSION,
                    }
                ),
                503,
            )

        ollama_ms = 0
        ans = ""
        try:
            t_ollama = time.time()
            ans = ollama_generate_cancelable(
                prompt=prompt,
                model=Config.OLLAMA_MODEL_ANSWER,
                cancel_event=cancel_event,
                temperature=0.2 if answer_mode == "NORMAL" else 0.1,
                num_predict=1100 if answer_mode == "NORMAL" else 520,
            )
            ollama_ms = int((time.time() - t_ollama) * 1000)
        except Exception as e:
            if "CANCELLED" in str(e):
                logger.info(f"[{rid}] cancelled")
                return jsonify({"error": "cancelled", "rid": rid}), 499
            logger.exception(f"[{rid}] ollama failed: {e}")
            return jsonify({"error": f"ollama_generate failed: {e}", "rid": rid}), 500
        finally:
            OLLAMA_SEM.release()
            _pop_cancel(rid)

        blocked = False
        bad_models: List[str] = []
        if intent in ("PRODUCT_SPEC", "DECISION_RESULT") and Config.STRICT_DECISION_ALLOWLIST:
            ans, blocked, bad_models = enforce_allowlist_or_block(ans, allowlist_picks)

        ans2 = append_contact_if_needed(ans, strong_source=strong_source)

        if looks_unanswerable(ans, strong_source=strong_source) and intent != "COMPANY_INFO":
            cc = build_company_card()
            if cc:
                relevant_docs = [ensure_card_images(cc)] + (relevant_docs or [])

        sess_msgs.append({"role": "user", "content": user_text, "ts": now_iso()})
        sess_msgs.append({"role": "assistant", "content": ans2, "ts": now_iso()})
        sess["messages"] = sess_msgs[-(Config.SESSION_MAX_TURNS * 2) :]
        sess_meta["cache_q"] = norm_q
        sess_meta["cache_ans"] = ans2
        sess_meta["cache_fp"] = vdb_fp
        sess["meta"] = sess_meta
        save_session(session_id, sess)

        elapsed_ms = int((time.time() - t0) * 1000)
        unans = looks_unanswerable(ans, strong_source=strong_source)

        logger.info(
            f"[{rid}] chat done intent={intent} elapsed_ms={elapsed_ms} ollama_ms={ollama_ms} "
            f"unanswerable={unans} allowlist={len(allowlist_picks)} blocked={blocked} bad_models={bad_models} "
            f"bm25_enabled={Config.BM25_ENABLED} system_bm25={Config.SYSTEM_BM25_ENABLED} fusion={Config.FUSION_ENABLED}"
        )

        return jsonify(
            {
                "response": ans2,
                "intent": intent,
                "answer_mode": answer_mode,
                "elapsed_ms": elapsed_ms,
                "ollama_ms": ollama_ms,
                "query_terms": query_terms,
                "allowlist_picks": allowlist_picks,
                "blocked": blocked,
                "bad_models": bad_models,
                "relevant_docs": relevant_docs,
                "answer_sources": {
                    "product_md": used_md_files,
                    "rag_blocks": rag_blocks_used,
                    "db_blocks": db_blocks_used,
                    "bm25_models": bm25_models[:10],
                    "fusion": fusion_meta,
                },
                "timestamp": now_iso(),
                "session_id": session_id,
                "rid": rid,
                "prompt_version": Config.PROMPT_VERSION,
                "vdb_fingerprint": vdb_fp,
                "cached": False,
            }
        )

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host=Config.HOST, port=Config.PORT, debug=Config.DEBUG)
