# vector_db.py (Incremental Chroma Updater + Manifest per-file + Scope + Status) - UPGRADED
import os
import re
import glob
import json
import time
import hashlib
import logging
import threading
from typing import Any, Dict, List, Optional, Tuple

import chromadb
from chromadb.utils import embedding_functions

logger = logging.getLogger("vector-db")


# -----------------------------
# IO
# -----------------------------
def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()


def chunk_text(text: str, chunk_size: int = 900, overlap: int = 120) -> List[str]:
    text = (text or "").strip()
    if not text:
        return []
    chunk_size = max(200, int(chunk_size))
    overlap = max(0, min(int(overlap), chunk_size - 50))

    chunks: List[str] = []
    i = 0
    n = len(text)
    while i < n:
        j = min(n, i + chunk_size)
        chunks.append(text[i:j])
        if j >= n:
            break
        i = max(0, j - overlap)
    return chunks


def _norm_path(p: str) -> str:
    # manifest keys / metadata source 統一用這個
    return os.path.normpath(p).replace("\\", "/")


def _safe_id(s: str) -> str:
    s = _norm_path(s)
    s = re.sub(r"[^a-zA-Z0-9_\-:./]+", "_", s)
    return s


# -----------------------------
# Fingerprint mode
# -----------------------------
def _file_fingerprint_stat(path: str) -> str:
    """
    ✅ 快速 fingerprint：hash(path + mtime + size)
    """
    p2 = _norm_path(path)
    try:
        st = os.stat(path)
        mtime = int(st.st_mtime)
        size = int(st.st_size)
    except Exception:
        mtime, size = 0, 0
    raw = f"{p2}\t{mtime}\t{size}".encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()


def _file_fingerprint_content(path: str, max_bytes: int = 32 * 1024 * 1024) -> str:
    """
    ✅ 內容 fingerprint：hash(file bytes)（較慢，但更準）
    - max_bytes: 避免超大檔案拖垮；超過就只 hash 前 max_bytes（可接受）
    """
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            remain = max_bytes
            while True:
                if remain <= 0:
                    break
                chunk = f.read(min(1024 * 1024, remain))
                if not chunk:
                    break
                h.update(chunk)
                remain -= len(chunk)
    except Exception:
        return _file_fingerprint_stat(path)

    # 同時混入路徑避免不同檔案同內容被誤判
    h.update(("\n" + _norm_path(path)).encode("utf-8", errors="ignore"))
    return h.hexdigest()


def _choose_fingerprint_fn():
    mode = (os.getenv("VDB_FINGERPRINT_MODE", "stat") or "stat").strip().lower()
    if mode in ("content", "sha", "sha256"):
        return _file_fingerprint_content
    return _file_fingerprint_stat


FINGERPRINT_FN = _choose_fingerprint_fn()


# -----------------------------
# Helpers: doc type / title
# -----------------------------
RE_MD_TITLE = re.compile(r"^\s*#{1,6}\s+(.+?)\s*$", re.MULTILINE)


def _guess_doc_type(norm_path: str, system_docs_dir: str, product_structured_dir: str, company_info_path: str) -> str:
    p = _norm_path(norm_path)
    sys_dir = _norm_path(system_docs_dir) if system_docs_dir else ""
    prod_dir = _norm_path(product_structured_dir) if product_structured_dir else ""
    comp_p = _norm_path(company_info_path) if company_info_path else ""

    if comp_p and p == comp_p:
        return "company"
    if prod_dir and p.startswith(prod_dir.rstrip("/") + "/"):
        return "product"
    if sys_dir and p.startswith(sys_dir.rstrip("/") + "/"):
        return "system"
    return "extra"


def _guess_title(base_name: str, doc_type: str, text_head: str) -> str:
    """
    - product: product_XXX.md -> XXX
    - company: company_info
    - system/extra: 第一個 markdown 標題，沒有就用檔名去副檔名
    """
    base = (base_name or "").strip()
    if doc_type == "company":
        return "company_info"

    if doc_type == "product" and base.startswith("product_") and base.lower().endswith(".md"):
        return base[len("product_") : -len(".md")].strip()

    m = RE_MD_TITLE.search(text_head or "")
    if m:
        t = (m.group(1) or "").strip()
        if t:
            return t

    # fallback: filename without ext
    t = re.sub(r"\.(md|txt)$", "", base, flags=re.IGNORECASE).strip()
    return t or base


# -----------------------------
# Vector DB
# -----------------------------
class VectorDatabase:
    """
    Chroma PersistentClient + SentenceTransformerEmbeddingFunction

    - search(query, top_k)
    - ensure_fresh(scope="all|product|system"): 增量更新（只更新變動檔案 / 刪除移除檔案）
    - rebuild_full(scope="all|product|system"): 完整刪掉 collection 後重建
    - list_sources(scope=...): 給 admin 後台列出來源清單
    - get_manifest_fingerprint(scope=...): 給 app.py cache version 用（整體 fingerprint）
    - get_vdb_status(): running/dirty/last_error（給後台顯示狀態）
    """

    def __init__(self, db_path: str, collection_name: str = "company_knowledge"):
        self.db_path = db_path
        self.collection_name = collection_name

        self.persist_dir = os.getenv("CHROMA_DIR", "chroma_db")
        os.makedirs(self.persist_dir, exist_ok=True)

        self.client = chromadb.PersistentClient(path=self.persist_dir)

        self.embedding_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name=os.getenv("EMBED_MODEL", "all-MiniLM-L6-v2")
        )

        self.collection = self.client.get_or_create_collection(
            name=self.collection_name,
            embedding_function=self.embedding_fn,
            metadata={"hnsw:space": "cosine"},
        )

        self.chunk_size = int(os.getenv("RAG_CHUNK_SIZE", "900"))
        self.chunk_overlap = int(os.getenv("RAG_CHUNK_OVERLAP", "120"))

        # status / lock
        self._lock = threading.Lock()
        self._op_lock = threading.Lock()  # ✅ prevent concurrent ensure/rebuild

        self._running = False
        self._dirty = True  # 初始視為 dirty（尚未 build）
        self._last_error = ""

        # 允許自訂額外掃描路徑（預設不掃整個 data，避免雜訊）
        # e.g. VDB_EXTRA_ROOTS="data/extra_docs;data/policies"
        self._extra_roots = [p.strip() for p in (os.getenv("VDB_EXTRA_ROOTS", "") or "").split(";") if p.strip()]

    # -----------------------------
    # Status
    # -----------------------------
    def mark_dirty(self, v: bool = True) -> None:
        with self._lock:
            self._dirty = bool(v)

    def get_vdb_status(self) -> Dict[str, Any]:
        with self._lock:
            return {"running": bool(self._running), "dirty": bool(self._dirty), "last_error": self._last_error or ""}

    def _set_running(self, v: bool) -> None:
        with self._lock:
            self._running = bool(v)

    def _set_error(self, msg: str) -> None:
        with self._lock:
            self._last_error = (msg or "").strip()

    def get_collection_count(self) -> int:
        try:
            return int(self.collection.count())
        except Exception:
            return 0

    # -----------------------------
    # Query
    # -----------------------------
    def search(self, query: str, top_k: int = 6) -> List[Dict[str, Any]]:
        query = (query or "").strip()
        if not query:
            return []
        res = self.collection.query(
            query_texts=[query],
            n_results=int(top_k),
            include=["documents", "metadatas", "distances"],
        )
        docs = res.get("documents", [[]])[0] or []
        metas = res.get("metadatas", [[]])[0] or []
        dists = res.get("distances", [[]])[0] or []

        out: List[Dict[str, Any]] = []
        for txt, meta, dist in zip(docs, metas, dists):
            out.append({"text": txt, "metadata": meta or {}, "distance": dist})
        return out

    # -----------------------------
    # Source roots & collection (scoped)
    # -----------------------------
    def _paths_from_env(self) -> Tuple[str, str, str]:
        product_structured_dir = os.getenv("PRODUCT_STRUCTURED_DIR", "data/product_structured")
        system_docs_dir = os.getenv("SYSTEM_DOCS_DIR", "data/system_docs")
        company_info_path = os.getenv("COMPANY_INFO_PATH", "data/company_info.md")
        return system_docs_dir, product_structured_dir, company_info_path

    def _source_roots(self, scope: str = "all") -> Tuple[List[str], List[str]]:
        """
        scope:
          - all: system_docs + product_structured + company_info (+ extra roots)
          - product: product_structured (+ extra roots if you want)
          - system: system_docs + company_info (+ extra roots if you want)
        """
        scope = (scope or "all").strip().lower()
        system_docs_dir, product_structured_dir, company_info_path = self._paths_from_env()

        roots: List[str] = []
        extra_files: List[str] = []

        if scope in ("all", "system"):
            if system_docs_dir and os.path.isdir(system_docs_dir):
                roots.append(system_docs_dir)

        if scope in ("all", "product"):
            if product_structured_dir and os.path.isdir(product_structured_dir):
                roots.append(product_structured_dir)

        # company_info 永遠視為 system 類（但 all 也要包含）
        if scope in ("all", "system"):
            if company_info_path:
                if os.path.isfile(company_info_path):
                    extra_files.append(company_info_path)

        # optional extra roots
        for r in self._extra_roots:
            if os.path.isdir(r):
                roots.append(r)

        return roots, extra_files

    def _collect_sources(self, scope: str = "all") -> List[Tuple[str, str]]:
        """
        回傳 (real_path, norm_path)
        - manifest/metadata 只用 norm_path
        - 讀檔用 real_path（Windows 路徑更穩）
        """
        roots, extra_files = self._source_roots(scope=scope)

        files: List[str] = []
        for r in roots:
            if not r or (not os.path.isdir(r)):
                continue
            files += glob.glob(os.path.join(r, "**/*.md"), recursive=True)
            files += glob.glob(os.path.join(r, "**/*.txt"), recursive=True)

        files += extra_files

        uniq: List[Tuple[str, str]] = []
        seen = set()
        for p in files:
            if not p:
                continue
            rp = os.path.normpath(p)
            if rp in seen:
                continue
            if os.path.isfile(rp):
                seen.add(rp)
                uniq.append((rp, _norm_path(rp)))

        uniq.sort(key=lambda x: x[1])
        return uniq

    def list_sources(self, scope: str = "all") -> List[Dict[str, Any]]:
        sources = self._collect_sources(scope=scope)
        out: List[Dict[str, Any]] = []
        for real_p, norm_p in sources:
            try:
                st = os.stat(real_p)
                out.append({"path": norm_p, "mtime": int(st.st_mtime), "size": int(st.st_size)})
            except Exception:
                out.append({"path": norm_p, "mtime": 0, "size": 0})
        return out

    # -----------------------------
    # Manifest (scoped)
    # -----------------------------
    def manifest_path(self, scope: str = "all") -> str:
        scope = (scope or "all").strip().lower()
        return os.path.join(self.persist_dir, f"{self.collection_name}__manifest__{scope}.json")

    def _read_manifest(self, scope: str = "all") -> Dict[str, Any]:
        p = self.manifest_path(scope=scope)
        if not os.path.isfile(p):
            return {}
        try:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f) or {}
        except Exception:
            return {}

    def _write_manifest(self, data: Dict[str, Any], scope: str = "all") -> None:
        p = self.manifest_path(scope=scope)
        try:
            with open(p, "w", encoding="utf-8") as f:
                json.dump(data or {}, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"write manifest failed: {e}")

    def get_manifest_fingerprint(self, scope: str = "all") -> str:
        m = self._read_manifest(scope=scope)
        return (m.get("fingerprint") or "").strip()

    def _build_global_fingerprint(self, per_file: Dict[str, Any]) -> str:
        items = []
        for k in sorted(per_file.keys()):
            v = per_file.get(k) or {}
            items.append(f"{k}\t{v.get('fp','')}\t{v.get('chunks',0)}")
        raw = "\n".join(items).encode("utf-8", errors="ignore")
        return hashlib.sha256(raw).hexdigest()

    # -----------------------------
    # Incremental ensure (scoped)
    # -----------------------------
    def ensure_fresh(self, scope: str = "all") -> Dict[str, Any]:
        """
        ✅ 增量更新策略（scope）
        - 讀 manifest（每個檔案 fp + chunk_count）
        - 重新計算每個檔案 fp
        - changed -> delete where source=... -> add new chunks
        - removed -> delete where source=...
        - unchanged -> skip
        """
        scope = (scope or "all").strip().lower()
        t0 = time.time()

        with self._op_lock:
            self._set_running(True)
            self._set_error("")

            try:
                system_docs_dir, product_structured_dir, company_info_path = self._paths_from_env()

                sources = self._collect_sources(scope=scope)
                norm_to_real = {norm_p: real_p for (real_p, norm_p) in sources}
                sources_norm = [norm_p for (_, norm_p) in sources]

                old = self._read_manifest(scope=scope)
                old_files: Dict[str, Any] = old.get("files") or {}

                # compute new per-file fp
                new_files: Dict[str, Any] = {}
                for norm_p in sources_norm:
                    real_p = norm_to_real.get(norm_p, norm_p)
                    new_files[norm_p] = {"fp": FINGERPRINT_FN(real_p), "chunks": int(old_files.get(norm_p, {}).get("chunks", 0) or 0)}

                removed = [p for p in old_files.keys() if p not in new_files]
                changed: List[str] = []
                unchanged = 0

                for norm_p, info in new_files.items():
                    old_fp = (old_files.get(norm_p, {}) or {}).get("fp") or ""
                    if old_fp and old_fp == info["fp"]:
                        unchanged += 1
                    else:
                        changed.append(norm_p)

                added_chunks = 0
                skipped_files = 0
                doc_type_counter = {"product": 0, "system": 0, "company": 0, "extra": 0}

                # delete removed sources
                for norm_p in removed:
                    try:
                        self.collection.delete(where={"source": norm_p})
                    except Exception:
                        pass

                batch = int(os.getenv("CHROMA_ADD_BATCH", "256"))
                batch = max(64, min(batch, 1024))

                for norm_p in changed:
                    real_p = norm_to_real.get(norm_p, norm_p)
                    try:
                        # delete old chunks for this source
                        try:
                            self.collection.delete(where={"source": norm_p})
                        except Exception:
                            pass

                        text = read_text(real_p)
                        chunks = chunk_text(text, chunk_size=self.chunk_size, overlap=self.chunk_overlap)
                        if not chunks:
                            skipped_files += 1
                            new_files[norm_p]["chunks"] = 0
                            continue

                        base = os.path.basename(real_p)
                        head = text[:4000] if text else ""
                        doc_type = _guess_doc_type(norm_p, system_docs_dir, product_structured_dir, company_info_path)
                        title = _guess_title(base, doc_type, head)

                        doc_type_counter[doc_type] = doc_type_counter.get(doc_type, 0) + 1

                        ids: List[str] = []
                        docs: List[str] = []
                        metas: List[Dict[str, Any]] = []

                        for idx, ch in enumerate(chunks):
                            doc_id = _safe_id(f"{norm_p}::chunk{idx}")
                            ids.append(doc_id)
                            docs.append(ch)
                            metas.append(
                                {
                                    "source": norm_p,            # ✅ stable key
                                    "file": base,
                                    "title": title,              # ✅ now for system docs too
                                    "chunk": idx,
                                    "doc_type": doc_type,        # ✅ product/system/company/extra
                                    "scope_call": scope,         # ✅ keep caller scope for debug
                                    "relpath": norm_p,           # ✅ alias for debugging
                                }
                            )

                        for i in range(0, len(ids), batch):
                            self.collection.add(
                                ids=ids[i : i + batch],
                                documents=docs[i : i + batch],
                                metadatas=metas[i : i + batch],
                            )

                        new_files[norm_p]["chunks"] = len(chunks)
                        added_chunks += len(chunks)

                    except Exception as e:
                        skipped_files += 1
                        logger.warning(f"ensure_fresh skip {norm_p}: {e}")

                global_fp = self._build_global_fingerprint(new_files)

                manifest = {
                    "collection": self.collection_name,
                    "scope": scope,
                    "fingerprint": global_fp,
                    "rebuilt_at": int(time.time()),
                    "mode": "incremental",
                    "fingerprint_mode": os.getenv("VDB_FINGERPRINT_MODE", "stat"),
                    "files": new_files,
                    "stats": {
                        "sources_total": len(sources_norm),
                        "unchanged_files": unchanged,
                        "changed_files": len(changed),
                        "removed_files": len(removed),
                        "added_chunks": added_chunks,
                        "deleted_removed_files": len(removed),
                        "skipped_files": skipped_files,
                        "doc_type_files_touched": doc_type_counter,
                        "chunk_size": self.chunk_size,
                        "chunk_overlap": self.chunk_overlap,
                    },
                }
                self._write_manifest(manifest, scope=scope)

                elapsed_ms = int((time.time() - t0) * 1000)
                self.mark_dirty(False)

                return {
                    "ok": True,
                    "action": "incremental",
                    "scope": scope,
                    "elapsed_ms": elapsed_ms,
                    "fingerprint": global_fp,
                    "stats": manifest["stats"],
                }

            except Exception as e:
                self._set_error(str(e))
                self.mark_dirty(True)
                return {"ok": False, "action": "incremental", "scope": scope, "error": str(e)}
            finally:
                self._set_running(False)

    # -----------------------------
    # Full rebuild (scoped)
    # -----------------------------
    def rebuild_full(self, scope: str = "all") -> Dict[str, Any]:
        """
        ✅ 完整重建（刪 collection 後全部重灌）
        注意：這是全 collection 刪除重建（無法只重建 scope 的 subset）
        """
        scope = (scope or "all").strip().lower()
        t0 = time.time()

        with self._op_lock:
            self._set_running(True)
            self._set_error("")

            try:
                system_docs_dir, product_structured_dir, company_info_path = self._paths_from_env()

                try:
                    self.client.delete_collection(self.collection_name)
                except Exception:
                    pass

                self.collection = self.client.get_or_create_collection(
                    name=self.collection_name,
                    embedding_function=self.embedding_fn,
                    metadata={"hnsw:space": "cosine"},
                )

                sources = self._collect_sources(scope=scope)

                total_docs = 0
                skipped = 0
                per_file: Dict[str, Any] = {}
                doc_type_counter = {"product": 0, "system": 0, "company": 0, "extra": 0}

                batch = int(os.getenv("CHROMA_ADD_BATCH", "256"))
                batch = max(64, min(batch, 1024))

                for real_p, norm_p in sources:
                    try:
                        text = read_text(real_p)
                        chunks = chunk_text(text, chunk_size=self.chunk_size, overlap=self.chunk_overlap)
                        if not chunks:
                            skipped += 1
                            per_file[norm_p] = {"fp": FINGERPRINT_FN(real_p), "chunks": 0}
                            continue

                        base = os.path.basename(real_p)
                        head = text[:4000] if text else ""
                        doc_type = _guess_doc_type(norm_p, system_docs_dir, product_structured_dir, company_info_path)
                        title = _guess_title(base, doc_type, head)

                        doc_type_counter[doc_type] = doc_type_counter.get(doc_type, 0) + 1

                        ids: List[str] = []
                        docs: List[str] = []
                        metas: List[Dict[str, Any]] = []

                        for idx, ch in enumerate(chunks):
                            doc_id = _safe_id(f"{norm_p}::chunk{idx}")
                            ids.append(doc_id)
                            docs.append(ch)
                            metas.append(
                                {
                                    "source": norm_p,
                                    "file": base,
                                    "title": title,
                                    "chunk": idx,
                                    "doc_type": doc_type,
                                    "scope_call": scope,
                                    "relpath": norm_p,
                                }
                            )

                        for i in range(0, len(ids), batch):
                            self.collection.add(
                                ids=ids[i : i + batch],
                                documents=docs[i : i + batch],
                                metadatas=metas[i : i + batch],
                            )

                        total_docs += len(chunks)
                        per_file[norm_p] = {"fp": FINGERPRINT_FN(real_p), "chunks": len(chunks)}

                    except Exception as e:
                        skipped += 1
                        logger.warning(f"rebuild_full skip {norm_p}: {e}")
                        per_file[norm_p] = {"fp": FINGERPRINT_FN(real_p), "chunks": 0}

                global_fp = self._build_global_fingerprint(per_file)

                manifest = {
                    "collection": self.collection_name,
                    "scope": scope,
                    "fingerprint": global_fp,
                    "rebuilt_at": int(time.time()),
                    "mode": "full",
                    "fingerprint_mode": os.getenv("VDB_FINGERPRINT_MODE", "stat"),
                    "files": per_file,
                    "stats": {
                        "documents_loaded": total_docs,
                        "sources": len(sources),
                        "skipped_files": skipped,
                        "doc_type_files_loaded": doc_type_counter,
                        "chunk_size": self.chunk_size,
                        "chunk_overlap": self.chunk_overlap,
                        "persist_dir": self.persist_dir,
                        "collection": self.collection_name,
                    },
                }
                self._write_manifest(manifest, scope=scope)

                elapsed_ms = int((time.time() - t0) * 1000)
                self.mark_dirty(False)

                return {
                    "ok": True,
                    "action": "full_rebuild",
                    "scope": scope,
                    "elapsed_ms": elapsed_ms,
                    "fingerprint": global_fp,
                    "stats": manifest["stats"],
                }

            except Exception as e:
                self._set_error(str(e))
                self.mark_dirty(True)
                return {"ok": False, "action": "full_rebuild", "scope": scope, "error": str(e)}
            finally:
                self._set_running(False)
