import re
import time
import sqlite3
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

DB_PATH = "company_data.db"
BASE = "http://www.optimumopt.com/"
START_URLS = [
    "http://www.optimumopt.com/",              # 首頁
    "http://www.optimumopt.com/mod-product/",  # 產品列表（你站上常見）
    "http://www.optimumopt.com/mod-news/",     # 可能新聞/文章
]

HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_PAGES = 200          # 先抓 200 頁，後面你再加
SLEEP = 0.4

def norm_url(u: str) -> str:
    u = u.split("#")[0]
    return u.rstrip("/")

def same_domain(u: str) -> bool:
    return urlparse(u).netloc == urlparse(BASE).netloc

def extract_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")

    # 移除不需要的區塊
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = soup.get_text("\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def guess_title(soup: BeautifulSoup) -> str:
    if soup.title and soup.title.get_text(strip=True):
        return soup.title.get_text(strip=True)
    h1 = soup.find("h1")
    if h1:
        return h1.get_text(strip=True)
    return "(no title)"

def guess_category(url: str) -> str:
    u = url.lower()
    if "product" in u or "mod-product" in u:
        return "PRODUCT"
    if "faq" in u:
        return "FAQ"
    if "download" in u:
        return "DOWNLOAD"
    if "contact" in u:
        return "CONTACT"
    return "PAGE"

def chunk_text(text: str, min_len=300, max_len=900):
    # 以空行段落切，再合併到 300~900 字範圍
    paras = [p.strip() for p in text.split("\n\n") if p.strip()]
    chunks = []
    buf = ""
    for p in paras:
        if len(buf) + len(p) + 2 <= max_len:
            buf = (buf + "\n\n" + p).strip() if buf else p
        else:
            if buf:
                chunks.append(buf)
            buf = p
    if buf:
        chunks.append(buf)

    # 過短就合併
    merged = []
    tmp = ""
    for c in chunks:
        if len(tmp) + len(c) + 2 < min_len:
            tmp = (tmp + "\n\n" + c).strip() if tmp else c
        else:
            if tmp:
                merged.append(tmp)
                tmp = ""
            merged.append(c)
    if tmp:
        merged.append(tmp)
    return merged

def save_chunks(conn, title, url, category, content_chunks):
    cur = conn.cursor()
    for ch in content_chunks:
        cur.execute("""
        INSERT INTO documents(title, url, category, product_category, keywords, content)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (title, url, category, "", "", ch))
    conn.commit()

def exists_url(conn, url):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM documents WHERE url=? LIMIT 1", (url,))
    return cur.fetchone() is not None

def crawl():
    seen = set()
    queue = list(dict.fromkeys([norm_url(u) for u in START_URLS]))

    with sqlite3.connect(DB_PATH) as conn:
        while queue and len(seen) < MAX_PAGES:
            url = queue.pop(0)
            if url in seen:
                continue
            seen.add(url)

            try:
                r = requests.get(url, headers=HEADERS, timeout=20)
                if r.status_code != 200:
                    continue

                html = r.text
                soup = BeautifulSoup(html, "lxml")

                title = guess_title(soup)
                category = guess_category(url)
                text = extract_text(html)

                if len(text) > 200 and not exists_url(conn, url):
                    chunks = chunk_text(text)
                    save_chunks(conn, title, url, category, chunks)
                    print(f"✅ saved {len(chunks):2d} chunks | {title} | {url}")
                else:
                    print(f"↩️ skip (short/exist) | {url}")

                # 抽連結加入 queue
                for a in soup.find_all("a", href=True):
                    href = a["href"].strip()
                    if href.startswith("mailto:") or href.startswith("javascript:"):
                        continue
                    absu = norm_url(urljoin(url, href))
                    if same_domain(absu) and absu.startswith(BASE):
                        if absu not in seen:
                            queue.append(absu)

                time.sleep(SLEEP)

            except Exception as e:
                print("❌", url, e)

    print("DONE pages:", len(seen))

if __name__ == "__main__":
    crawl()
