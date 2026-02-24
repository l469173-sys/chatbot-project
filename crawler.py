import os
import re
import json
import time
import hashlib
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


BASE_URL = "http://www.optimumopt.com/"
START_URL = BASE_URL + "?mod=product&col_key=product&lang=cn"
HOME_URL = BASE_URL + "?lang=cn"

OUTPUT_DIR = "crawled_data"
IMAGES_DIR = os.path.join(OUTPUT_DIR, "images")
DATA_PATH = os.path.join(OUTPUT_DIR, "data.json")

os.makedirs(IMAGES_DIR, exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
})


def clean_text(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def get_soup(url: str) -> BeautifulSoup | None:
    for attempt in range(3):
        try:
            r = SESSION.get(url, timeout=20)
            r.raise_for_status()
            # 有些頁面用 utf-8，但也可能是 big5；用 apparent_encoding 較穩
            r.encoding = r.apparent_encoding or "utf-8"
            return BeautifulSoup(r.text, "lxml")
        except Exception as e:
            print(f"[get_soup] {url} attempt {attempt+1}/3 failed: {e}")
            time.sleep(1.5)
    return None


def is_probably_ui_asset(filename: str) -> bool:
    """避免下載 icon/小圖/介面圖（可依實際站台再調）"""
    name = filename.lower()
    bad_keywords = ["icon", "logo", "btn", "arrow", "sprite", "loading", "favicon"]
    return any(k in name for k in bad_keywords)


def download_image(img_url: str) -> str | None:
    if not img_url:
        return None

    full_url = urljoin(BASE_URL, img_url)
    path = urlparse(full_url).path
    filename = os.path.basename(path)

    if not filename:
        # 用 URL hash 當檔名（避免無檔名）
        filename = hashlib.md5(full_url.encode("utf-8")).hexdigest() + ".jpg"

    # 過濾非圖片
    if not re.search(r"\.(png|jpg|jpeg|webp|gif)$", filename, re.IGNORECASE):
        return None

    if is_probably_ui_asset(filename):
        return None

    save_path = os.path.join(IMAGES_DIR, filename)
    if os.path.exists(save_path):
        return filename  # ✅ 回傳相對檔名（給後端 /images/<filename> 用）

    try:
        r = SESSION.get(full_url, stream=True, timeout=30)
        if r.status_code != 200:
            return None

        with open(save_path, "wb") as f:
            for chunk in r.iter_content(1024 * 64):
                if chunk:
                    f.write(chunk)

        print(f"Downloaded image: {filename}")
        return filename
    except Exception as e:
        print(f"[download_image] {full_url} failed: {e}")
        return None


def get_categories() -> list[dict]:
    """
    更保守抓分類：只抓含 cate_id 的連結，並排除 pro_id。
    也會做去重與清洗。
    """
    print("Fetching categories...")
    soup = get_soup(START_URL)
    if not soup:
        return []

    categories = []
    seen = set()

    for a in soup.select("a[href*='cate_id=']"):
        href = a.get("href", "")
        if "pro_id=" in href:
            continue
        name = clean_text(a.get_text())
        if not name:
            continue
        full_url = urljoin(BASE_URL, href)
        key = (name, full_url)
        if key in seen:
            continue
        seen.add(key)
        categories.append({"name": name, "url": full_url})

    print(f"Found {len(categories)} categories.")
    return categories


def get_products_from_category(category_url: str) -> list[dict]:
    """
    抓該分類的產品列表：只抓 prodetail 連結，並以 URL 去重。
    """
    print(f"Fetching products from {category_url}...")
    soup = get_soup(category_url)
    if not soup:
        return []

    products = []
    seen = set()

    for a in soup.select("a[href*='pro_id='][href*='t=prodetail']"):
        href = a.get("href", "")
        full_url = urljoin(BASE_URL, href)
        if full_url in seen:
            continue
        seen.add(full_url)

        name = clean_text(a.get_text())
        if not name:
            img = a.find("img")
            if img and img.get("alt"):
                name = clean_text(img.get("alt"))
            elif a.get("title"):
                name = clean_text(a.get("title"))

        products.append({"name": name or "", "url": full_url})

    print(f"Found {len(products)} products in category.")
    return products


def split_description_and_specs(text: str) -> tuple[str, str]:
    """
    粗略把內容切成描述與規格：
    - 優先找「規格」「Specification」「Specifications」「產品規格」等段落
    """
    raw = (text or "").strip()
    if not raw:
        return "", ""

    # 常見分隔關鍵字
    markers = [
        "規格", "產品規格", "Specifications", "Specification", "SPEC", "規  格"
    ]
    # 找第一個出現位置
    idx = None
    for m in markers:
        p = raw.lower().find(m.lower())
        if p != -1:
            idx = p
            break

    if idx is None or idx < 20:
        # 找不到或太前面（不可信），全部當描述
        return clean_text(raw), ""

    desc = clean_text(raw[:idx])
    specs = raw[idx:].strip()
    return desc, specs


def get_product_details(product_url: str) -> dict | None:
    print(f"Fetching details for {product_url}...")
    soup = get_soup(product_url)
    if not soup:
        return None

    details = {
        "url": product_url,
        "title": "",
        "description": "",
        "specifications": "",
        "images": [],
    }

    # Title
    title_tag = soup.select_one(".pro_title") or soup.find("h1")
    if title_tag:
        details["title"] = clean_text(title_tag.get_text())

    # 內容區域（盡量縮小範圍，避免抓到整頁導覽文字）
    content_div = soup.select_one("#block-body") or soup.select_one(".block-body") or soup.select_one(".edit-wrap-content")
    if content_div:
        raw_text = content_div.get_text("\n", strip=True)
        desc, specs = split_description_and_specs(raw_text)
        details["description"] = desc
        details["specifications"] = specs

        # 圖片：只取內容區域的圖（可視情況放寬）
        for img in content_div.find_all("img"):
            src = img.get("src") or ""
            # 偏好 uploads 路徑（通常是內容圖）
            if "uploads" not in src and "Upload" not in src:
                continue
            fn = download_image(src)
            if fn and fn not in details["images"]:
                details["images"].append(fn)

    # 若 title 還是空，試著從 meta 取
    if not details["title"]:
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            details["title"] = clean_text(og["content"])

    return details


def get_company_info() -> dict:
    print("Fetching company info...")
    soup = get_soup(HOME_URL)
    if not soup:
        return {}

    about_url = None
    for a in soup.find_all("a", href=True):
        if "關於尚澤" in clean_text(a.get_text()):
            about_url = urljoin(BASE_URL, a["href"])
            break

    if not about_url:
        print("Could not find About Us link.")
        return {}

    soup = get_soup(about_url)
    if not soup:
        return {}

    content_div = soup.select_one(".about_content") or soup.select_one(".content") or soup.select_one("#main_content")
    content = clean_text(content_div.get_text("\n", strip=True)) if content_div else ""

    return {"url": about_url, "content": content}


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_data = {"company_info": {}, "products": []}
    all_data["company_info"] = get_company_info()

    categories = get_categories()

    seen_products = set()  # 以 URL 去重

    for cat in categories:
        cat_name = clean_text(cat["name"])
        cat_products = get_products_from_category(cat["url"])

        for prod in cat_products:
            url = prod["url"]
            if url in seen_products:
                continue
            seen_products.add(url)

            details = get_product_details(url)
            if not details:
                continue

            details["category"] = cat_name or "未分類"

            # 基本清洗
            details["title"] = clean_text(details.get("title", "")) or clean_text(prod.get("name", ""))
            details["description"] = clean_text(details.get("description", ""))
            details["specifications"] = (details.get("specifications", "") or "").strip()

            all_data["products"].append(details)

            # 每筆存一次，避免中途掛掉全沒了
            with open(DATA_PATH, "w", encoding="utf-8") as f:
                json.dump(all_data, f, ensure_ascii=False, indent=2)

            time.sleep(0.8)  # polite

    print(f"Crawling finished. Saved to {DATA_PATH}")
    print(f"Total products: {len(all_data['products'])}")


if __name__ == "__main__":
    main()
