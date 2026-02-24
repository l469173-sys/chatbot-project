import os
import re
import json
import time
import hashlib
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


BASE_URL = "http://www.optimumopt.com/"
DATA_FILE = "crawled_data/data.json"
IMAGES_DIR = "crawled_data/images"

os.makedirs(IMAGES_DIR, exist_ok=True)

S = requests.Session()
S.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
})


def get_soup(url: str) -> BeautifulSoup | None:
    for attempt in range(3):
        try:
            r = S.get(url, timeout=20)
            r.raise_for_status()
            r.encoding = r.apparent_encoding or "utf-8"
            return BeautifulSoup(r.text, "lxml")
        except Exception as e:
            print(f"❌ Error fetching {url} (attempt {attempt+1}/3): {e}")
            time.sleep(1.2)
    return None


def safe_filename_from_url(full_url: str) -> str:
    """把 URL 變成安全檔名（避免包含 / 造成子資料夾）"""
    path = urlparse(full_url).path
    name = os.path.basename(path)

    if not name:
        name = hashlib.md5(full_url.encode("utf-8")).hexdigest() + ".jpg"

    # 去除 query 殘留
    name = name.split("?")[0].split("#")[0]

    # 若檔名仍含奇怪字元，做簡化
    name = re.sub(r"[^A-Za-z0-9._-]", "_", name)

    # 若沒副檔名，補 jpg
    if not re.search(r"\.(png|jpg|jpeg|webp|gif)$", name, re.IGNORECASE):
        name += ".jpg"

    return name


def download_image(full_img_url: str) -> str | None:
    """下載圖片並回傳『檔名』"""
    try:
        filename = safe_filename_from_url(full_img_url)
        save_path = os.path.join(IMAGES_DIR, filename)

        if os.path.exists(save_path):
            return filename

        r = S.get(full_img_url, timeout=30)
        r.raise_for_status()

        with open(save_path, "wb") as f:
            f.write(r.content)

        return filename
    except Exception as e:
        print(f"    ❌ 下載失敗: {full_img_url} - {e}")
        return None


def clean_text(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def split_desc_specs(raw_text: str) -> tuple[str, str]:
    """
    嘗試把內容切成描述/規格。
    - 找到『規格/Specifications』等關鍵字，後半當規格
    """
    raw = (raw_text or "").strip()
    if not raw:
        return "", ""

    markers = ["產品規格", "規格", "Specifications", "Specification", "SPEC"]
    idx = None
    lower = raw.lower()
    for m in markers:
        p = lower.find(m.lower())
        if p != -1 and p > 20:
            idx = p
            break

    if idx is None:
        return clean_text(raw), ""

    return clean_text(raw[:idx]), raw[idx:].strip()


def extract_and_download_all_images(url: str) -> dict | None:
    print(f"\n正在處理: {url}")
    soup = get_soup(url)
    if not soup:
        return None

    result = {"description": "", "images": [], "specifications": ""}

    images: list[str] = []

    # ===== 1) 主圖：保留你原本邏輯（width=400），但更保守 =====
    print("  🔍 尋找商品主圖...")
    main_img = soup.find("img", {"width": "400"})
    if main_img:
        src = main_img.get("src") or ""
        if src:
            full_img_url = urljoin(BASE_URL, src)
            fn = download_image(full_img_url)
            if fn and fn not in images:
                images.append(fn)
                print(f"  ✅ 主圖: {fn}")
    else:
        print("  ⚠️  未找到商品主圖 (width=400)")

    # ===== 2) 詳情區塊：info-cnt-0（你原本的） =====
    print("  🔍 尋找產品詳情區域...")
    detail_div = soup.find("div", {"id": "info-cnt-0"})

    # 若找不到，退而求其次抓 block-body
    if not detail_div:
        detail_div = soup.find("div", {"id": "block-body"}) or soup.find("div", {"class": "block-body"})

    if detail_div:
        raw_text = detail_div.get_text("\n", strip=True)
        desc, specs = split_desc_specs(raw_text)
        result["description"] = desc
        result["specifications"] = specs

        # 抓所有內容圖
        for img in detail_div.find_all("img"):
            src = img.get("src") or ""
            if not src:
                continue

            # 只抓看起來像內容圖的（偏好 uploads）
            if "uploads" not in src and "Upload" not in src:
                continue

            full_img_url = urljoin(BASE_URL, src)
            fn = download_image(full_img_url)
            if fn and fn not in images:
                images.append(fn)
                print(f"  ✅ 詳情圖: {fn}")

        print(f"  📝 描述長度: {len(result['description'])} 字元")
    else:
        print("  ⚠️  未找到產品詳情區域")

    # 去重保序
    seen = set()
    uniq = []
    for fn in images:
        if fn in seen:
            continue
        seen.add(fn)
        uniq.append(fn)

    result["images"] = uniq
    print(f"  📊 圖片數量: {len(uniq)}")

    return result


def update_data_json():
    if not os.path.exists(DATA_FILE):
        print(f"❌ 錯誤: 找不到 {DATA_FILE}")
        return

    with open(DATA_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    products = data.get("products", [])
    print(f"\n{'='*60}\n共有 {len(products)} 個商品需要處理\n{'='*60}\n")

    processed_urls = set()
    updated_count = 0
    total_new_images = 0

    for i, product in enumerate(products):
        url = product.get("url")
        if not url:
            continue

        if url in processed_urls:
            print(f"[{i+1}/{len(products)}] ⏭️  跳過重複 URL")
            continue
        processed_urls.add(url)

        title = product.get("title") or "Unknown"
        print(f"\n{'='*60}\n[{i+1}/{len(products)}] 商品: {title}")

        old_images = product.get("images") or []
        if isinstance(old_images, str):
            try:
                old_images = json.loads(old_images)
            except Exception:
                old_images = []
        old_image_count = len(old_images)

        details = extract_and_download_all_images(url)
        if details:
            product["description"] = details["description"]
            product["images"] = details["images"]          # ✅ 存檔名 list
            product["specifications"] = details["specifications"]

            new_image_count = len(details["images"])
            if new_image_count > old_image_count:
                total_new_images += (new_image_count - old_image_count)

            updated_count += 1

            if updated_count % 5 == 0:
                with open(DATA_FILE, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print(f"  💾 已保存進度（{updated_count} 個商品已更新）")

        time.sleep(1)

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    total_images = sum(len(p.get("images") or []) for p in products)
    products_with_images = sum(1 for p in products if p.get("images"))

    print(f"\n{'='*60}")
    print("✅ 完成！")
    print(f"  • 更新商品: {updated_count}")
    print(f"  • 新增圖片: {total_new_images}")
    print(f"  • 圖片總數: {total_images}")
    print(f"  • 有圖片的商品: {products_with_images}/{len(products)}")
    print(f"  • 平均每個商品: {total_images/len(products):.1f} 張")
    print(f"📁 資料已保存至 {DATA_FILE}")
    print(f"{'='*60}")


if __name__ == "__main__":
    update_data_json()
