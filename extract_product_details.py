import requests
from bs4 import BeautifulSoup
import json
import os
import re
import time
from urllib.parse import urljoin, urlparse

BASE_URL = "http://www.optimumopt.com/"
DATA_FILE = "crawled_data/data.json"
IMAGES_DIR = "crawled_data/images"

os.makedirs(IMAGES_DIR, exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
})


def get_soup(url):
    """獲取網頁內容並解析為 BeautifulSoup 對象"""
    for attempt in range(3):
        try:
            response = SESSION.get(url, timeout=20)
            response.raise_for_status()
            response.encoding = response.apparent_encoding or "utf-8"
            return BeautifulSoup(response.text, "lxml")
        except Exception as e:
            print(f"Error fetching {url} (attempt {attempt+1}/3): {e}")
            time.sleep(1.2)
    return None


def safe_filename(img_src: str) -> str | None:
    """從 img src 轉成安全檔名（只取 uploads 後面的 basename）"""
    if not img_src:
        return None

    img_src = img_src.split("?")[0].split("#")[0]
    img_src = re.sub(r"^/gen2/\d+/", "/", img_src)

    if "uploads/" not in img_src:
        return None

    tail = img_src.split("uploads/")[-1]
    tail = tail.replace("\\", "/")
    filename = os.path.basename(tail)  # ✅ 只取檔名
    if not filename:
        return None

    # 只接受圖片副檔名
    if not re.search(r"\.(png|jpg|jpeg|webp|gif)$", filename, re.IGNORECASE):
        return None

    return filename


def download_image(img_src: str, filename: str) -> bool:
    """下載圖片到本地 images 資料夾（img_src 可相對路徑）"""
    try:
        full_url = urljoin(BASE_URL, img_src)
        save_path = os.path.join(IMAGES_DIR, filename)

        if os.path.exists(save_path):
            return True

        resp = SESSION.get(full_url, timeout=30)
        resp.raise_for_status()
        with open(save_path, "wb") as f:
            f.write(resp.content)
        return True
    except Exception as e:
        print(f"  ❌ 下載失敗: {img_src} -> {filename}: {e}")
        return False


def extract_product_detail(url):
    """從商品頁面提取詳細資訊"""
    print(f"\n正在處理: {url}")
    soup = get_soup(url)
    if not soup:
        return None

    result = {"description": "", "images": [], "specifications": ""}

    detail_div = soup.find("div", {"id": "info-cnt-0"})
    if not detail_div:
        print("  ⚠️  未找到產品詳情區域 (info-cnt-0)")
        return result

    # 描述（保留你的做法）
    result["description"] = detail_div.get_text(strip=True, separator="\n")

    images = []
    seen = set()

    for img in detail_div.find_all("img"):
        src = img.get("src") or ""
        filename = safe_filename(src)
        if not filename:
            continue

        if filename in seen:
            continue
        seen.add(filename)

        # ✅ 若本地沒有，就下載
        if download_image(src, filename):
            images.append(filename)
            print(f"  ✅ 圖片: {filename}")

    result["images"] = images
    print(f"  共處理 {len(images)} 張圖片")
    print(f"  描述長度: {len(result['description'])} 字元")

    return result


def update_data_json():
    """更新 data.json 文件，填充商品描述和圖片"""
    if not os.path.exists(DATA_FILE):
        print(f"錯誤: 找不到 {DATA_FILE}")
        return

    with open(DATA_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    products = data.get("products", [])
    print(f"\n共有 {len(products)} 個商品需要處理\n")

    processed_urls = set()
    updated_count = 0

    for i, product in enumerate(products):
        url = product.get("url")
        if not url:
            continue

        if url in processed_urls:
            print(f"[{i+1}/{len(products)}] 跳過重複的商品: {product.get('title', 'Unknown')}")
            continue
        processed_urls.add(url)

        print(f"\n{'='*60}")
        print(f"[{i+1}/{len(products)}] 商品: {product.get('title', 'Unknown')}")

        # 如果已經有描述和圖片，跳過
        if product.get("description") and product.get("images"):
            print("  ℹ️  此商品已有資料，跳過...")
            continue

        details = extract_product_detail(url)
        if details is None:
            continue

        product["description"] = details.get("description", "")
        product["images"] = details.get("images", [])          # ✅ 存檔名 list
        product["specifications"] = details.get("specifications", "")

        updated_count += 1

        if updated_count % 5 == 0:
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"\n  💾 已保存進度 ({updated_count} 個商品已更新)")

        time.sleep(1)

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print(f"✅ 完成！共更新了 {updated_count} 個商品的資料")
    print(f"📁 資料已保存至 {DATA_FILE}")


if __name__ == "__main__":
    update_data_json()
