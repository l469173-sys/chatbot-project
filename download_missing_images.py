import requests
from bs4 import BeautifulSoup
import json
import os
import re
from urllib.parse import urljoin, urlparse
import time

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
    """獲取網頁內容並解析為 BeautifulSoup 對象（含簡單重試）"""
    for attempt in range(3):
        try:
            resp = SESSION.get(url, timeout=20)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or "utf-8"
            return BeautifulSoup(resp.text, "lxml")
        except Exception as e:
            print(f"❌ Error fetching {url} (attempt {attempt+1}/3): {e}")
            time.sleep(1.2)
    return None


def safe_filename_from_src(src: str) -> str | None:
    """
    從 src 取得安全檔名：
    - 移除 /gen2/數字/ 前綴
    - 只保留 uploads/ 後面的路徑
    - 最後只取 basename（避免子資料夾）
    """
    if not src:
        return None

    src = src.split("?")[0].split("#")[0]
    src = re.sub(r"^/gen2/\d+/", "/", src)

    if "uploads/" not in src:
        return None

    tail = src.split("uploads/")[-1]
    tail = tail.replace("\\", "/")
    filename = os.path.basename(tail)

    if not filename:
        return None

    # 只接受常見圖片副檔名
    if not re.search(r"\.(png|jpg|jpeg|webp|gif)$", filename, re.IGNORECASE):
        return None

    return filename


def download_image(img_url: str, save_path: str) -> bool:
    """下載圖片到指定路徑（img_url 可為相對路徑）"""
    try:
        full_url = urljoin(BASE_URL, img_url)
        resp = SESSION.get(full_url, timeout=30)
        resp.raise_for_status()
        with open(save_path, "wb") as f:
            f.write(resp.content)
        return True
    except Exception as e:
        print(f"    ❌ 下載失敗: {img_url} - {e}")
        return False


def extract_and_download_images(url):
    """從商品頁面提取圖片並下載缺少的圖片（images 存檔名）"""
    print(f"\n正在處理: {url}")
    soup = get_soup(url)
    if not soup:
        return None

    result = {
        "description": "",
        "images": [],          # ✅ 存檔名 list: ["xxx.jpg", ...]
        "specifications": ""
    }

    detail_div = soup.find("div", {"id": "info-cnt-0"})
    if not detail_div:
        print("  ⚠️  未找到產品詳情區域 (info-cnt-0)")
        return result

    # 描述：保留原本做法，但避免超多空白
    text = detail_div.get_text(separator="\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text)
    result["description"] = text

    images = []
    seen = set()

    for img in detail_div.find_all("img"):
        src = img.get("src") or ""
        filename = safe_filename_from_src(src)
        if not filename:
            continue

        if filename in seen:
            continue
        seen.add(filename)

        local_path = os.path.join(IMAGES_DIR, filename)

        if os.path.exists(local_path):
            images.append(filename)
            print(f"  ✓ 已存在: {filename}")
            continue

        print(f"  ⬇️  下載中: {filename}")
        if download_image(src, local_path):
            images.append(filename)
            print(f"  ✅ 下載成功: {filename}")
        else:
            print(f"  ❌ 下載失敗: {filename}")

    result["images"] = images
    print(f"  📊 共處理 {len(images)} 張圖片")
    print(f"  📝 描述長度: {len(result['description'])} 字元")

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
    downloaded_new_images = 0

    for i, product in enumerate(products):
        url = product.get("url")
        if not url:
            continue

        if url in processed_urls:
            print(f"[{i+1}/{len(products)}] ⏭️  跳過重複 URL: {product.get('title', 'Unknown')}")
            continue
        processed_urls.add(url)

        print(f"\n{'='*60}")
        print(f"[{i+1}/{len(products)}] 商品: {product.get('title', 'Unknown')}")

        # 需要更新：description 空 或 images 空
        needs_update = (not product.get("description")) or (not product.get("images"))
        if not needs_update:
            print("  ℹ️  此商品已有完整資料，跳過...")
            continue

        # 先記錄更新前已有多少圖片檔名
        old_images = product.get("images") or []
        if isinstance(old_images, str):
            try:
                old_images = json.loads(old_images)
            except Exception:
                old_images = []
        old_set = set(old_images) if isinstance(old_images, list) else set()

        details = extract_and_download_images(url)
        if details is None:
            continue

        product["description"] = details.get("description", "")
        product["images"] = details.get("images", [])          # ✅ 檔名 list
        product["specifications"] = details.get("specifications", "")

        # 只算新增加的圖片數（避免統計失真）
        new_set = set(product["images"])
        downloaded_new_images += max(0, len(new_set - old_set))

        updated_count += 1

        if updated_count % 5 == 0:
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"\n  💾 已保存進度 ({updated_count} 個商品已更新)")

        time.sleep(1)

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print("✅ 完成！")
    print("📊 統計資訊：")
    print(f"  • 更新了 {updated_count} 個商品的資料")
    print(f"  • 新下載圖片: {downloaded_new_images} 張")
    print(f"📁 資料已保存至 {DATA_FILE}")
    print(f"{'='*60}")


if __name__ == "__main__":
    update_data_json()
