import requests
from bs4 import BeautifulSoup
import json
import re
import time
from urllib.parse import urljoin

BASE_URL = "http://www.optimumopt.com/"
DATA_PATH = "crawled_data/data.json"

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
            print(f"❌ 無法抓取 {url} (attempt {attempt+1}/3): {e}")
            time.sleep(1.2)
    return None


def clean_lines(text: str) -> str:
    text = (text or "").strip()
    # 先做基本空白清理
    lines = [ln.strip() for ln in text.split("\n")]
    lines = [ln for ln in lines if ln]
    # 合併過多空行
    return "\n".join(lines)


def extract_main_content(soup: BeautifulSoup | None) -> str:
    if not soup:
        return ""

    # 優先找可能的主內容區
    selectors = [
        ("div", {"id": "block-body"}),
        ("div", {"class": "block-body"}),
        ("div", {"class": "site-widget-bd"}),
        ("div", {"class": "edit-wrap-content"}),
        ("div", {"class": "content"}),
        ("div", {"id": "main_content"}),
    ]

    def strip_noise(node):
        for tag in node.find_all(["script", "style", "noscript"]):
            tag.decompose()
        # 常見雜訊區塊
        for sel in ["header", "footer", "nav", "aside"]:
            for tag in node.find_all(sel):
                tag.decompose()

    for tag_name, attrs in selectors:
        content_div = soup.find(tag_name, attrs)
        if content_div:
            strip_noise(content_div)
            text = content_div.get_text("\n", strip=True)
            return clean_lines(text)

    # fallback: body
    body = soup.find("body")
    if body:
        strip_noise(body)
        text = body.get_text("\n", strip=True)
        return clean_lines(text)

    return ""


def extract_news_items(soup: BeautifulSoup | None, page_url: str) -> list[dict]:
    if not soup:
        return []

    items = []

    # 先找可能的新聞容器
    candidates = (
        soup.find_all("div", class_="article-item")
        or soup.find_all("div", class_="news-item")
        or soup.find_all("li", class_="news")
        or soup.find_all("article")
    )

    # 若找不到，退而求其次：抓所有疑似新聞連結
    if not candidates:
        for a in soup.find_all("a", href=True):
            txt = a.get_text(strip=True)
            href = a["href"]
            if not txt or len(txt) < 4:
                continue
            # 簡單判斷：新聞頁常見參數
            if "mod=info" in href or "col_key=news" in href or "t=" in href:
                items.append({
                    "title": txt,
                    "url": urljoin(BASE_URL, href)
                })
        # 去重取前10
        seen = set()
        uniq = []
        for x in items:
            if x["url"] in seen:
                continue
            seen.add(x["url"])
            uniq.append(x)
        return uniq[:10]

    for block in candidates[:10]:
        title_tag = block.find(["h2", "h3", "h4", "a"])
        date_tag = block.find(class_=["date", "time", "publish-time"])
        content_tag = block.find(["p", "div"], class_=["summary", "excerpt", "content"])
        link_tag = block.find("a", href=True)

        title = title_tag.get_text(strip=True) if title_tag else ""
        date = date_tag.get_text(strip=True) if date_tag else ""
        summary = content_tag.get_text(strip=True) if content_tag else ""

        url = ""
        if link_tag:
            url = urljoin(BASE_URL, link_tag["href"])

        if title:
            items.append({
                "title": title,
                "date": date,
                "url": url or page_url,
                "content": summary
            })

    return items


def fetch_company_info() -> dict:
    company_pages = {
        "home": {
            "url": BASE_URL + "?lang=cn",
            "name": "公司簡介（首頁）"
        },
        "about": {
            "url": BASE_URL + "?mod=page&col_key=company&t=about&lang=cn",
            "name": "關於公司"
        },
        "news": {
            "url": BASE_URL + "?mod=info&col_key=news&lang=cn",
            "name": "相關新聞"
        },
        "contact": {
            "url": BASE_URL + "?mod=page&col_key=contact&lang=cn",
            "name": "聯絡資訊"
        }
    }

    out = {}

    print("=" * 60)
    print("開始抓取公司資訊...")
    print("=" * 60)

    for key, page in company_pages.items():
        print(f"\n📄 正在抓取: {page['name']}")
        print(f"   URL: {page['url']}")

        soup = get_soup(page["url"])

        if key == "news":
            news_items = extract_news_items(soup, page["url"])
            if news_items:
                out[key] = {
                    "url": page["url"],
                    "news_count": len(news_items),
                    "items": news_items
                }
                print(f"   ✅ 成功抓取 {len(news_items)} 則新聞（列表）")
            else:
                content = extract_main_content(soup)
                out[key] = {"url": page["url"], "content": content}
                print(f"   ✅ 以全文方式抓取（{len(content)} 字元）")
        else:
            content = extract_main_content(soup)
            out[key] = {"url": page["url"], "content": content}
            print(f"   ✅ 成功抓取內容（{len(content)} 字元）")

    return out


def update_data_json(company_info: dict):
    try:
        with open(DATA_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"❌ 找不到 {DATA_PATH}")
        return

    # ✅ 只更新 company_info，不動 products
    data["company_info"] = company_info

    with open(DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 60)
    print(f"✅ 公司資訊已更新至 {DATA_PATH}")
    print("=" * 60)

    print("\n📊 更新摘要：")
    for key, info in company_info.items():
        if key == "news" and isinstance(info, dict) and "items" in info:
            print(f"  • news: {info.get('news_count', len(info['items']))} 則")
        else:
            print(f"  • {key}: {len(info.get('content', ''))} 字元")


if __name__ == "__main__":
    company_info = fetch_company_info()
    update_data_json(company_info)
    print("\n✨ 完成！")
