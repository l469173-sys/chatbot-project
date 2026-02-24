# tools/check_assets.py
import os, json, re, sqlite3

DB = "company_data.db"
IMG_DIR = "crawled_data/images"
MD_DIR = "data/product_structured"


def main():
    missing = []

    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("SELECT title, images FROM products")
    for title, imgs in cur.fetchall():
        try:
            arr = json.loads(imgs)
        except:
            arr = []

        for f in arr:
            p = os.path.join(IMG_DIR, f)
            if not os.path.isfile(p):
                missing.append(("DB", title, f))

    for fn in os.listdir(MD_DIR):
        if not fn.endswith(".md"): continue
        path = os.path.join(MD_DIR, fn)
        text = open(path,encoding="utf8").read()

        for im in re.findall(r"\b([\w\-]+\.jpg)", text):
            p = os.path.join(IMG_DIR, im)
            if not os.path.isfile(p):
                missing.append(("MD", fn, im))

    with open("missing_assets.json","w",encoding="utf8") as f:
        json.dump(missing,f,ensure_ascii=False,indent=2)

    print("Missing:",len(missing))


if __name__=="__main__":
    main()
