import sqlite3, json

DB = "company_data.db"

FIX = {
    "IS治具": "IS_fixture_main.jpg",
    "廣角鏡頭量測-積分球均勻光源": "integrating_sphere_uniform_light_source_main.jpg",
}

def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    for title, fn in FIX.items():
        cur.execute("SELECT id, title, images FROM products WHERE title = ?", (title,))
        row = cur.fetchone()
        if not row:
            print(f"[MISS] 找不到 title：{title}")
            continue

        _id, _title, _images = row
        new_images = json.dumps([fn], ensure_ascii=False)

        cur.execute("UPDATE products SET images = ? WHERE id = ?", (new_images, _id))
        print(f"[OK] id={_id} {title} -> images={new_images}")

    conn.commit()
    conn.close()
    print("done.")

if __name__ == "__main__":
    main()
