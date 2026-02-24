import sqlite3

DB_PATH = "company_data.db"

with sqlite3.connect(DB_PATH) as conn:
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        url TEXT,
        category TEXT,
        product_category TEXT,
        keywords TEXT,          -- 用逗號分隔的關鍵詞
        content TEXT NOT NULL,  -- chunk 內容
        created_at TEXT DEFAULT (datetime('now'))
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_url ON documents(url);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_category ON documents(category);")
    conn.commit()

print("✅ documents table ready")
