import requests
from bs4 import BeautifulSoup
import psycopg2
import time
from datetime import datetime
import re

# CONFIG
TELEGRAM_TOKEN = "8008729896:AAHtytcW1Psa1wiFrNzHwHH4J7GEBSFTYSs"
TELEGRAM_CHAT_ID = "1793924830"
DB_URL = "postgresql://neondb_owner:npg_cTxWyO9hfA5C@ep-steep-surf-a12jsqi7-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"

def get_db():
    return psycopg2.connect(DB_URL)

def create_table():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS news_articles (
            id SERIAL PRIMARY KEY,
            original_url TEXT UNIQUE NOT NULL,
            title TEXT,
            slug TEXT UNIQUE,
            content TEXT,
            telugu_summary TEXT,
            author TEXT,
            category TEXT,
            published_at TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Table ready")

def send_telegram(message):
    url = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/sendMessage"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        requests.post(url, data=data, timeout=10)
    except Exception as e:
        print("Telegram error: " + str(e))

def make_slug(title):
    slug = title.lower()
    slug = re.sub(r'[^a-z0-9\s-]', '', slug)
    slug = re.sub(r'\s+', '-', slug.strip())
    slug = slug[:80]
    return slug

def fetch_article(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    try:
        response = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(response.text, "html.parser")

        title = ""
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)

        author = ""
        author_tag = soup.find("a", href=lambda x: x and "/author/" in x)
        if author_tag:
            author = author_tag.get_text(strip=True)

        date = ""
        for tag in soup.find_all(string=re.compile(r"202[0-9]")):
            if "IST" in tag or "2026" in tag:
                date = tag.strip()
                break

        category = ""
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/exams/" in href or "/school/" in href or "/college-university/" in href or "/workplace/" in href:
                category = a.get_text(strip=True)
                break

        paragraphs = []
        for tag in soup.find_all(["p", "h2", "h3", "li"]):
            text = tag.get_text(strip=True)
            if len(text) > 30:
                paragraphs.append(text)

        content = "\n\n".join(paragraphs[:50])

        return {
            "title": title,
            "author": author,
            "date": date,
            "category": category,
            "content": content
        }

    except Exception as e:
        print("Fetch error: " + str(e))
        return None

def process_pending():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, url, title FROM news_queue
        WHERE status = 'pending'
        ORDER BY scraped_at ASC
        LIMIT 5
    """)
    rows = cur.fetchall()

    if not rows:
        print("No pending articles")
        cur.close()
        conn.close()
        return

    for row in rows:
        queue_id = row[0]
        url = row[1]
        print("Processing: " + url)

        cur.execute("UPDATE news_queue SET status = 'processing' WHERE id = %s", (queue_id,))
        conn.commit()

        article = fetch_article(url)
        if not article or not article["content"]:
            cur.execute("UPDATE news_queue SET status = 'failed' WHERE id = %s", (queue_id,))
            conn.commit()
            print("Failed to fetch: " + url)
            continue

        slug = make_slug(article["title"])

        try:
            cur.execute("""
                INSERT INTO news_articles
                (original_url, title, slug, content, telugu_summary, author, category, published_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (original_url) DO NOTHING
            """, (
                url,
                article["title"],
                slug,
                article["content"],
                "",
                article["author"],
                article["category"],
                article["date"]
            ))
            conn.commit()

            cur.execute("UPDATE news_queue SET status = 'done' WHERE id = %s", (queue_id,))
            conn.commit()

            msg = "✅ <b>Article Saved!</b>\n\n"
            msg += "<b>" + article["title"] + "</b>\n\n"
            msg += "👤 " + article["author"] + "\n"
            msg += "📂 " + article["category"] + "\n"
            msg += "🔗 " + url
            send_telegram(msg)
            print("Done: " + article["title"])

        except Exception as e:
            print("Save error: " + str(e))
            cur.execute("UPDATE news_queue SET status = 'failed' WHERE id = %s", (queue_id,))
            conn.commit()

        time.sleep(3)

    cur.close()
    conn.close()

def run():
    print("Creating table...")
    create_table()
    print("Rewriter started. Running every 10 minutes.")

    while True:
        print("\n[" + str(datetime.now()) + "] Processing pending articles...")
        process_pending()
        print("Sleeping 10 minutes...")
        time.sleep(600)

if __name__ == "__main__":
    run()
