import requests
from bs4 import BeautifulSoup
import psycopg2
import time
from datetime import datetime

# CONFIG
TELEGRAM_TOKEN = "8008729896:AAHtytcW1Psa1wiFrNzHwHH4J7GEBSFTYSs"
TELEGRAM_CHAT_ID = "1793924830"
DB_URL = "postgresql://neondb_owner:npg_cTxWyO9hfA5C@ep-steep-surf-a12jsqi7-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"

# CATEGORY PAGES - NOT articles
SKIP_SLUGS = [
    "/latest", "/featured-news", "/exam-news",
    "/college-university", "/school", "/workplace",
    "/opinion", "/study-abroad", "/policies",
    "/competitive-exams", "/exams", "/author",
    "/search", "/hindi", "/news-sitemap", "/sitemap"
]

def get_db():
    return psycopg2.connect(DB_URL)

def create_table():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS news_queue (
            id SERIAL PRIMARY KEY,
            url TEXT UNIQUE NOT NULL,
            title TEXT,
            status TEXT DEFAULT 'pending',
            scraped_at TIMESTAMP DEFAULT NOW()
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

def is_article_url(url):
    if not url.startswith("https://news.careers360.com/"):
        return False
    slug = url.replace("https://news.careers360.com", "")
    if slug == "" or slug == "/":
        return False
    for skip in SKIP_SLUGS:
        if slug.startswith(skip):
            return False
    if len(slug) < 10:
        return False
    if slug.endswith(".xml") or slug.endswith(".json"):
        return False
    return True

def scrape_homepage():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    try:
        response = requests.get("https://news.careers360.com", headers=headers, timeout=15)
        soup = BeautifulSoup(response.text, "html.parser")

        links = []
        seen = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("/"):
                href = "https://news.careers360.com" + href
            if is_article_url(href) and href not in seen:
                title = a.get_text(strip=True)
                links.append({"url": href, "title": title})
                seen.append(href)

        print("Found " + str(len(links)) + " article links")
        return links

    except Exception as e:
        print("Scrape error: " + str(e))
        return []

def save_new_articles(links):
    if not links:
        return

    conn = get_db()
    cur = conn.cursor()
    new_count = 0

    for link in links:
        url = link["url"]
        title = link["title"][:500] if link["title"] else ""

        try:
            cur.execute(
                "INSERT INTO news_queue (url, title) VALUES (%s, %s) ON CONFLICT (url) DO NOTHING",
                (url, title)
            )
            if cur.rowcount > 0:
                new_count += 1
                conn.commit()
                msg = "📰 <b>New Article Found!</b>\n\n"
                msg += "<b>" + title + "</b>\n\n"
                msg += "🔗 " + url
                send_telegram(msg)
                print("NEW: " + url)
            else:
                print("SKIP: " + url)

        except Exception as e:
            print("DB error: " + str(e))
            conn.rollback()

    cur.close()
    conn.close()
    print("New articles saved: " + str(new_count))

def run():
    print("Creating table...")
    create_table()
    print("Scraper started. Running every 5 minutes.")
    send_telegram("🚀 Vidyalo News Scraper Started!\n\nMonitoring news.careers360.com every 5 minutes.")

    while True:
        print("\n[" + str(datetime.now()) + "] Scraping...")
        links = scrape_homepage()
        save_new_articles(links)
        print("Sleeping 5 minutes...")
        time.sleep(300)

if __name__ == "__main__":
    run()
