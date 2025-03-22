import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import sqlite3
from tqdm import tqdm
import os
import time
import random
from hashlib import md5
from datetime import datetime, timedelta, UTC
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from PIL import Image

# --- Constants ---
DB_PATH = "../links.db"
FAVICON_DIR = "../favicons"
os.makedirs(FAVICON_DIR, exist_ok=True)

DEFAULT_USER_AGENT = "NovaCrawler/1.1"
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0",
    "Dalvik/2.1.0 (Linux; U; Android 11; Pixel 3a XL Build/RQ2A.210305.006)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
]
MAX_THREADS = 50  # Adjust based on system performance

# --- Database Setup ---
def connect_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute('PRAGMA journal_mode=WAL;')
    return conn

def get_stale_urls(conn):
    """Retrieve URLs where last_crawled is null or older than 14 days."""
    cutoff_date = (datetime.now(UTC) - timedelta(days=14)).isoformat()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT url FROM pages
        WHERE last_crawled IS NULL OR datetime(last_crawled) < datetime(?)
    ''', (cutoff_date,))
    return [row[0] for row in cursor.fetchall()]

# --- Helper Functions ---
def get_headers(stealth_mode, referrer=None):
    headers = {
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'DNT': '1' if stealth_mode else '0',
        'User-Agent': random.choice(USER_AGENTS) if stealth_mode else DEFAULT_USER_AGENT
    }
    if stealth_mode and referrer:
        headers['Referer'] = referrer
    return headers

def normalize_url(url):
    return urlparse(url).geturl().rstrip('/')

def update_page(conn, url, title, description, keywords, favicon_id=None):
    current_time = datetime.now(UTC).isoformat()
    conn.execute('''
        UPDATE pages
        SET title = ?, description = ?, keywords = ?, last_crawled = ?, favicon_id = ?
        WHERE url = ?
    ''', (title, description, keywords, current_time, favicon_id, url))
    conn.commit()

def save_page(conn, url, title, description, keywords, favicon_id=None):
    current_time = datetime.now(UTC).isoformat()
    conn.execute('''
        INSERT INTO pages (url, title, description, keywords, priority, last_crawled, favicon_id)
        VALUES (?, ?, ?, ?, 0, ?, ?)
    ''', (url, title, description, keywords, current_time, favicon_id))
    conn.commit()

def remove_url(conn, url):
    """Remove a URL from the database."""
    conn.execute('DELETE FROM pages WHERE url = ?', (url,))
    conn.commit()
    tqdm.write(f"Removed: {url} (status: 4xx error)")

def get_favicon_url_from_html(domain):
    """Try to find a favicon URL by parsing the HTML of the home page."""
    try:
        response = requests.get(f"https://{domain}", headers=get_headers(False), timeout=5)
        soup = BeautifulSoup(response.content, "html.parser")

        # Search for <link rel="icon"> or <link rel="shortcut icon">
        icon_link = soup.find("link", rel=lambda value: value and "icon" in value.lower())
        if icon_link and icon_link.get("href"):
            return urljoin(f"https://{domain}", icon_link["href"])
    except requests.RequestException:
        pass

    # Fallback to /favicon.ico if not found
    return f"https://{domain}/favicon.ico"

def download_favicon(domain):
    """Download the favicon for a given domain."""
    favicon_url = get_favicon_url_from_html(domain)
    headers = get_headers(stealth_mode=False)

    try:
        response = requests.get(favicon_url, headers=headers, timeout=5)
        if response.status_code == 200:
            content_type = response.headers.get('Content-Type', '').lower()
            if content_type.startswith('text/html'):
                tqdm.write(f"HTML content received instead of image for {domain}")
                return None

            ext = {
                'image/png': 'png',
                'image/jpeg': 'jpg',
                'image/svg+xml': 'svg',
                'image/x-icon': 'ico',
                'image/vnd.microsoft.icon': 'ico',
                'image/webp': 'webp',
                'image/avif': 'avif'
            }.get(content_type, None)

            if ext is None:
                tqdm.write(f"Unknown favicon type for {domain}: {content_type}")
                return None

            favicon_hash = md5(favicon_url.encode()).hexdigest()
            file_path = os.path.join(FAVICON_DIR, f"{favicon_hash}.{ext}")

            with open(file_path, "wb") as f:
                f.write(response.content)

            return favicon_hash
    except requests.RequestException:
        pass

    return None

def crawl(url, session, conn, stealth_mode, retries=3):
    try:
        response = session.get(url, headers=get_headers(stealth_mode), timeout=10)
        
        # Handle different status codes
        if response.status_code == 429:  # Too Many Requests, retry later
            if retries > 0:
                tqdm.write(f"429 Too Many Requests for {url}. Retrying in 5 seconds...")
                time.sleep(5)
                return crawl(url, session, conn, stealth_mode, retries - 1)
            else:
                tqdm.write(f"Max retries reached for {url}. Skipping.")
                return
        elif 400 <= response.status_code < 500:  # Remove 4xx errors (excluding 429) from the database
            remove_url(conn, url)
            return
        elif response.status_code != 200 or 'text/html' not in response.headers.get('Content-Type', ''):
            tqdm.write(f"Skipping: {url} (status: {response.status_code})")
            return

        # Proceed with parsing and saving page data if response is successful
        soup = BeautifulSoup(response.content, 'lxml')
        title = soup.title.string if soup.title else ''
        description = soup.find('meta', attrs={'name': 'description'})
        description = description['content'] if description else ''
        keywords = soup.find('meta', attrs={'name': 'keywords'})
        keywords = keywords['content'] if keywords else ''

        domain = urlparse(url).netloc
        favicon_id = download_favicon(domain)

        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM pages WHERE url = ?', (url,))
        if cursor.fetchone():
            update_page(conn, url, title, description, keywords, favicon_id)
            tqdm.write(f"Updated: {url}")
        else:
            save_page(conn, url, title, description, keywords, favicon_id)
            tqdm.write(f"Saved: {url}")

    except Exception as e:
        tqdm.write(f"Error crawling {url}: {e}")

# --- Multithreading Logic ---
def process_url(url, stealth_mode):
    """Wrapper function for multithreading."""
    conn = connect_db()
    session = requests.Session()
    crawl(url, session, conn, stealth_mode)
    conn.close()

# --- Main Logic ---
def main():
    conn = connect_db()
    tqdm.write("Fetching stale URLs...")
    stale_urls = get_stale_urls(conn)
    conn.close()

    tqdm.write(f"Found {len(stale_urls)} URLs to re-crawl.")

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(process_url, url, True) for url in stale_urls]
        for future in tqdm(as_completed(futures), total=len(stale_urls), desc="Crawling"):
            try:
                future.result()  # Raise exceptions if any occurred during crawling
            except Exception as e:
                tqdm.write(f"Error during crawling: {e}")

    tqdm.write("Crawl complete.")

if __name__ == "__main__":
    main()