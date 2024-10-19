import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import sqlite3
from tqdm import tqdm
import os
import time
import random
from hashlib import md5
from concurrent.futures import ThreadPoolExecutor, as_completed
from PIL import Image
from io import BytesIO
import sys
import argparse

# Initialize SQLite DB
DB_PATH = "../links.db"
FAVICON_DIR = "../favicons"
os.makedirs(FAVICON_DIR, exist_ok=True)

# --- Database Functions ---
def check_db_exists():
    """Check if the database exists."""
    if not os.path.exists(DB_PATH):
        print("\033[1;33mWarning: Database does not exist.\033[0m")
        choice = input("Do you want to recreate the database? (yes/no): ").strip().lower()
        if choice == 'yes':
            create_db()
        else:
            print("Stopping API.")
            sys.exit(1)

def create_db():
    """Create the database and necessary tables."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE pages (
            id INTEGER PRIMARY KEY,
            url TEXT NOT NULL,
            title TEXT,
            description TEXT,
            keywords TEXT,
            priority INTEGER DEFAULT 0,
            favicon_id TEXT
        )
    ''')
    conn.commit()
    conn.close()
    print("Database created successfully.")

check_db_exists()  # Check DB at startup

conn = sqlite3.connect(DB_PATH)
conn.execute('PRAGMA journal_mode=WAL;')  # Enable WAL mode
c = conn.cursor()

c.execute('''
    CREATE TABLE IF NOT EXISTS pages (
        url TEXT PRIMARY KEY,
        title TEXT,
        description TEXT,
        keywords TEXT,
        favicon_id TEXT,
        priority INTEGER DEFAULT 0
    )
''')

# --- User-Agent Handling ---
DEFAULT_USER_AGENT = "NovaCrawler/1.1"

USER_AGENTS = [  # Expanded list for stealth mode
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0",
    "Dalvik/2.1.0 (Linux; U; Android 11; Pixel 3a XL Build/RQ2A.210305.006)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
]

def get_headers(stealth_mode, referrer=None):
    headers = {
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',  # Mimic browser behavior
        'DNT': '1' if stealth_mode else '0'  # Respect DNT if requested
    }

    if stealth_mode:
        headers['User-Agent'] = random.choice(USER_AGENTS)
        headers['Referer'] = referrer if referrer else 'https://www.google.com'  # Dynamic referrer or default
        headers['Cache-Control'] = 'max-age=0'
        headers['Sec-Fetch-Dest'] = 'document'
        headers['Sec-Fetch-Mode'] = 'navigate'
        headers['Sec-Fetch-Site'] = 'none'
        headers['Sec-Fetch-User'] = '?1'

    else:
        headers['User-Agent'] = DEFAULT_USER_AGENT

    return headers

def normalize_url(url):
    """Remove query parameters, fragments, and trailing slashes."""
    parsed_url = urlparse(url)
    normalized = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
    return normalized.rstrip('/')

def is_home_page(url):
    """Check if the URL is a home page."""
    parsed_url = urlparse(url)
    return parsed_url.path in ('', '/')

def update_priority(url, amount):
    """Update the priority of a page."""
    c.execute('UPDATE pages SET priority = priority + ? WHERE url = ?', (amount, url))
    conn.commit()

def save_page(url, title, description, keywords):
    """Save a new page to the database."""
    c.execute('''
        INSERT INTO pages (url, title, description, keywords, priority)
        VALUES (?, ?, ?, ?, 0)
    ''', (url, title, description, keywords))
    conn.commit()

def update_page(url, title, description, keywords):
    """Update an existing page in the database."""
    c.execute('''
        UPDATE pages
        SET title = ?, description = ?, keywords = ?
        WHERE url = ?
    ''', (title, description, keywords, url))
    conn.commit()

def get_meta_content(soup, name):
    """Extract meta tag content."""
    tag = soup.find('meta', attrs={'name': name})
    return tag['content'] if tag else ''

def is_valid_link(link):
    """Filter non-HTML links."""
    invalid_extensions = (
        '.css', '.js', '.jpg', '.jpeg', '.png', '.gif', 
        '.svg', '.woff', '.pdf', '.zip', '.mp4', '.mp3', '.exe'
    )
    return not any(link.lower().endswith(ext) for ext in invalid_extensions)

def crawl(url, max_depth, session, stealth_mode, visited=set(), saved_urls=set(), referrer=None):
    """Recursive crawler that collects metadata."""
    normalized_url = normalize_url(url)

    if max_depth == 0 or normalized_url in visited:
        return

    visited.add(normalized_url)
    print(f'Crawling: {normalized_url}')

    try:
        response = session.get(normalized_url, headers=get_headers(stealth_mode, referrer), timeout=5)

        if response.status_code != 200 or 'text/html' not in response.headers.get('Content-Type', ''):
            print(f"Skipping: {normalized_url} ({response.status_code})")
            return

        soup = BeautifulSoup(response.content, 'lxml')

        # Check for noindex meta tag
        robots_meta = soup.find('meta', attrs={'name': 'robots'})
        if robots_meta and 'noindex' in robots_meta.get('content', '').lower():
            print(f"Skipping noindex page: {normalized_url}")
            return

        title = soup.title.string if soup.title else ''
        description = get_meta_content(soup, 'description')
        keywords = get_meta_content(soup, 'keywords')

        if '404' in title or not title:
            print(f"Skipping 404 page: {normalized_url}")
            return

        c.execute('SELECT title, description, keywords FROM pages WHERE url = ?', (normalized_url,))
        row = c.fetchone()

        priority_adjustment = 5 if is_home_page(normalized_url) else 0
        priority_adjustment -= 3 if not description else 0

        if row:
            stored_title, stored_description, stored_keywords = row
            if (stored_title != title) or (stored_description != description) or (stored_keywords != keywords):
                update_page(normalized_url, title, description, keywords)
                print(f"Updated: {title} ({normalized_url})")
            update_priority(normalized_url, priority_adjustment + 1)
        else:
            save_page(normalized_url, title, description, keywords)
            saved_urls.add(normalized_url)
            print(f"Saved: {title} ({normalized_url})")
            update_priority(normalized_url, priority_adjustment)

        for link in soup.find_all('a', href=True):
            full_url = urljoin(normalized_url, link['href'])
            if is_valid_link(full_url):
                crawl(full_url, max_depth - 1, session, stealth_mode, visited, saved_urls, referrer=normalized_url)  # Pass current URL as referrer

    except Exception as e:
        print(f'Error: {url} - {e}')

def get_favicon_url_from_html(domain):
    """Try to find a favicon URL by parsing the HTML of the home page."""
    try:
        response = requests.get(f"https://{domain}", headers=get_headers(False), timeout=5)
        soup = BeautifulSoup(response.content, "html.parser")

        # Search for <link rel="icon"> or <link rel="shortcut icon">
        icon_link = soup.find("link", rel=lambda value: value and "icon" in value.lower())
        if icon_link and icon_link.get("href"):
            return urljoin(f"https://{domain}", icon_link["href"])
    except requests.RequestException as e:
        print(f"Error fetching HTML from {domain}: {e}")
    
    # Fallback to /favicon.ico if not found
    return f"https://{domain}/favicon.ico"

def download_favicon(domain):
    """Download the favicon for a given domain."""
    favicon_url = get_favicon_url_from_html(domain)
    headers = get_headers(stealth_mode=False)  # Don't need stealth for favicons

    try:
        response = requests.get(favicon_url, headers=headers, timeout=5, stream=True)  # Stream for large files
        response.raise_for_status()  # Raise an exception for bad status codes

        content_type = response.headers.get('Content-Type', '').lower()
        if content_type.startswith('text/html'):  # Check content type *before* reading content
            print(f"HTML received instead of image for {domain}")
            return domain, None

        # Identify file extension from content type
        ext = 'ico'  # Default to ICO if unknown
        if 'image/png' in content_type:
            ext = 'png'
        elif 'image/jpeg' in content_type:
            ext = 'jpg'
        elif 'image/svg+xml' in content_type:
            ext = 'svg'
        elif 'image/webp' in content_type:
            ext = 'webp'
        elif 'image/avif' in content_type:
            ext = 'avif'

        # Save the favicon with a hash-based filename
        favicon_hash = md5(favicon_url.encode()).hexdigest()
        file_path = os.path.join(FAVICON_DIR, f"{favicon_hash}.{ext}")

        with open(file_path, "wb") as f:
            f.write(response.content)

        return domain, favicon_hash
    except requests.RequestException as e:
        print(f"Failed to download favicon from {favicon_url}: {e}")

    return domain, None  # Return None if download fails

def crawl_for_favicons(saved_urls):
    """Download favicons for all saved URLs using multithreading."""
    domains = {urlparse(url).netloc for url in saved_urls}

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(download_favicon, domain): domain for domain in domains}

        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing favicons"):
            domain, favicon_id = future.result()
            if favicon_id:
                print(f"Downloaded favicon for {domain}")

                c.execute('''
                    UPDATE pages SET favicon_id = ?
                    WHERE url LIKE ?
                ''', (favicon_id, f'%{domain}%'))
                conn.commit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Web crawler with favicon downloader.")
    parser.add_argument("-u", "--url", help="URL to start crawling")
    parser.add_argument("-d", "--depth", type=int, help="Crawl depth")
    parser.add_argument("-s", "--stealth", action="store_true", help="Enable stealth mode (random user-agents)") 
    args = parser.parse_args()

    session = requests.Session()
    saved_urls = set()

    print("Starting crawl...")
    crawl(args.url, args.depth, session, args.stealth, saved_urls=saved_urls)
    print("Crawl complete.")

    print("Starting favicon crawl...")
    crawl_for_favicons(saved_urls)
    print("Favicon crawl complete.")
    conn.close()