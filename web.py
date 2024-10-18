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

# Initialize SQLite DB
DB_PATH = "../links.db"
FAVICON_DIR = "../favicons"
os.makedirs(FAVICON_DIR, exist_ok=True)
conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

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

# Check if the database exists at startup
check_db_exists()

# Create table with extra metadata and priority field
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

# User Agent Pool
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0"
]

def get_random_headers():
    """Return headers mimicking a real browser."""
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Connection': 'keep-alive'
    }

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

def crawl(url, max_depth, session, visited=set(), saved_urls=set()):
    """Recursive crawler that collects metadata."""
    normalized_url = normalize_url(url)

    if max_depth == 0 or normalized_url in visited:
        return

    visited.add(normalized_url)
    print(f'Crawling: {normalized_url}')

    try:
        response = session.get(normalized_url, headers=get_random_headers(), timeout=5)

        if response.status_code != 200 or 'text/html' not in response.headers.get('Content-Type', ''):
            print(f"Skipping: {normalized_url} ({response.status_code})")
            return

        soup = BeautifulSoup(response.content, 'lxml')

        title = soup.title.string if soup.title else ''
        description = get_meta_content(soup, 'description')
        keywords = get_meta_content(soup, 'keywords')

        if '404' in title or not title:
            print(f"Skipping 404 page: {normalized_url}")
            return

        c.execute('SELECT 1 FROM pages WHERE url = ?', (normalized_url,))
        exists = c.fetchone()

        priority_adjustment = 5 if is_home_page(normalized_url) else 0
        priority_adjustment -= 3 if not description else 0

        if exists:
            update_priority(normalized_url, priority_adjustment + 1)
        else:
            save_page(normalized_url, title, description, keywords)
            saved_urls.add(normalized_url)
            print(f"Saved: {title} ({normalized_url})")
            update_priority(normalized_url, priority_adjustment)

        for link in soup.find_all('a', href=True):
            full_url = urljoin(normalized_url, link['href'])
            if is_valid_link(full_url):
                crawl(full_url, max_depth - 1, session, visited, saved_urls)

    except Exception as e:
        print(f'Error: {url} - {e}')

def get_favicon_url_from_html(domain):
    """Try to find a favicon URL by parsing the HTML of the home page."""
    try:
        response = requests.get(f"https://{domain}", headers=get_random_headers(), timeout=5)
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
    headers = get_random_headers()

    try:
        response = requests.get(favicon_url, headers=headers, timeout=5)
        if response.status_code == 200:
            content_type = response.headers.get('Content-Type', '').lower()

            # Skip if the content is HTML (likely incorrect response)
            if content_type.startswith('text/html'):
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

                # Update the database with the favicon ID for this domain
                c.execute('''
                    UPDATE pages SET favicon_id = ?
                    WHERE url LIKE ?
                ''', (favicon_id, f'%{domain}%'))
                conn.commit()

if __name__ == "__main__":
    session = requests.Session()  # Reuse session for cookies
    start_url = input("Enter a URL to crawl: ")
    depth = int(input("Enter crawl depth: "))
    saved_urls = set()
    print("Starting crawl...")
    crawl(start_url, depth, session, saved_urls=saved_urls)
    print("Crawl complete.")
    print("Starting favicon crawl...")
    crawl_for_favicons(saved_urls)
    print("Favicon crawl complete.")