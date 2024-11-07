import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import sqlite3
from tqdm import tqdm
import os
import random
from hashlib import md5
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import argparse
from datetime import datetime, UTC
from contextlib import contextmanager
import brotli

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
            print("Stopping crawler.")
            sys.exit(1)

def create_db():
    """Create the database and necessary tables."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE pages (
                url TEXT PRIMARY KEY,
                title TEXT,
                description TEXT,
                keywords TEXT,
                favicon_id TEXT,
                priority INTEGER DEFAULT 0,
                last_crawled TIMESTAMP
            )
        ''')
        conn.commit()

check_db_exists()  # Check DB at startup

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
        headers['Referer'] = referrer if referrer else 'https://novasearch.xyz'  # Dynamic referrer or default
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
    """Thread-safe priority update."""
    with get_db() as db:
        db.execute('UPDATE pages SET priority = priority + ? WHERE url = ?', (amount, url))

def save_page(url, title, description, keywords):
    """Save a new page to the database with timestamp."""
    with get_db() as db:
        current_time = datetime.now(UTC).isoformat()
        db.execute('''
            INSERT INTO pages (url, title, description, keywords, priority, last_crawled)
            VALUES (?, ?, ?, ?, 0, ?)
        ''', (url, title, description, keywords, current_time))

def update_page(url, title, description, keywords):
    """Update an existing page in the database with new timestamp."""
    with get_db() as db:
        current_time = datetime.now(UTC).isoformat()
        db.execute('''
            UPDATE pages
            SET title = ?, 
                description = ?, 
                keywords = ?,
                last_crawled = ?
            WHERE url = ?
        ''', (title, description, keywords, current_time, url))

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

MAX_THREADS = 50  # Adjust based on system capabilities
thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

@contextmanager
def get_db():
    """Thread-safe database connection context manager"""
    db = sqlite3.connect(DB_PATH)
    db.execute('PRAGMA journal_mode=WAL;')
    try:
        yield db
    finally:
        db.commit()
        db.close()

def thread_safe_update_page(url, title, description, keywords):
    """Thread-safe database update"""
    with get_db() as db:
        current_time = datetime.now(UTC).isoformat()
        db.execute('''
            UPDATE pages
            SET title = ?, description = ?, keywords = ?, last_crawled = ?
            WHERE url = ?
        ''', (title, description, keywords, current_time, url))

def thread_safe_save_page(url, title, description, keywords):
    """Thread-safe database insert"""
    with get_db() as db:
        current_time = datetime.now(UTC).isoformat()
        db.execute('''
            INSERT INTO pages (url, title, description, keywords, priority, last_crawled)
            VALUES (?, ?, ?, ?, 0, ?)
        ''', (url, title, description, keywords, current_time))

saved_urls = set()
saved_urls_lock = threading.Lock()

def crawl_page(url, stealth_mode, referrer=None):
    session = get_session()
    normalized_url = normalize_url(url)
    links = set()
    
    try:
        headers = get_headers(stealth_mode, referrer)
        headers['Accept-Encoding'] = 'gzip, deflate, br'
        
        response = session.get(
            normalized_url, 
            headers=headers,
            timeout=5
        )

        if response.status_code != 200:
            return links

        try:
            content = response.text
        except Exception as e:
            try:
                content = brotli.decompress(response.content).decode('utf-8')
            except Exception as be:
                content = response.content.decode('utf-8', errors='ignore')

        soup = BeautifulSoup(content, 'lxml')
        
        # Extract page data
        title = soup.title.string if soup.title else ''
        description = get_meta_content(soup, 'description')
        keywords = get_meta_content(soup, 'keywords')

        if not description:
            text_elements = soup.find_all(['p', 'pre'])
            text_content = ' '.join(element.get_text() for element in text_elements)
            description = text_content[:200]

        if '404' in title:
            print(f"Skipping 404 page: {normalized_url} (found 404 in title)")
            return

        c.execute('SELECT title, description, keywords, last_crawled FROM pages WHERE url = ?', (normalized_url,))
        row = c.fetchone()

        priority_adjustment = 5 if is_home_page(normalized_url) else 0
        priority_adjustment -= 5 if not title else 0
        priority_adjustment -= 3 if not description else 0
        priority_adjustment += 1 if keywords else 0

        if row:
            stored_title, stored_description, stored_keywords, last_crawled = row
            if (stored_title != title) or (stored_description != description) or (stored_keywords != keywords):
                update_page(normalized_url, title, description, keywords)
                tqdm.write(f"Updated: {normalized_url}")
            else:
                save_page(normalized_url, title, description, keywords)
                tqdm.write(f"Saved: {normalized_url}")

        # Continue with link collection
        links_found = soup.find_all('a', href=True)
        tqdm.write(f"Found {len(links_found)} links on {normalized_url}")

        base_domain = urlparse(normalized_url).netloc
        for link in links_found:
            try:
                full_url = urljoin(normalized_url, link['href'])
                parsed = urlparse(full_url)
                
                if (parsed.scheme in ('http', 'https') and 
                    is_valid_link(full_url) and
                    parsed.netloc == base_domain):
                    normalized_link = normalize_url(full_url)
                    links.add(normalized_link)
                    tqdm.write(f"Added: {normalized_link}")
            except Exception as e:
                pass  # Silently skip invalid links

        return links
        
    except Exception as e:
        tqdm.write(f'Error crawling {url}: {str(e)}')
        return set()

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
    try:
        # Try multiple potential favicon locations
        favicon_url = get_favicon_url_from_html(domain)
        if not favicon_url:
            favicon_url = f"https://{domain}/favicon.ico"  # Fallback
        
        response = requests.get(favicon_url, timeout=5)
        if response.status_code != 200:
            tqdm.write(f"No favicon found for {domain}")
            return None

        # Generate unique filename
        favicon_hash = md5(favicon_url.encode()).hexdigest()
        ext = 'ico'  # Default extension
        
        # Get correct extension from content type
        content_type = response.headers.get('Content-Type', '').lower()
        if 'png' in content_type:
            ext = 'png'
        elif 'jpg' in content_type or 'jpeg' in content_type:
            ext = 'jpg'
        elif 'svg' in content_type:
            ext = 'svg'
        
        # Save favicon
        file_path = os.path.join(FAVICON_DIR, f"{favicon_hash}.{ext}")
        with open(file_path, 'wb') as f:
            f.write(response.content)
        
        tqdm.write(f"Downloaded favicon for {domain}")
        return favicon_hash

    except Exception as e:
        tqdm.write(f"Error downloading favicon for {domain}: {e}")
        return None

def crawl_for_favicons(saved_urls):
    """Download favicons for all saved URLs."""
    domains = {urlparse(url).netloc for url in saved_urls}
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Create a dictionary to track domain-future pairs
        future_to_domain = {}
        
        # Submit jobs and track domains
        for domain in domains:
            future = executor.submit(download_favicon, domain)
            future_to_domain[future] = domain
        
        # Process results
        for future in tqdm(as_completed(future_to_domain), total=len(future_to_domain), desc="Downloading favicons"):
            domain = future_to_domain[future]
            try:
                favicon_hash = future.result()
                if favicon_hash:
                    with get_db() as db:
                        db.execute('UPDATE pages SET favicon_id = ? WHERE url LIKE ?', 
                                 (favicon_hash, f'%{domain}%'))
            except Exception as e:
                tqdm.write(f"Error downloading favicon for {domain}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Multithreaded web crawler with favicon downloader.")
    parser.add_argument("-u", "--url", help="URL to start crawling", required=True)
    parser.add_argument("-d", "--depth", type=int, default=2, help="Crawl depth")
    parser.add_argument("-s", "--stealth", action="store_true", help="Enable stealth mode")
    args = parser.parse_args()

    visited = set()
    to_visit = {args.url}
    all_found_urls = set()  # Track all URLs found during crawl

    for depth in range(args.depth):
        if not to_visit:
            break
            
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = {executor.submit(crawl_page, url, args.stealth): url 
                      for url in to_visit if url not in visited}
            
            new_urls = set()
            for future in tqdm(as_completed(futures), total=len(futures), 
                             desc=f"Crawling depth {depth + 1}/{args.depth}"):
                url = futures[future]
                visited.add(url)
                found_urls = future.result()
                new_urls.update(found_urls)
                all_found_urls.update(found_urls)  # Add to master list
                all_found_urls.add(url)  # Add current URL too

            to_visit = new_urls - visited

    print("\nStarting favicon download...")
    crawl_for_favicons(all_found_urls)
    print("Crawl complete.")

if __name__ == "__main__":
    main()