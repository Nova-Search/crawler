# Used for updating the favicon IDs in the database and downloading missing ones; don't run unless you need to

import sqlite3
import requests
import os
from urllib.parse import urlparse, urljoin
from hashlib import md5
from bs4 import BeautifulSoup  # Install with: pip install beautifulsoup4
from tqdm import tqdm  # Install with: pip install tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from PIL import Image  # Install with: pip install pillow
from io import BytesIO

DB_PATH = "../links.db"
FAVICON_DIR = "../favicons"
os.makedirs(FAVICON_DIR, exist_ok=True)

def confirm_execution():
    """Ask the user for confirmation before proceeding."""
    while True:
        response = input("Are you sure you want to run this script? (yes/no): ").strip().lower()
        if response in ["yes", "no"]:
            return response == "yes"
        print("Please respond with 'yes' or 'no'.")

if not confirm_execution():
    print("Execution cancelled.")
    exit(0)

def get_db_connection():
    """Establish a new database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def extract_domain(url):
    """Extract the domain or subdomain from a URL."""
    parsed_url = urlparse(url)
    return parsed_url.netloc

def get_favicon_url_from_html(domain):
    """Try to find a favicon URL by parsing the HTML of the page."""
    try:
        response = requests.get(f"https://{domain}", timeout=5)
        soup = BeautifulSoup(response.content, "html.parser")
        
        # Look for <link rel="icon" or <link rel="shortcut icon">
        icon_link = soup.find("link", rel=lambda value: value and "icon" in value.lower())
        if icon_link and icon_link.get("href"):
            return urljoin(f"https://{domain}", icon_link["href"])
    except requests.RequestException:
        pass  # Fail silently, fallback will be used

    # Default to /favicon.ico if no <link rel="icon"> is found
    return f"https://{domain}/favicon.ico"

def convert_to_ico(image_content, output_path):
    """Convert an image to ICO format and save it."""
    with Image.open(BytesIO(image_content)) as img:
        img.save(output_path, format='ICO')

def download_favicon(domain):
    """Download the favicon for a given domain."""
    favicon_url = get_favicon_url_from_html(domain)
    headers = {'User-Agent': 'NovaSearchCrawler/1.0'}

    try:
        response = requests.get(favicon_url, headers=headers, timeout=5)
        if response.status_code == 200:
            content_type = response.headers.get('Content-Type', '').lower()

            # Skip if the content is HTML
            if content_type.startswith('text/html'):
                print(f"HTML content received instead of image for {domain}")
                return domain, None

            # Determine the appropriate file extension
            if 'image/png' in content_type:
                ext = 'png'
            elif 'image/jpeg' in content_type:
                ext = 'jpg'
            elif 'image/svg+xml' in content_type:
                ext = 'svg'
            elif 'image/x-icon' in content_type or 'image/vnd.microsoft.icon' in content_type:
                ext = 'ico'
            elif 'image/webp' in content_type:
                ext = 'webp'
            elif 'image/avif' in content_type:
                ext = 'avif'
            else:
                print(f"Unknown favicon type for {domain}: {content_type}")
                return domain, None  # Skip unknown types

            favicon_hash = md5(favicon_url.encode()).hexdigest()
            file_path = os.path.join(FAVICON_DIR, f"{favicon_hash}.{ext}")

            # Save the favicon directly
            with open(file_path, "wb") as f:
                f.write(response.content)

            return domain, favicon_hash
    except requests.RequestException:
        pass  # Fail silently if the request fails

    return domain, None  # Return None if download fails

def batch_update_favicon_ids(updates):
    """Batch update the favicon IDs in the database."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.executemany(
            "UPDATE pages SET favicon_id = ? WHERE url LIKE ?", updates
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"Database update error: {e}")
    finally:
        conn.close()

def crawl_for_favicons():
    """Main function to crawl and update favicons using multithreading."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get all unique domains/subdomains from the database
    cursor.execute("SELECT DISTINCT url FROM pages")
    urls = cursor.fetchall()
    conn.close()

    processed_domains = set()
    updates = []  # Store updates to batch later

    # Extract domains and skip duplicates
    domains = {extract_domain(row["url"]) for row in urls}

    # Use ThreadPoolExecutor for multithreading
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(download_favicon, domain): domain for domain in domains}

        # Use tqdm to track progress
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing domains", unit="domain"):
            domain, favicon_id = future.result()
            if favicon_id:
                updates.append((favicon_id, f"%{domain}%"))

    # Batch update all favicon IDs in the database
    if updates:
        batch_update_favicon_ids(updates)

if __name__ == "__main__":
    confirm_execution()
    crawl_for_favicons()