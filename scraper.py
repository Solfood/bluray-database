import requests
from bs4 import BeautifulSoup
import json
import time
import os
import re

DB_DIR = "upc"
STATE_FILE = "state.json"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
}

def fetch_with_retry(url, max_retries=5, backoff_factor=2):
    """Fetches a URL with exponential backoff on connection errors/timeouts."""
    for attempt in range(max_retries):
        try:
            res = requests.get(url, headers=HEADERS, timeout=15)
            return res
        except requests.exceptions.RequestException as e:
            wait_time = backoff_factor ** attempt
            print(f"Connection error: {e}. Retrying in {wait_time}s...")
            time.sleep(wait_time)
    print(f"Failed to fetch {url} after {max_retries} retries.")
    return None

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {"last_page": 1, "processed_ids": []}

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

def save_movie(movie_data):
    """Saves movie data in a chunked directory structure based on UPC."""
    upc = movie_data.get('upc')
    if not upc:
        return
        
    # Edge case hardening: Strip non-numeric and ensure at least 3 chars
    upc = re.sub(r'\D', '', str(upc))
    if len(upc) < 3:
        upc = upc.zfill(3)
    movie_data['upc'] = upc # Ensure cleaned UPC is saved
        
    # Create chunked path: upc/0/4/3/0433...json
    chunk_path = os.path.join(DB_DIR, upc[0], upc[1], upc[2])
    os.makedirs(chunk_path, exist_ok=True)
    
    file_path = os.path.join(chunk_path, f"{upc}.json")
    with open(file_path, 'w') as f:
        json.dump(movie_data, f, indent=2)

def scrape_movie_detail(url):
    """Scrapes a specific movie page to extract the UPC."""
    try:
        res = fetch_with_retry(url)
        if not res or res.status_code != 200:
            print(f"Failed to get {url}")
            return None
            
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # Title is usually in an h1 or title tag
        # Example format: Movie Title Blu-ray (Edition string)
        title_full = soup.title.string.replace("Blu-ray", "").strip() if soup.title else ""
        
        # The UPC/EAN is often listed in the product details section
        # Look for text matching "EAN:" or "UPC:" 
        upc_match = re.search(r'(UPC|EAN):\s*(\d+)', res.text)
        year_match = re.search(r'\b(19\d{2}|20\d{2})\b', title_full)
        
        upc = upc_match.group(2) if upc_match else None
        year = year_match.group(1) if year_match else None
        
        if upc:
            return {
                "upc": upc,
                "title": title_full.split("(")[0].strip(), # Basic clean title
                "edition": title_full,
                "year": year,
                "bluray_url": url
            }
            
    except Exception as e:
        print(f"Error scraping detail {url}: {e}")
    return None

def run_backfill():
    """Phase A: Scrapes the 'All Movies' sorted index."""
    print("Starting Phase A: Historical Backfill...")
    state = load_state()
    current_page = state.get('last_page', 1)
    processed_ids = set(state.get('processed_ids', []))
    
    while True:
        print(f"\n--- Scraping Index Page {current_page} ---")
        # Example list URL for blu-ray.com (Popular/All movies)
        # Note: You have to find a valid index URL. E.g., The main search page with no query sorts by popularity
        url = f"https://www.blu-ray.com/movies/movies.php?show=all&page={current_page}"
        
        res = fetch_with_retry(url)
        if not res or res.status_code != 200:
            print(f"Index fetch failed for page {current_page}.")
            break
            
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # Find movie links. Usually they look like /movies/Movie-Name-Blu-ray/12345/
        all_links = soup.find_all('a', href=True)
        movie_urls = []
        
        for l in all_links:
            href = l['href']
            # Unwrap link.php wrapper
            if 'url=' in href:
                href = href.split('url=')[1]
                
            # Must be a movie detail page with an ID at the end
            if re.search(r'/movies/[a-zA-Z0-9-]+-Blu-ray/\d+/$', href):
                # Ensure it's absolute
                if href.startswith('/'):
                    href = f"https://www.blu-ray.com{href}"
                movie_urls.append(href)
                
        # Deduplicate
        movie_urls = list(set(movie_urls))
        
        print(f"Found {len(movie_urls)} movie links on page {current_page}.")
        
        for m_url in movie_urls:
            m_id = m_url.split('/')[-2]
            if m_id in processed_ids:
                continue
                
            print(f"Scraping {m_url}...")
            movie_data = scrape_movie_detail(m_url)
            
            if movie_data:
                print(f"  -> Found UPC: {movie_data['upc']} - {movie_data['title']}")
                save_movie(movie_data)
                
            processed_ids.add(m_id)
            state['processed_ids'] = list(processed_ids)
            save_state(state)
            
            # DELAY to prevent IP Ban
            time.sleep(2)
            
        current_page += 1
        state['last_page'] = current_page
        save_state(state)
        
    print("\nBackfill Test Complete!")

if __name__ == "__main__":
    run_backfill()
