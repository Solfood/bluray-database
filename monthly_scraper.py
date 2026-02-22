import requests
from bs4 import BeautifulSoup
import json
import time
import os
import re
from datetime import datetime, date
from urllib.parse import unquote, urlsplit

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE_DIR, "upc")
STATE_FILE = os.path.join(BASE_DIR, "state_monthly.json")
MOVIE_URL_RE = re.compile(r"/movies/[^/]+-Blu-ray/(\d+)/$")

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
    return {"last_run": None, "processed_ids": []}

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

def save_movie(movie_data):
    upc = movie_data.get('upc')
    if not upc:
        return
        
    upc = re.sub(r'\D', '', str(upc))
    if len(upc) < 3:
        upc = upc.zfill(3)
    movie_data['upc'] = upc
        
    chunk_path = os.path.join(DB_DIR, upc[0], upc[1], upc[2])
    os.makedirs(chunk_path, exist_ok=True)
    
    file_path = os.path.join(chunk_path, f"{upc}.json")
    with open(file_path, 'w') as f:
        json.dump(movie_data, f, indent=2)

def scrape_movie_detail(url):
    try:
        res = fetch_with_retry(url)
        if not res or res.status_code != 200:
            return None
            
        soup = BeautifulSoup(res.text, 'html.parser')
        title_full = soup.title.string.replace("Blu-ray", "").strip() if soup.title else ""
        upc_match = re.search(r'(UPC|EAN):\s*(\d+)', res.text)
        year_match = re.search(r'\b(19\d{2}|20\d{2})\b', title_full)
        
        upc = upc_match.group(2) if upc_match else None
        year = year_match.group(1) if year_match else None
        
        if upc:
            return {
                "upc": upc,
                "title": title_full.split("(")[0].strip(),
                "edition": title_full,
                "year": year,
                "bluray_url": url,
                "scraped_at": datetime.utcnow().isoformat()
            }
    except Exception as e:
        print(f"Error scraping detail {url}: {e}")
    return None

def normalize_movie_url(href):
    if not href:
        return None

    href = unquote(href)
    if 'url=' in href:
        href = href.split('url=', 1)[1]
        href = unquote(href)

    if href.startswith('/'):
        href = f"https://www.blu-ray.com{href}"
    elif href.startswith('//'):
        href = f"https:{href}"
    elif not href.startswith('http://') and not href.startswith('https://'):
        return None

    parsed = urlsplit(href)
    if parsed.netloc not in ("www.blu-ray.com", "blu-ray.com"):
        return None

    clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    match = MOVIE_URL_RE.search(parsed.path)
    if not match:
        return None
    return clean_url, match.group(1)

def extract_index_movies(soup):
    candidates = []
    for link in soup.find_all('a', href=True):
        normalized = normalize_movie_url(link['href'])
        if normalized:
            candidates.append(normalized)

    deduped = {}
    for url, movie_id in candidates:
        deduped.setdefault(movie_id, url)

    ordered_ids = sorted(deduped.keys(), key=int)
    return [(deduped[movie_id], movie_id) for movie_id in ordered_ids]

def run_monthly_update():
    print("Starting Phase B: Monthly Update Scraper...")
    state = load_state()
    processed_ids = set(state.get('processed_ids', []))
    
    # Target: New Releases calendar. 
    calendar_url = "https://www.blu-ray.com/movies/movies.php?show=newreleases"
    
    print(f"Fetching calendar: {calendar_url}")
    res = fetch_with_retry(calendar_url)
    
    if not res or res.status_code != 200:
        print(f"Failed to fetch calendar.")
        return
        
    soup = BeautifulSoup(res.text, 'html.parser')
    movies = extract_index_movies(soup)
    print(f"Found {len(movies)} recent releases on the calendar.")
    
    new_additions = 0
    for m_url, m_id in movies:
        
        if m_id in processed_ids:
            continue
            
        print(f"New Release Found! Scraping {m_url}...")
        movie_data = scrape_movie_detail(m_url)
        
        if movie_data:
            print(f"  -> Added UPC: {movie_data['upc']} - {movie_data['title']}")
            save_movie(movie_data)
            new_additions += 1
            
        processed_ids.add(m_id)
        state['processed_ids'] = list(processed_ids)
        state['last_run'] = datetime.utcnow().isoformat()
        save_state(state)
        
        time.sleep(2) # Play nice with rate limits
        
    print(f"\nMonthly Update Complete. Added {new_additions} new items to DB.")

if __name__ == "__main__":
    run_monthly_update()
