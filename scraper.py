import requests
from bs4 import BeautifulSoup
import json
import time
import os
import re
from datetime import datetime
from urllib.parse import unquote, urlsplit

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE_DIR, "upc")
STATE_FILE = os.path.join(BASE_DIR, "state.json")
MOVIE_URL_RE = re.compile(r"/movies/[^/]+-Blu-ray/(\d+)/$")
MAX_RETRIES_PER_MOVIE = 3
MAX_REPEAT_PAGES = 6
MAX_NO_NEW_ID_PAGES = 80
MAX_PAGES_THIS_RUN = int(os.environ.get("MAX_PAGES_THIS_RUN", "0"))

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
    default_state = {
        "last_page": 1,
        "attempted_ids": [],
        "saved_ids": [],
        "failed_attempts": {},
        "last_page_signature": None,
        "repeat_signature_pages": 0,
        "no_new_ids_pages": 0,
        "stats": {
            "pages_scanned": 0,
            "movies_saved": 0,
            "last_run_started_at": None,
            "last_run_finished_at": None,
            "last_stop_reason": None
        }
    }

    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            loaded = json.load(f)
        # Backward compatibility with older state shape.
        loaded.setdefault("attempted_ids", loaded.get("processed_ids", []))
        loaded.setdefault("saved_ids", [])
        loaded.setdefault("failed_attempts", {})
        loaded.setdefault("last_page_signature", None)
        loaded.setdefault("repeat_signature_pages", 0)
        loaded.setdefault("no_new_ids_pages", 0)
        loaded.setdefault("stats", default_state["stats"])
        loaded["stats"].setdefault("pages_scanned", 0)
        loaded["stats"].setdefault("movies_saved", 0)
        loaded["stats"].setdefault("last_run_started_at", None)
        loaded["stats"].setdefault("last_run_finished_at", None)
        loaded["stats"].setdefault("last_stop_reason", None)
        return loaded
    return default_state

def save_state(state):
    # Keep legacy key for compatibility with older tooling.
    state["processed_ids"] = state.get("attempted_ids", [])
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

    # Keep first seen URL for each ID to avoid duplicate processing.
    deduped = {}
    for url, movie_id in candidates:
        deduped.setdefault(movie_id, url)

    ordered_ids = sorted(deduped.keys(), key=int)
    movies = [(deduped[movie_id], movie_id) for movie_id in ordered_ids]
    return movies

def run_backfill():
    """Phase A: Scrapes the 'All Movies' sorted index with stop guards."""
    print("Starting Phase A: Historical Backfill...")
    state = load_state()
    current_page = state.get('last_page', 1)
    attempted_ids = set(state.get('attempted_ids', []))
    saved_ids = set(state.get('saved_ids', []))
    failed_attempts = dict(state.get('failed_attempts', {}))
    last_page_signature = state.get('last_page_signature')
    repeat_signature_pages = int(state.get('repeat_signature_pages', 0))
    no_new_ids_pages = int(state.get('no_new_ids_pages', 0))
    state["stats"]["last_run_started_at"] = datetime.utcnow().isoformat()
    state["stats"]["last_stop_reason"] = None
    save_state(state)
    stop_reason = None
    
    while True:
        if MAX_PAGES_THIS_RUN > 0 and state["stats"].get("pages_scanned", 0) >= MAX_PAGES_THIS_RUN:
            stop_reason = f"run_budget_reached:{MAX_PAGES_THIS_RUN}_pages"
            print(f"Stopping: run budget reached ({MAX_PAGES_THIS_RUN} pages).")
            break

        print(f"\n--- Scraping Index Page {current_page} ---")
        # Example list URL for blu-ray.com (Popular/All movies)
        # Note: You have to find a valid index URL. E.g., The main search page with no query sorts by popularity
        url = f"https://www.blu-ray.com/movies/movies.php?show=all&page={current_page}"
        
        res = fetch_with_retry(url)
        if not res or res.status_code != 200:
            print(f"Index fetch failed for page {current_page}.")
            stop_reason = f"index_fetch_failed:{current_page}"
            break
            
        soup = BeautifulSoup(res.text, 'html.parser')

        movies_on_page = extract_index_movies(soup)
        movie_ids_on_page = [movie_id for _, movie_id in movies_on_page]
        page_signature = ",".join(movie_ids_on_page)

        if page_signature and page_signature == last_page_signature:
            repeat_signature_pages += 1
        else:
            repeat_signature_pages = 0
        last_page_signature = page_signature

        pending = [(url, movie_id) for url, movie_id in movies_on_page if movie_id not in attempted_ids]
        new_ids_count = len(pending)

        if new_ids_count == 0:
            no_new_ids_pages += 1
        else:
            no_new_ids_pages = 0

        print(
            f"Found {len(movies_on_page)} movie links on page {current_page}. "
            f"New IDs this page: {new_ids_count}. "
            f"repeat_signature_pages={repeat_signature_pages}, no_new_ids_pages={no_new_ids_pages}"
        )

        if repeat_signature_pages >= MAX_REPEAT_PAGES:
            stop_reason = f"repeating_page_signature_at:{current_page}"
            print(f"Stopping: repeated page signature reached {repeat_signature_pages} pages.")
            break

        if no_new_ids_pages >= MAX_NO_NEW_ID_PAGES:
            stop_reason = f"no_new_ids_for_pages:{no_new_ids_pages}"
            print(f"Stopping: no new IDs for {no_new_ids_pages} consecutive pages.")
            break

        saved_this_page = 0
        for m_url, m_id in pending:
            print(f"Scraping {m_url}...")
            movie_data = scrape_movie_detail(m_url)
            
            if movie_data:
                print(f"  -> Found UPC: {movie_data['upc']} - {movie_data['title']}")
                save_movie(movie_data)
                saved_ids.add(m_id)
                attempted_ids.add(m_id)
                failed_attempts.pop(m_id, None)
                saved_this_page += 1
            else:
                attempts = int(failed_attempts.get(m_id, 0)) + 1
                failed_attempts[m_id] = attempts
                if attempts >= MAX_RETRIES_PER_MOVIE:
                    attempted_ids.add(m_id)

            state['attempted_ids'] = list(attempted_ids)
            state['saved_ids'] = list(saved_ids)
            state['failed_attempts'] = failed_attempts
            save_state(state)
            
            # DELAY to prevent IP Ban
            time.sleep(2)

        print(
            f"Page {current_page} complete. "
            f"Saved this page: {saved_this_page}. "
            f"Total saved IDs: {len(saved_ids)}. "
            f"Attempted IDs: {len(attempted_ids)}."
        )
            
        current_page += 1
        state['last_page'] = current_page
        state['attempted_ids'] = list(attempted_ids)
        state['saved_ids'] = list(saved_ids)
        state['failed_attempts'] = failed_attempts
        state['last_page_signature'] = last_page_signature
        state['repeat_signature_pages'] = repeat_signature_pages
        state['no_new_ids_pages'] = no_new_ids_pages
        state['stats']['pages_scanned'] = int(state['stats'].get('pages_scanned', 0)) + 1
        state['stats']['movies_saved'] = len(saved_ids)
        save_state(state)

    state['attempted_ids'] = list(attempted_ids)
    state['saved_ids'] = list(saved_ids)
    state['failed_attempts'] = failed_attempts
    state['last_page_signature'] = last_page_signature
    state['repeat_signature_pages'] = repeat_signature_pages
    state['no_new_ids_pages'] = no_new_ids_pages
    state['stats']['movies_saved'] = len(saved_ids)
    state['stats']['last_run_finished_at'] = datetime.utcnow().isoformat()
    state['stats']['last_stop_reason'] = stop_reason or "manual_or_unknown"
    save_state(state)

    print("\nBackfill Complete.")
    print(f"Stop reason: {state['stats']['last_stop_reason']}")
    print(f"Saved IDs: {len(saved_ids)}")

if __name__ == "__main__":
    run_backfill()
