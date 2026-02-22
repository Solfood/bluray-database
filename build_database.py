import hashlib
import json
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_UPC_DIR = os.path.join(BASE_DIR, "upc")
DB_DIR = os.path.join(BASE_DIR, "db")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")

MOVIE_BY_ID_PATH = os.path.join(DB_DIR, "movie_by_id.json")
UPC_INDEX_DB_PATH = os.path.join(DB_DIR, "upc_index.json")
TITLE_YEAR_INDEX_PATH = os.path.join(DB_DIR, "title_year_index.json")
REVIEW_QUEUE_PATH = os.path.join(DB_DIR, "review_queue.json")
STAGING_PATH = os.path.join(DB_DIR, "staging_candidates.json")
MANIFEST_PATH = os.path.join(DB_DIR, "manifest.json")
LATEST_REPORT_PATH = os.path.join(REPORTS_DIR, "latest.json")

# Compatibility outputs currently used by the web app.
ROOT_UPC_INDEX_PATH = os.path.join(BASE_DIR, "upc_index.json")
ROOT_TITLE_INDEX_PATH = os.path.join(BASE_DIR, "title_index.json")


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def normalize_title(value):
    cleaned = (value or "").lower()
    out = []
    for ch in cleaned:
        if ch.isalnum():
            out.append(ch)
        else:
            out.append(" ")
    return " ".join("".join(out).split())


def normalize_upc(value):
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits


def safe_year(value):
    if value is None:
        return None
    text = str(value).strip()
    if len(text) >= 4 and text[:4].isdigit():
        return int(text[:4])
    return None


def iter_raw_records():
    if not os.path.isdir(RAW_UPC_DIR):
        return
    for root, _, files in os.walk(RAW_UPC_DIR):
        for name in files:
            if not name.endswith(".json"):
                continue
            path = os.path.join(root, name)
            try:
                with open(path, "r") as f:
                    payload = json.load(f)
            except Exception:
                continue
            yield path, payload


def movie_id_for(payload):
    tmdb_id = payload.get("tmdb_id")
    if isinstance(tmdb_id, int):
        return f"tmdb_{tmdb_id}"

    title_key = normalize_title(payload.get("title") or "")
    year = safe_year(payload.get("year"))
    digest = hashlib.sha1(f"{title_key}|{year or 0}".encode("utf-8")).hexdigest()[:16]
    return f"mov_{digest}"


def choose_primary(counter, fallback):
    if not counter:
        return fallback
    return counter.most_common(1)[0][0]


def file_sha256(path):
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def ensure_dirs():
    os.makedirs(DB_DIR, exist_ok=True)
    os.makedirs(REPORTS_DIR, exist_ok=True)


def build_database():
    ensure_dirs()
    generated_at = utc_now()

    records = {}
    upc_to_movie_ids = defaultdict(set)
    review_queue = []
    ingestion_count = 0
    invalid_count = 0

    for path, raw in iter_raw_records() or []:
        ingestion_count += 1
        upc = normalize_upc(raw.get("upc"))
        title = (raw.get("title") or "").strip()
        year = safe_year(raw.get("year"))
        edition = (raw.get("edition") or "").strip()
        bluray_url = (raw.get("bluray_url") or "").strip()
        scraped_at = raw.get("scraped_at")

        if not upc or not title:
            invalid_count += 1
            review_queue.append({
                "type": "invalid_raw_record",
                "reason": "missing_upc_or_title",
                "path": os.path.relpath(path, BASE_DIR),
                "payload": raw,
            })
            continue

        movie_id = movie_id_for({
            "title": title,
            "year": year,
            "tmdb_id": raw.get("tmdb_id"),
        })
        rec = records.setdefault(
            movie_id,
            {
                "movie_id": movie_id,
                "tmdb_id": raw.get("tmdb_id") if isinstance(raw.get("tmdb_id"), int) else None,
                "status": "needs_review",
                "title_counts": Counter(),
                "year_counts": Counter(),
                "upc": set(),
                "editions": set(),
                "bluray_urls": set(),
                "sources": [],
                "last_seen_at": generated_at,
            },
        )

        rec["title_counts"][title] += 1
        if year:
            rec["year_counts"][year] += 1
        rec["upc"].add(upc)
        if edition:
            rec["editions"].add(edition)
        if bluray_url:
            rec["bluray_urls"].add(bluray_url)

        rec["sources"].append(
            {
                "source": "blu-ray-scrape",
                "confidence": 0.55,
                "observed_at": scraped_at or generated_at,
                "path": os.path.relpath(path, BASE_DIR),
                "bluray_url": bluray_url or None,
            }
        )

        upc_to_movie_ids[upc].add(movie_id)

    # UPC conflict validation.
    for upc, movie_ids in sorted(upc_to_movie_ids.items()):
        if len(movie_ids) <= 1:
            continue
        review_queue.append(
            {
                "type": "upc_conflict",
                "upc": upc,
                "movie_ids": sorted(movie_ids),
                "reason": "same_upc_maps_to_multiple_movies",
            }
        )
        for mid in movie_ids:
            records[mid]["status"] = "needs_review"

    normalized_records = []
    staging_candidates = []
    movie_by_id_index = {}
    title_year_index = defaultdict(list)
    upc_index = {}
    title_index = defaultdict(list)

    for movie_id, rec in sorted(records.items()):
        title = choose_primary(rec["title_counts"], "Unknown Title")
        year = choose_primary(rec["year_counts"], None)
        editions = sorted(rec["editions"])
        upcs = sorted(rec["upc"])
        bluray_urls = sorted(rec["bluray_urls"])
        aliases = sorted([k for k in rec["title_counts"].keys() if k != title])

        if not year:
            review_queue.append(
                {
                    "type": "missing_year",
                    "movie_id": movie_id,
                    "title": title,
                    "reason": "no_year_detected",
                }
            )

        if rec["tmdb_id"] and len(upcs) > 0:
            status = "verified"
        elif len(upcs) > 0 and not any(item.get("type") == "upc_conflict" and movie_id in item.get("movie_ids", []) for item in review_queue):
            status = "verified"
        else:
            status = "needs_review"

        normalized = {
            "movie_id": movie_id,
            "tmdb_id": rec["tmdb_id"],
            "status": status,
            "title": title,
            "title_normalized": normalize_title(title),
            "title_aliases": aliases,
            "year": year,
            "upc": upcs,
            "editions": editions,
            "bluray_urls": bluray_urls,
            "sources": rec["sources"],
            "last_seen_at": rec["last_seen_at"],
        }
        normalized_records.append(normalized)
        if normalized["status"] != "verified":
            staging_candidates.append(normalized)
        movie_by_id_index[movie_id] = normalized

        year_key = year if year else 0
        ty_key = f"{normalized['title_normalized']}|{year_key}"
        title_year_index[ty_key].append(movie_id)

        best_edition = editions[0] if editions else title
        best_url = bluray_urls[0] if bluray_urls else ""
        for upc in upcs:
            upc_index[upc] = {
                "movie_id": movie_id,
                "upc": upc,
                "title": title,
                "year": year,
                "edition": best_edition,
                "bluray_url": best_url,
                "status": status,
            }

        title_index[normalized["title_normalized"]].append(
            {
                "movie_id": movie_id,
                "title": title,
                "year": year,
                "edition": best_edition,
                "status": status,
            }
        )

    movie_payload = {
        "version": generated_at,
        "generated_at": generated_at,
        "count": len(normalized_records),
        "records": normalized_records,
    }
    upc_payload = {
        "version": generated_at,
        "generated_at": generated_at,
        "count": len(upc_index),
        "index": upc_index,
    }
    title_year_payload = {
        "version": generated_at,
        "generated_at": generated_at,
        "count": len(title_year_index),
        "index": dict(sorted(title_year_index.items())),
    }
    review_payload = {
        "version": generated_at,
        "generated_at": generated_at,
        "count": len(review_queue),
        "items": review_queue,
    }
    staging_payload = {
        "version": generated_at,
        "generated_at": generated_at,
        "count": len(staging_candidates),
        "records": staging_candidates,
    }

    with open(MOVIE_BY_ID_PATH, "w") as f:
        json.dump(movie_payload, f, indent=2)
    with open(UPC_INDEX_DB_PATH, "w") as f:
        json.dump(upc_payload, f, indent=2)
    with open(TITLE_YEAR_INDEX_PATH, "w") as f:
        json.dump(title_year_payload, f, indent=2)
    with open(REVIEW_QUEUE_PATH, "w") as f:
        json.dump(review_payload, f, indent=2)
    with open(STAGING_PATH, "w") as f:
        json.dump(staging_payload, f, indent=2)

    # Compatibility files for existing web app.
    with open(ROOT_UPC_INDEX_PATH, "w") as f:
        json.dump(upc_payload, f, indent=2)
    with open(ROOT_TITLE_INDEX_PATH, "w") as f:
        json.dump(
            {
                "version": generated_at,
                "generated_at": generated_at,
                "count": sum(len(v) for v in title_index.values()),
                "keys": len(title_index),
                "index": dict(sorted(title_index.items())),
            },
            f,
            indent=2,
        )

    manifest = {
        "version": generated_at,
        "generated_at": generated_at,
        "counts": {
            "raw_records_seen": ingestion_count,
            "invalid_raw_records": invalid_count,
            "canonical_movies": len(normalized_records),
            "upc_entries": len(upc_index),
            "review_queue_items": len(review_queue),
        },
        "files": {},
    }
    for path in [MOVIE_BY_ID_PATH, UPC_INDEX_DB_PATH, TITLE_YEAR_INDEX_PATH, REVIEW_QUEUE_PATH, STAGING_PATH, ROOT_UPC_INDEX_PATH, ROOT_TITLE_INDEX_PATH]:
        manifest["files"][os.path.relpath(path, BASE_DIR)] = {
            "sha256": file_sha256(path),
            "bytes": os.path.getsize(path),
        }

    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2)

    report = {
        "generated_at": generated_at,
        "summary": manifest["counts"],
        "review_queue_preview": review_queue[:25],
    }
    with open(LATEST_REPORT_PATH, "w") as f:
        json.dump(report, f, indent=2)

    print("Database build complete.")
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    build_database()
