# Database Pipeline

This repository now treats `upc/` as raw ingestion input and publishes canonical artifacts under `db/`.

## Source and Promotion Flow

1. Raw scrape output lands in `upc/`.
2. `build_database.py` canonicalizes records into `db/movie_by_id.json`.
3. Validation runs during canonicalization:
   - duplicate UPC collisions
   - invalid raw records
   - missing year checks
4. Non-verified records are written to `db/staging_candidates.json`.
5. Review items are written to `db/review_queue.json`.
6. Indexes and manifests are written:
   - `db/upc_index.json`
   - `db/title_year_index.json`
   - `db/manifest.json`
   - `reports/latest.json`

Compatibility indexes for the current web app are still emitted at repository root:
- `upc_index.json`
- `title_index.json`

## Canonical Record Fields

Each record in `db/movie_by_id.json` has:

- `movie_id` (stable internal id)
- `tmdb_id` (nullable)
- `status` (`verified` or `needs_review`)
- `title`
- `title_normalized`
- `title_aliases`
- `year`
- `upc` (array)
- `editions` (array)
- `bluray_urls` (array)
- `sources` (array with source/confidence/path metadata)
- `last_seen_at`

## Build

Run:

```bash
python build_database.py
```

This command is also run by the monthly workflow.
