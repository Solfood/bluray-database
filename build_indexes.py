import json
import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPC_ROOT = os.path.join(BASE_DIR, "upc")
UPC_INDEX_PATH = os.path.join(BASE_DIR, "upc_index.json")
TITLE_INDEX_PATH = os.path.join(BASE_DIR, "title_index.json")


def normalize_title(value):
    cleaned = (value or "").lower()
    out = []
    for ch in cleaned:
        if ch.isalnum():
            out.append(ch)
        else:
            out.append(" ")
    return " ".join("".join(out).split())


def iter_upc_files():
    if not os.path.isdir(UPC_ROOT):
        return
    for root, _, files in os.walk(UPC_ROOT):
        for name in files:
            if name.endswith(".json"):
                yield os.path.join(root, name)


def build_indexes():
    upc_index = {}
    title_index = {}
    count = 0

    for path in iter_upc_files() or []:
        try:
            with open(path, "r") as f:
                record = json.load(f)
        except Exception:
            continue

        upc = str(record.get("upc", "")).strip()
        title = str(record.get("title", "")).strip()
        year = str(record.get("year", "")).strip()[:4] if record.get("year") else ""
        edition = str(record.get("edition", "")).strip()
        bluray_url = str(record.get("bluray_url", "")).strip()

        if not upc or not title:
            continue

        minimal = {
            "upc": upc,
            "title": title,
            "year": year,
            "edition": edition,
            "bluray_url": bluray_url,
        }

        upc_index[upc] = minimal

        key = normalize_title(title)
        if key:
            title_index.setdefault(key, []).append(minimal)

        count += 1

    generated_at = datetime.utcnow().isoformat()
    upc_payload = {
        "generated_at": generated_at,
        "count": len(upc_index),
        "index": upc_index,
    }
    title_payload = {
        "generated_at": generated_at,
        "count": sum(len(v) for v in title_index.values()),
        "keys": len(title_index),
        "index": title_index,
    }

    with open(UPC_INDEX_PATH, "w") as f:
        json.dump(upc_payload, f, indent=2)
    with open(TITLE_INDEX_PATH, "w") as f:
        json.dump(title_payload, f, indent=2)

    print(f"Indexed {count} records.")
    print(f"Wrote {UPC_INDEX_PATH}")
    print(f"Wrote {TITLE_INDEX_PATH}")


if __name__ == "__main__":
    build_indexes()
