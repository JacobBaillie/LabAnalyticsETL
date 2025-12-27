# ============================
# User inputs (edit these)
# ============================
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta, date
import numpy as np
import psycopg2

# — Shared drive root (UNC path)
ROOT_DIR = r"Z:\Former_Group_People"  # <-- CHANGE ME

# — Timestamp correction: add +1122 days and +1309 minutes
#TIME_OFFSET = timedelta(days=1122, minutes=1309) #used for fixing dates after 2025 due to error
TIME_OFFSET = timedelta(days=0, minutes=0)

# — Only keep files whose *corrected* creation timestamp falls in 2025
YEAR_START = date(2020, 1, 1)
YEAR_END_EXCLUSIVE = date(2026, 1, 1)

# — Which timestamp to use: "ctime" (creation on Windows) or "mtime" (modified)
TIMESTAMP_FIELD = "ctime"  # try "mtime" if your share has weird ctime behavior

# — Skip dotfiles/folders
SKIP_DOTFILES = True

# — Progress print every N files
PROGRESS_EVERY = 200

# — PostgreSQL connection (match your existing ETL DB so you can JOIN/UNION)
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "lab_analytics"
DB_USER = "postgres"
DB_PASSWORD = "***" # <-- CHANGE ME

# — Target table name
TABLE_NAME = "user_files_alumni"


# ============================
# Helpers
# ============================
def _is_dot(name: str) -> bool:
    return name.startswith(".")

def _get_file_time_utc(path_str: str, field: str) -> datetime:
    st = os.stat(path_str)
    ts = st.st_ctime if field == "ctime" else st.st_mtime
    return datetime.fromtimestamp(ts, tz=timezone.utc)

def _iter_mother_folders(root_dir: str):
    root = Path(root_dir)
    for child in root.iterdir():
        if not child.is_dir():
            continue

        name = child.name

        # Skip dot folders
        if SKIP_DOTFILES and _is_dot(name):
            continue

        # Skip any folder containing space or underscore or the Website folder
        if (" " in name) or ("_" in name) or (name == "Website") or not(name in ["Carmelita", "Kimo", "Kelly", "Diana", "Stephen", "Tyler", "Laura"]):
            continue

        yield child


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )


# ============================
# DB schema + upsert
# ============================
DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
  mother_folder TEXT NOT NULL,
  day DATE NOT NULL,
  file_count BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (mother_folder, day)
);
"""

UPSERT = f"""
INSERT INTO {TABLE_NAME} (mother_folder, day, file_count, updated_at)
VALUES (%s, %s, %s, now())
ON CONFLICT (mother_folder, day) DO UPDATE SET
  file_count = EXCLUDED.file_count,
  updated_at = now();
"""


# ============================
# Scan + aggregate
# ============================
def _collapse_name(name: str) -> str:
    # ignore last 9 characters (before extension)
    base, ext = os.path.splitext(name)
    if len(base) <= 9:
        return base + ext
    return base[:-9] + ext

def scan_counts_2025() -> dict:
    # (mother_folder, yyyy-mm-dd) -> count
    counts = {}
    total_files = 0
    errors = 0

    for mother in _iter_mother_folders(ROOT_DIR):
        mother_name = mother.name

        for dirpath, dirnames, filenames in os.walk(mother):
            # Skip dot directories
            if SKIP_DOTFILES:
                dirnames[:] = [d for d in dirnames if not _is_dot(d)]
                filenames = [f for f in filenames if not _is_dot(f)]

            # ----------------------------
            # Batch short-circuit rule
            # ----------------------------
            name_groups = {}
            for fn in filenames:
                collapsed = _collapse_name(fn)
                name_groups.setdefault(collapsed, 0)
                name_groups[collapsed] += 1

            batch_key = None

            for collapsed, n in name_groups.items():
                if n >= 40:
                    # pick representative file
                    rep_file = None
                    for fn in filenames:
                        if _collapse_name(fn) == collapsed:
                            rep_file = fn
                            break

                    if rep_file is not None:
                        try:
                            fp = os.path.join(dirpath, rep_file)
                            raw_dt = _get_file_time_utc(fp, TIMESTAMP_FIELD)
                            corrected_dt = raw_dt + TIME_OFFSET
                            d = corrected_dt.date()

                            if YEAR_START <= d < YEAR_END_EXCLUSIVE:
                                batch_key = (mother_name, d.isoformat())
                        except Exception:
                            batch_key = None
                    break

            # If batch rule triggered, +5 and skip this directory’s remaining files
            if batch_key is not None:
                counts[batch_key] = counts.get(batch_key, 0) + 5
                continue

            # ----------------------------
            # Normal per-file counting
            # ----------------------------
            for fn in filenames:
                total_files += 1

                if total_files % PROGRESS_EVERY == 0:
                    print(f"Scanned {total_files:,} files... (errors: {errors})")
                    print(f"Current folder: {mother_name} / {dirpath}")

                fp = os.path.join(dirpath, fn)
                try:
                    raw_dt = _get_file_time_utc(fp, TIMESTAMP_FIELD)
                    corrected_dt = raw_dt + TIME_OFFSET
                    d = corrected_dt.date()

                    if not (YEAR_START <= d < YEAR_END_EXCLUSIVE):
                        continue

                    key = (mother_name, d.isoformat())
                    counts[key] = counts.get(key, 0) + 1

                except Exception:
                    errors += 1
                    continue

    print(f"Done scanning. Files scanned: {total_files:,}. Errors: {errors}. Groups: {len(counts):,}")
    return counts



# ============================
# Load to PostgreSQL
# ============================
def upsert_counts(conn, counts: dict):
    # Convert dict to numpy arrays for sorting + efficient iteration
    keys = list(counts.keys())
    mother = np.array([k[0] for k in keys], dtype=object)
    day = np.array([k[1] for k in keys], dtype=object)
    file_count = np.array([counts[k] for k in keys], dtype=np.int64)

    # Sort by mother, then day
    order = np.lexsort((day, mother))
    mother_s = mother[order]
    day_s = day[order]
    file_count_s = file_count[order]

    with conn.cursor() as cur:
        cur.execute(DDL)

        # Batch upsert
        for m, d, c in zip(mother_s, day_s, file_count_s):
            cur.execute(UPSERT, (m, d, int(c)))

    conn.commit()
    print(f"Upserted {len(file_count_s):,} rows into {TABLE_NAME}.")


if __name__ == "__main__":
    counts = scan_counts_2025()
    conn = get_conn()
    try:
        upsert_counts(conn, counts)
    finally:
        conn.close()
