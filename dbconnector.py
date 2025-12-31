#!/usr/bin/env python3

import sys
import signal
import argparse
import mysql.connector
from mysql.connector import Error as MySQLError
from tqdm import tqdm
import unicodedata

# ==============================
# PIPELINE SAFETY
# ==============================
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

# ==============================
# CONFIG
# ==============================
BATCH_SIZE = 5000
FETCH_CHUNK = 10000

DB_HOST = ""
DB_USER = ""
DB_PASSWORD = ""
DB_NAME = ""
DB_PORT = 3306


def eprint(*args, **kwargs):
    """Always log to stderr (safe for pipelines)."""
    print(*args, file=sys.stderr, **kwargs)


# ==============================
# DATABASE
# ==============================
def connect_to_db():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT,
        autocommit=False,
    )


def truncate_table(table_name):
    conn = connect_to_db()
    cursor = conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE `{table_name}`")
        conn.commit()
        eprint(f"[OK] Truncated table {table_name}")
    finally:
        cursor.close()
        conn.close()


# ==============================
# STREAMING INSERT (APPEND / BULK)
# ==============================
def stream_insert(table_name, batch_size=BATCH_SIZE, show_progress=True):
    conn = connect_to_db()
    cursor = conn.cursor()

    insert_sql = f"INSERT IGNORE INTO `{table_name}` (url) VALUES (%s)"

    batch = []
    read_count = 0
    inserted_est = 0

    pbar = tqdm(
        desc=f"Inserting into {table_name}",
        unit="url",
        disable=not show_progress or not sys.stderr.isatty(),
    )

    try:
        # Read stdin as bytes and decode safely
        for raw_line in sys.stdin.buffer:
            url = raw_line.decode('utf-8', errors='ignore').strip()
            if not url:
                continue
            # Optional: normalize Unicode
            url = unicodedata.normalize('NFC', url)

            read_count += 1
            batch.append((url,))

            if len(batch) >= batch_size:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                inserted_est += max(cursor.rowcount, 0)
                pbar.update(len(batch))
                batch.clear()

        if batch:
            cursor.executemany(insert_sql, batch)
            conn.commit()
            inserted_est += max(cursor.rowcount, 0)
            pbar.update(len(batch))

    except BrokenPipeError:
        sys.exit(0)

    finally:
        try:
            pbar.close()
        except Exception:
            pass
        cursor.close()
        conn.close()

    return read_count, inserted_est


# ==============================
# FETCH URLs (DB → STDOUT)
# ==============================
def get_urls(table_name, chunk_size=FETCH_CHUNK):
    conn = connect_to_db()
    cursor = conn.cursor()

    try:
        cursor.execute(f"SELECT url FROM `{table_name}`")
        fetched = 0
        eprint(f"[INFO] Fetching URLs from {table_name}")

        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break

            for (url,) in rows:
                print(url)

            fetched += len(rows)
            eprint(f"[INFO] Fetched {fetched} rows")

        eprint(f"[OK] Finished fetching {fetched} rows")

    except BrokenPipeError:
        sys.exit(0)

    finally:
        cursor.close()
        conn.close()


# ==============================
# MAIN
# ==============================
def main():
    parser = argparse.ArgumentParser(
        description="Stream URLs into MySQL with batching and pipeline safety"
    )

    parser.add_argument("--table", required=True, help="Table name (e.g. all_urls)")
    parser.add_argument("--append", action="store_true", help="Append URLs from stdin")
    parser.add_argument("--bulk", action="store_true", help="Truncate then insert from stdin")
    parser.add_argument("--get", action="store_true", help="Print URLs from table")
    parser.add_argument("--delete", action="store_true", help="Truncate table only")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--no-progress", action="store_true")

    args = parser.parse_args()

    if not (args.append or args.bulk or args.get or args.delete):
        args.append = True

    try:
        if args.get:
            get_urls(args.table)
            return

        if args.delete:
            truncate_table(args.table)
            return

        if args.bulk:
            truncate_table(args.table)

        read_count, inserted_est = stream_insert(
            args.table,
            batch_size=args.batch_size,
            show_progress=not args.no_progress,
        )

        eprint(
            f"[SUMMARY] table={args.table} "
            f"read={read_count} inserted_estimate={inserted_est} "
            f"batch_size={args.batch_size}"
        )

    except MySQLError as e:
        eprint(f"[DB_ERROR] {e}")
        sys.exit(3)

    except KeyboardInterrupt:
        eprint("[WARN] Interrupted by user")
        sys.exit(130)

    except BrokenPipeError:
        sys.exit(0)

    except Exception as e:
        eprint(f"[UNEXPECTED] {type(e).__name__}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
