import sys
import mysql.connector
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

BATCH_SIZE = 5000  # Adjust this to optimize performance based on your DB performance

def connect_to_db():
    return mysql.connector.connect(
        host="",
        user="",
        password="",
        database=""
    )

def bulk_insert(cursor, table_name, batch):
    cursor.executemany(f"INSERT IGNORE INTO `{table_name}` (url) VALUES (%s)", batch)

def process_urls(table_name, urls, batch_size=BATCH_SIZE):
    conn = connect_to_db()
    cursor = conn.cursor()
    total_urls = len(urls)

    batches = [urls[i:i + batch_size] for i in range(0, total_urls, batch_size)]

    with tqdm(total=total_urls, desc=f"Inserting into {table_name}", unit="url") as pbar:
        for batch in batches:
            bulk_insert(cursor, table_name, batch)
            conn.commit()  # Commit after each batch insert
            pbar.update(len(batch))

    cursor.close()
    conn.close()

def bulk_file(table_name, urls):
    # Delete all entries first
    delete_all(table_name)
    process_urls(table_name, urls)

def append_file(table_name, urls):
    process_urls(table_name, urls)

def get_urls(table_name):
    conn = connect_to_db()
    cursor = conn.cursor()

    cursor.execute(f"SELECT url FROM `{table_name}`")
    results = cursor.fetchall()

    cursor.close()
    conn.close()

    for url in results:
        print(url[0])

def delete_all(table_name):
    conn = connect_to_db()
    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE `{table_name}`")
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Deleted all entries in table {table_name}.")

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Update, retrieve, or delete URLs from the database.')
    parser.add_argument('--table', required=True, help='Table name to operate on (e.g., all_urls)')
    parser.add_argument('--bulk', action='store_true', help='Bulk file into the specified table')
    parser.add_argument('--append', action='store_true', help='Append file into the specified table without deleting existing content')
    parser.add_argument('--get', action='store_true', help='Retrieve URLs from the specified table')
    parser.add_argument('--delete', action='store_true', help='Delete all entries in the specified table')

    args = parser.parse_args()

    urls = []

    # If --get is used, retrieve and print URLs from the specified table
    if args.get:
        get_urls(args.table)
        return

    try:
        # Reading from the pipeline or standard input
        for line in sys.stdin:
            url = line.strip()
            if url:
                urls.append((url,))  # Tuple for executemany
    except EOFError:
        pass

    if urls:
        if args.bulk:
            bulk_file(args.table, urls)
        elif args.append:
            append_file(args.table, urls)

    if args.delete:
        delete_all(args.table)

if __name__ == '__main__':
    main()
