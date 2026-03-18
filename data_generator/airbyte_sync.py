import psycopg2

SOURCE = {
    "host": "localhost", "port": 5433,
    "dbname": "risk_source", "user": "admin", "password": "password"
}

DEST = {
    "host": "localhost", "port": 5434,
    "dbname": "risk_dest", "user": "admin", "password": "password"
}

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS raw_transactions (
    transaction_id    VARCHAR(36) PRIMARY KEY,
    account_id        VARCHAR(20) NOT NULL,
    amount            NUMERIC(12,2) NOT NULL,
    transaction_type  VARCHAR(20),
    merchant_category VARCHAR(50),
    country_code      CHAR(2),
    is_flagged        BOOLEAN DEFAULT FALSE,
    created_at        TIMESTAMP DEFAULT NOW()
);
"""

def sync():
    print("Connecting to source database...")
    src = psycopg2.connect(**SOURCE)
    src_cur = src.cursor()

    print("Connecting to destination database...")
    dst = psycopg2.connect(**DEST)
    dst_cur = dst.cursor()

    # Create table in destination if it doesn't exist
    dst_cur.execute(CREATE_TABLE)
    dst.commit()

    # Extract from source
    print("Extracting records from source...")
    src_cur.execute("SELECT * FROM raw_transactions;")
    rows = src_cur.fetchall()
    print(f"Found {len(rows)} records to sync.")

    # Load into destination
    print("Loading records into destination...")
    insert_sql = """
    INSERT INTO raw_transactions
      (transaction_id, account_id, amount, transaction_type,
       merchant_category, country_code, is_flagged, created_at)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (transaction_id) DO NOTHING;
    """
    dst_cur.executemany(insert_sql, rows)
    dst.commit()

    print(f"Sync complete. {dst_cur.rowcount} new records loaded.")

    src_cur.close()
    src.close()
    dst_cur.close()
    dst.close()

if __name__ == "__main__":
    sync()
