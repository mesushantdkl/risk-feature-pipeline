import psycopg2
import random
import uuid
from datetime import datetime, timedelta

CONN = {
    "host": "localhost",
    "port": 5433,
    "dbname": "risk_source",
    "user": "admin",
    "password": "password"
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

def generate_transaction():
    account_id = f"ACC{random.randint(1000, 9999)}"
    amount = round(random.uniform(1, 50000), 2)
    tx_type = random.choice(["purchase", "withdrawal", "transfer", "deposit"])
    merchant = random.choice(["retail", "gambling", "crypto", "food", "travel", "unknown"])
    country = random.choice(["AU", "US", "UK", "NG", "RU", "CN", "DE"])
    is_flagged = (
        (amount > 10000 and country in ["NG", "RU"]) or
        (merchant == "gambling" and amount > 5000) or
        (tx_type == "withdrawal" and amount > 20000)
    )
    return (str(uuid.uuid4()), account_id, amount, tx_type, merchant, country, is_flagged)

def seed_database(n=5000):
    print("Connecting to database...")
    conn = psycopg2.connect(**CONN)
    cur = conn.cursor()
    cur.execute(CREATE_TABLE)
    conn.commit()
    print("Table ready. Inserting transactions...")

    insert_sql = """
    INSERT INTO raw_transactions
      (transaction_id, account_id, amount, transaction_type,
       merchant_category, country_code, is_flagged)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT DO NOTHING;
    """
    batch = [generate_transaction() for _ in range(n)]
    cur.executemany(insert_sql, batch)
    conn.commit()
    print(f"Successfully inserted {n} mock transactions.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    seed_database(5000)
