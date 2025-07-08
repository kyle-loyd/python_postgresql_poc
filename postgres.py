import psycopg2

# Substring matches for table names to exclude (case-insensitive)
# === CONNECT & EXTRACT SCHEMA ===
def should_exclude(table_name: str) -> bool:
    EXCLUDE_SUBSTRINGS = ['temp', 'audit', 'log', "test"]
    return any(substr.lower() in table_name.lower() for substr in EXCLUDE_SUBSTRINGS)

def extract_schema(config: dict):
    conn = psycopg2.connect(**config)
    cur = conn.cursor()

    # Fetch all user tables from the 'public' schema
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
    """)

    tables = cur.fetchall()

    for (table_name,) in tables:
        if should_exclude(table_name):
            continue

        print(f"\n-- Schema for table: {table_name}")
        cur.execute(f"""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = 'public'
            ORDER BY ordinal_position;
        """, (table_name,))

        for column in cur.fetchall():
            col_name, col_type, is_nullable, default = column
            print(f"{col_name} {col_type} {'NOT NULL' if is_nullable == 'NO' else 'NULL'}"
                  f"{' DEFAULT ' + str(default) if default else ''}")

    cur.close()
    conn.close()
