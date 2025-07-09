import psycopg2

# Substring matches for table names to exclude (case-insensitive)
# === CONNECT & EXTRACT SCHEMA ===
def should_exclude(table_name: str) -> bool:
    EXCLUDE_SUBSTRINGS = ['temp', 'audit', 'log', "test"]
    return any(substr.lower() in table_name.lower() for substr in EXCLUDE_SUBSTRINGS)

def extract_schema(config: dict):
    conn = psycopg2.connect(**config)

    cur = conn.cursor()
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
    """)

    tables = cur.fetchall()

    for (table_name,) in tables:
        if should_exclude(table_name):
            continue

        print(f"\nTABLE SCHEMA: {table_name}")
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
            
        # === Get primary key columns ===
        cur.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY';
        """, (table_name,))
        pk_cols = [row[0] for row in cur.fetchall()]
        if pk_cols:
            print(f"\nPRIMARY KEY: {', '.join(pk_cols)}")

        # === Get foreign keys ===
        cur.execute("""
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table,
                ccu.column_name AS foreign_column
            FROM
                information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = %s;
        """, (table_name,))
        fks = cur.fetchall()
        if fks:
            print("\nFOREIGN KEYS:")
            for col, ref_table, ref_col in fks:
                print(f"  - {col} → {ref_table}.{ref_col}")

        # === Get unique constraints ===
        cur.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = %s AND tc.constraint_type = 'UNIQUE';
        """, (table_name,))
        uniques = [row[0] for row in cur.fetchall()]
        if uniques:
            print(f"\nUNIQUE: {', '.join(uniques)}")

        print("\n--------------------------------\n")

    cur.close()
    conn.close()
