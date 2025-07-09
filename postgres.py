import psycopg2

def should_exclude(table_name: str) -> bool:
    EXCLUDE_SUBSTRINGS = ['temp', 'audit', 'log', "test"]
    return any(substr.lower() in table_name.lower() for substr in EXCLUDE_SUBSTRINGS)

def yield_table(cur):
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
    """)

    tables = cur.fetchall()
    for (table_name,) in tables:
        if not should_exclude(table_name):
            yield table_name

def get_schema(cur, table_name):
    cur.execute(f"""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = %s AND table_schema = 'public'
        ORDER BY ordinal_position;
    """, (table_name,))
    return cur.fetchall()

def get_pks(cur, table_name):
    cur.execute("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY';
    """, (table_name,))
    return cur.fetchall()

def get_fks(cur, table_name):
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
    return cur.fetchall()

def get_unique(cur, table_name):
    cur.execute("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_name = %s AND tc.constraint_type = 'UNIQUE';
    """, (table_name,))
    return cur.fetchall()

def get_checks(cur, table_name):
    cur.execute("""
        SELECT
            conname,
            pg_get_constraintdef(c.oid)
        FROM pg_constraint c
        JOIN pg_class t ON c.conrelid = t.oid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE contype = 'c'
            AND t.relname = %s
            AND n.nspname = 'public';
    """, (table_name,))
    return cur.fetchall()

def yield_view(cur):
    cur.execute("""
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema = 'public';
    """)
    views = [row[0] for row in cur.fetchall()]
    for view_name in views:
        if not should_exclude(view_name):
            yield view_name
            
def get_definition(cur, view_name):
    cur.execute("""
        SELECT pg_get_viewdef(oid, true)
        FROM pg_class
        WHERE relname = %s AND relkind = 'v';
    """, (view_name,))
    return cur.fetchone()[0]

def get_db_metadata(config: dict):
    conn = psycopg2.connect(**config)
    cur = conn.cursor()
    
    table_data = []
    table_generator = yield_table(cur)
    for table_name in table_generator:
        schema = get_schema(cur, table_name)
        pks = get_pks(cur, table_name)
        fks = get_fks(cur, table_name)
        uniques = get_unique(cur, table_name)        
        checks = get_checks(cur, table_name)
        data = {
            "schema": schema,
            "pks": pks,
            "fks": fks,
            "uniques": uniques,
            "checks": checks
        }
        table_data.append({table_name: data})

    view_data = []
    view_generator = yield_view(cur)
    for view_name in view_generator:
        definition = get_definition(cur, view_name)
        data = {
            "definition": definition
        }
        view_data.append({view_name: data})

    cur.close()
    conn.close()

    return table_data, view_data