import psycopg2

def get_database_list(config):
    with psycopg2.connect(dbname='postgres', **config) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT datname
                FROM pg_database
                WHERE datistemplate = false
                  AND datallowconn = true;
            """)
            dbs = cur.fetchall()
            for (db_name,) in dbs:
                yield db_name

def yield_table(cur):
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
    """)

    tables = cur.fetchall()
    for (table_name,) in tables:
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
            tc.constraint_name,
            kcu.table_name AS source_table,
            array_agg(kcu.column_name ORDER BY kcu.ordinal_position)::TEXT[] AS source_columns,
            ccu.table_name AS target_table,
            array_agg(ccu.column_name ORDER BY kcu.ordinal_position)::TEXT[] AS target_columns,
            tc.constraint_type
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.referential_constraints rc
            ON tc.constraint_name = rc.constraint_name
        JOIN information_schema.constraint_column_usage ccu
            ON rc.unique_constraint_name = ccu.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = 'public'
        AND kcu.table_name = %s
        GROUP BY tc.constraint_name, kcu.table_name, ccu.table_name, tc.constraint_type;
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
        yield view_name
            
def get_definition(cur, view_name):
    cur.execute("""
        SELECT pg_get_viewdef(oid, true)
        FROM pg_class
        WHERE relname = %s AND relkind = 'v';
    """, (view_name,))
    return cur.fetchone()[0]

def get_db_metadata(config: dict, db_name: str):
    config["dbname"] = db_name
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

def get_metadata(config: dict):
    db_generator = get_database_list(config)
    db_data = []
    for db_name in db_generator:
        tables, views = get_db_metadata(config, db_name)
        data = {
            "tables": tables,
            "views": views
        }
        db_data.append({db_name: data})
    
    return db_data