header_template = lambda header: f"""
==============================
     {header}
==============================
"""
SPACER = "\n--------------------------------\n"

def print_schema(schema):
    for column in schema:
        col_name, col_type, is_nullable, default = column
        print(f"{col_name} {col_type} {'NOT NULL' if is_nullable == 'NO' else 'NULL'}"
                f"{' DEFAULT ' + str(default) if default else ''}")
    
def print_pks(pks):
    pk_cols = [row[0] for row in pks]
    if pk_cols:
        print(f"\nPRIMARY KEY: {', '.join(pk_cols)}")

def print_fks(fks):
    if fks:
        print("\nFOREIGN KEYS:")
        for row in fks:
            print(f"  - {row[0]}:")
            print(f"      {row[1]}({', '.join(row[2])}) → {row[3]}({', '.join(row[4])})")

def print_uniques(uniques):
    uniques_cols = [row[0] for row in uniques]
    if uniques_cols:
        print(f"\nUNIQUE: {', '.join(uniques_cols)}")

def print_checks(checks):
    if checks:
        print("\nCHECK:")
        for name, definition in checks:
            print(f"  - {name}: {definition}")

def print_view_definition(view_name, definition):
    print(f"\nVIEW: {view_name}")
    print(f"CREATE OR REPLACE VIEW {view_name} AS\n{definition};")

def print_tables(tables):
    for table_info in tables:
        for table_name, data in table_info.items():
            print(f"*** TABLE: {table_name} ***")
            print_schema(data["schema"])
            print_pks(data["pks"])
            print_fks(data["fks"])
            print_uniques(data["uniques"])
            print_checks(data["checks"])
            print(SPACER)
    
def print_views(views):
    for view_info in views:
        for view_name, data in view_info.items():
            print(f"*** VIEW: {view_name} ***")
            print_view_definition(view_name, data["definition"])
            print(SPACER)

def print_results(dbs):
    for db_info in dbs:
        for db_name, data in db_info.items():
            print(header_template(f"Database: {db_name}"))
            print_tables(data["tables"])
            print_views(data["views"]) 