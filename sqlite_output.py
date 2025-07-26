from datetime import datetime
import sqlite3
import os

DATETIME_STAMP = datetime.now().strftime("%d%b%Y_%H:%M:%S")
default_path = f"./sqlite_output/postgres_to_sqlite_{DATETIME_STAMP}.sqlite"

def export_to_sqlite(db_data, db_path=default_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS database_entities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            database_name TEXT NOT NULL,
            entity_type TEXT NOT NULL,  -- 'table' or 'view'
            entity_name TEXT NOT NULL,
            ddl TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS table_schema (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            database_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            column_name TEXT,
            data_type TEXT,
            nullable TEXT,
            default_value TEXT
        )
    """)

    for db_entry in db_data:
        for db_name, db_contents in db_entry.items():

            # Insert tables with DDL
            for table_obj in db_contents.get("tables", []):
                for table_name, table_def in table_obj.items():
                    ddl = table_def.get("ddl", "")
                    cursor.execute(
                        "INSERT INTO database_entities (database_name, entity_type, entity_name, ddl) VALUES (?, ?, ?, ?)",
                        (db_name, "table", table_name, ddl)
                    )
                    for col in table_def.get("schema", []):
                        column_name, data_type, nullable, default = col
                        cursor.execute(
                            "INSERT INTO table_schema (database_name, table_name, column_name, data_type, nullable, default_value) VALUES (?, ?, ?, ?, ?, ?)",
                            (db_name, table_name, column_name, data_type, nullable, default)
                        )

            # Insert views with DDL as 'definition'
            for view_obj in db_contents.get("views", []):
                for view_name, view_def in view_obj.items():
                    ddl = f"CREATE OR REPLACE VIEW {view_name} AS\n{view_def.get('definition', '')}"
                    cursor.execute(
                        "INSERT INTO database_entities (database_name, entity_type, entity_name, ddl) VALUES (?, ?, ?, ?)",
                        (db_name, "view", view_name, ddl)
                    )

    conn.commit()
    conn.close()
    print(f"SQLite Exported to: {db_path}")
