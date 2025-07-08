import os
import sys
from custom_enum import DBType
from db_connection import DBConnection, DBConnectionConfig
from dotenv import load_dotenv

db_connection_config = None
ARG_OPTION_KEY = "Postgres = 1; SQL Server = 2"

load_dotenv()

def get_db_type(arg: str) -> DBType:
    arg_to_type_map = {
        "1": DBType.Postgres,
        "2": DBType.SqlSvr
    }
    if arg not in arg_to_type_map:
        print(f"ERROR: Invalid type id. {ARG_OPTION_KEY}")
        exit(1)
    return arg_to_type_map[arg]


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"ERROR: No database type selected. Ex. \"python3 main.py <type_number>\" {ARG_OPTION_KEY}")
        exit(1)
    db_type = get_db_type(sys.argv[1])
    db_connection_config = DBConnectionConfig(os.environ, db_type)
    db_connection = DBConnection(db_connection_config)
    db_connection.get_schema()
