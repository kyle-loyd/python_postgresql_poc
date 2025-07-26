import os
import sys
from cli_output import print_results
from custom_enum import DBType, OutputType
from db_connection import DBConnection, DBConnectionConfig
from dotenv import load_dotenv
from sqlite_output import export_to_sqlite

db_connection_config = None
SOURCE_ARG_TEXT = "Source: --from_postgres || --from_sqlsvr"
DESTINATION_ARG_TEXT = "Destination: --to_txt || --to_sqlite"

source_args = {
    "--from_postgres": DBType.Postgres,
    "--from_sqlsvr": DBType.SqlSvr
}
destination_args = {
    "--to_txt": {
        "type": OutputType.Text, 
        "method": print_results
        },
    "--to_sqlite": {
        "type": OutputType.Sqlite, 
        "method": export_to_sqlite
    }
}

load_dotenv()

def print_execution_error():
    print("ERROR: Required arguments not provided.")
    print("Ex. \"python3 main.py <source> <destination>\"")
    print(SOURCE_ARG_TEXT)
    print(DESTINATION_ARG_TEXT)
    exit(1)

def process_arg(map: dict, arg: str):
    if arg not in map:
        print_execution_error()
    return map[arg]

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print_execution_error()

    db_type = process_arg(map=source_args, arg=sys.argv[1])
    print(f"Source Registered: {db_type}")
    
    output_config = process_arg(map=destination_args, arg=sys.argv[2])
    print(f"Output Registered: {output_config['type']}")

    db_config = DBConnectionConfig(os.environ, db_type)
    db = DBConnection(db_config)
    db_data = db.get_metadata()

    process_output = lambda data: output_config["method"](data)
    process_output(db_data)