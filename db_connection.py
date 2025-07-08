from custom_enum import DBType
from postgres import extract_schema as get_pg_schema

class DBConnectionConfig():
    def __init__(self, environment, db_type):
        self.db_type = db_type
        self.host = environment.get("DB_HOST")
        self.port = environment.get("DB_PORT")
        self.db_name = environment.get("DB_NAME")
        self.username = environment.get("DB_USER_NAME")
        self.password = environment.get("DB_USER_PASS")

    def get_postgres_config_dict(self) -> dict:
        return {
            'host': self.host,
            'port': self.port,
            'database': self.db_name,
            'user': self.username,
            'password': self.password
        }   

class DBConnection():
    def __init__(self, config: DBConnectionConfig):
        self.config = config

    def get_schema(self):
        if self.config.db_type == DBType.Postgres:
            config_dict = self.config.get_postgres_config_dict()
            schema = get_pg_schema(config_dict)