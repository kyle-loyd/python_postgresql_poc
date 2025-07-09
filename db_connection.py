from custom_enum import DBType
from postgres import get_db_metadata as get_pg_metadata

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

    def get_metadata(self):
        if self.config.db_type == DBType.Postgres:
            config_dict = self.config.get_postgres_config_dict()
            table_data, view_data = get_pg_metadata(config_dict)
        if self.config.db_type == DBType.SqlSvr:
            table_data = None
            view_data = None 
            #TODO: Implement SqlSvr
        return table_data, view_data