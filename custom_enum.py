from enum import Enum

class DBType(Enum):
    Postgres = 1
    SqlSvr = 2

class OutputType(Enum):
    Text = 1
    Sqlite = 2