from pydantic import BaseSettings


class Settings(BaseSettings):
    secret_key: str = "some_secret_key"
    timeout: int = 30 * 60  # 30 minutes
    system: str = None
    host: str = None
    database: str = None
    uid: str = None
    pwd: str = None
    mysql_driver: str = 'mysqlconnector'
    mariadb_driver: str = 'mysqlconnector'
    postgresql_driver: str = 'psycopg2'
    sqlite_driver: str = 'pysqlite'
    oracle_driver: str = 'cx_oracle'
    mssql_driver: str = 'pyodbc'
    norwegian_chars: bool = True 


    class Config:
        env_prefix = 'urdr_'


