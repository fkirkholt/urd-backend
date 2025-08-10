from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    secret_key: str = "some_secret_key"
    timeout: int = 30 * 60  # 30 minutes
    cnxn: str | None = None
    system: str | None = None
    host: str | None = None
    subfolders: list = []
    database: str | None = None
    uid: str | None = None
    pwd: str | None = None
    use_odbc: bool = False
    mysql_driver: str = 'mysqlconnector'
    mariadb_driver: str = 'mysqlconnector'
    postgresql_driver: str = 'psycopg2'
    sqlite_driver: str = 'pysqlite'
    oracle_driver: str = 'cx_oracle'
    mssql_driver: str = 'pyodbc'
    norwegian_chars: bool = False
    exportdir: str | None = None
    websocket: str | None = None
    # Filetypes that should be checked with LSP over websocket
    lsp_filetypes: str | None = None  # bar delimited: .py|.js

    class Config:
        env_prefix = 'urdr_'
