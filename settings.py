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
    mysql_driver: str = 'mysql.connector'
    mariadb_driver: str = 'mysql.connector'
    postgresql_driver: str = 'psycopg2'
    sqlite_driver: str = 'sqlite3'
    duckdb_driver: str = 'duckdb'
    oracle_driver: str = 'oracledb'
    mssql_driver: str = 'pymssql'
    norwegian_chars: bool = False
    exportdir: str | None = None
    websocket: str | None = None
    # Filetypes that should be checked with LSP over websocket
    lsp_filetypes: str = ''  # bar delimited: .py|.js

    class Config:
        env_prefix = 'urdr_'
