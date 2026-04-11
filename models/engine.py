import importlib
import os
import re
import hashlib
import queue
from addict import Dict
from litestar.status_codes import HTTP_401_UNAUTHORIZED
from fastapi import HTTPException
from contextlib import closing
from models.odbc_engine import ODBC_Engine
from models.expression import Expression
from settings import drivers


def get_engine(cfg, db_name=None):
    driver = drivers[cfg.system][cfg.driver]
    driver.name = cfg.driver
    if cfg.driver == 'pyodbc':
        engine = ODBC_Engine(cfg, driver, db_name)
    else:
        engine = Engine(cfg, driver, db_name)

    if cfg.system == 'sqlite' and db_name == 'urdr':
        with engine.connect() as cnxn:
            sql = """
            select count(*) from user
            where id = :id and password = :pwd
            """

            hashed_pwd = hashlib.sha256(cfg.pwd.encode('utf-8')).hexdigest()
            expr = Expression(engine)
            sql, params = expr.prepare(sql, {'id': cfg.uid, 'pwd': hashed_pwd})
            crsr = cnxn.cursor()
            crsr.execute(sql, params)
            count = crsr.fetchone()[0]

            if count == 0:
                raise HTTPException(
                    status_code=HTTP_401_UNAUTHORIZED,
                    detail={
                        'msg': "Invalid authentication",
                        "system": cfg.system,
                        "host": cfg.host,
                        "database": db_name
                    }
                )
    elif cfg.system == 'sqlite' and cfg.database == 'urdr':
        with engine.connect() as cnxn:
            path = os.path.join(cfg.host, cfg.database + '.db')
            sql = 'ATTACH DATABASE "' + path + '" as urdr'
            crsr = cnxn.cursor()
            crsr.execute(sql)

    return engine


class DatabaseManager:
    def __init__(self):
        self.pools = {}

    def get_pool(self, engine):
        """Get pool based on engine"""
        key = engine.driver_name + ':' + engine.host + '/' + str(engine.db_name or '')
        if key not in self.pools:
            self.pools[key] = ConnectionPool(engine.connect, pool_size=5)
        return self.pools[key]


class ConnectionPool:
    def __init__(self, create_connection_fn, pool_size=5):
        self._pool = queue.Queue(maxsize=pool_size)

        # Fill pool with initial connections
        for _ in range(pool_size):
            conn = create_connection_fn()
            self._pool.put(conn)

    def get_connection(self, timeout=None):
        """Get en available connection from the queue"""
        try:
            return self._pool.get(block=True, timeout=timeout)
        except queue.Empty:
            raise Exception("Ingen ledige tilkoblinger i poolen.")

    def release_connection(self, conn):
        """Return connection to queue"""
        self._pool.put(conn)

    def close_all(self):
        """Closes all connections in the pool"""
        while not self._pool.empty():
            conn = self._pool.get()
            try:
                conn.close()
            except Exception:
                pass # Ignorer feil under stenging

    def connection(self):
        """Context manager for using 'with'"""
        return ConnectionContextManager(self)

class ConnectionContextManager:
    def __init__(self, pool):
        self.pool = pool
        self.conn = None

    def __enter__(self):
        self.conn = self.pool.get_connection()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.pool.release_connection(self.conn)


class Connection:

    def __init__(self, cnxn, driver):
        self._cnxn = cnxn
        self.driver = driver

    def cursor(self):
        options = self.driver.get('options', {})
        # Make shure all cursor objects run .close() when exiting `with` statements
        return closing(self._cnxn.cursor(**options))

    def commit(self):
        return self._cnxn.commit()

    def close(self):
        return self._cnxn.close()


class Engine:

    def __init__(self, cfg, driver, db_name=None):

        self.name = cfg.system
        self.db_name = db_name
        self.host = cfg.host
        self.driver = driver
        try:
            self.driver_module = importlib.import_module(driver.name, package=None)
        except ImportError:
            msg = 'Please install driver ' + driver.name
            raise HTTPException(
                status_code=404,
                detail=msg
            )
        self.driver_name = driver.name

        pattern = r'([\w\.-]+)(:\d+)?([/\\]\w+)?'
        match = re.search(pattern, cfg.host)

        config = Dict({
            'host': match.group(1),
            'port': None if match.group(2) is None else match.group(2)[1:],
            'sid': None if match.group(3) is None else match.group(3)[1:],
            'user': cfg.uid,
            'pass': cfg.pwd,
            'dbname': db_name.split('.')[0] if db_name else None,
            'path': os.path.join(cfg.host, db_name) if db_name else None
        })

        cnxn_string = driver.string.format(**config)
        self.query = driver.query.format(**config) if driver.query else None
        cnxn_key_value_pairs = cnxn_string.split(';')
        self.connect_params = {}
        self.cnxnstr = None
        for key_value in cnxn_key_value_pairs:
            parts = key_value.split('=')
            if len(parts) > 1:
                key = parts[0].strip()
                value = parts[1]
                if value and value != 'None':
                    if value in ('True', 'False'):
                        value = value == 'True'
                    self.connect_params[key] = value
            else:
                self.cnxnstr = key_value

        self.url = Dict({
            'username': cfg.uid,
            'database': (config.path if cfg.system in ('sqlite', 'duckdb')
                         else db_name.split('.')[0] if db_name else None)
        })

    def connect(self):
        try:
            if len(self.connect_params):
                print('connect_params', self.connect_params)
                cnxn = self.driver_module.connect(**self.connect_params)
            else:
                print('cnxnstr', self.cnxnstr)
                cnxn = self.driver_module.connect(self.cnxnstr)
        except Exception as ex:
            print(ex)
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail=str(ex)
            )
        if self.query:
            cnxn.execute(self.query)

        return Connection(cnxn, self.driver)

