import importlib
import os
import re
import hashlib
import queue
import pyodbc
from addict import Dict
from litestar.status_codes import HTTP_401_UNAUTHORIZED
from litestar.exceptions import HTTPException
from contextlib import closing
from models.expression import Expression
from models.reflection import Reflection
from settings import drivers
import util


def get_engine(state, db_name=None):
    cfg = state.cfg
    driver = drivers[cfg.system][cfg.driver]
    driver.name = cfg.driver
    if cfg.driver == 'pyodbc':
        engine = ODBC_Engine(state, driver, db_name)
    else:
        engine = Engine(state, driver, db_name)

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
        self._pool = queue.LifoQueue(maxsize=pool_size)

        # Fill pool with initial connections
        for _ in range(pool_size):
            conn = create_connection_fn()
            self._pool.put(conn)

    def get_connection(self, timeout=None):
        """Get an available connection from the queue"""
        try:
            return self._pool.get(block=True, timeout=timeout)
        except queue.Empty:
            raise Exception("No available connections in the pool.")

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
        self.model_loaded = False

    def cursor(self):
        options = self.driver.get('options', {})
        # Make shure all cursor objects run .close() when exiting `with` statements
        return closing(self._cnxn.cursor(**options))

    def commit(self):
        return self._cnxn.commit()

    def close(self):
        return self._cnxn.close()

    def enable_load_extension(self, val):
        return self._cnxn.enable_load_extension(val)

    def load_extension(self, path):
        return self._cnxn.load_extension(path)

    def load_model(self, path):
        print('loads embedding model ...')
        self._cnxn.execute(f"SELECT llm_model_load('{path}', 'gpu_layers=0');")
        self._cnxn.execute("SELECT llm_context_create_embedding('embedding_type=FLOAT32');")
        self.model_loaded = True


class Engine:

    def __init__(self, state, driver, db_name=None):

        cfg = state.cfg
        self.state = state.meta[cfg.host]
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
        model_dir = os.path.expanduser(cfg.gguf_model_dir)
        self.model_path = os.path.join(model_dir, cfg.gguf_model)

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

    @property
    def version(self):
        if hasattr(self, '_version'):
            return self._version
        cnxn = self.connect()
        refl = Reflection(self, cnxn)
        self._version = refl.get_version()
        cnxn.close()
        return self._version

    def connect(self):
        try:
            if len(self.connect_params):
                cnxn = self.driver_module.connect(**self.connect_params)
            else:
                cnxn = self.driver_module.connect(self.cnxnstr)
        except Exception as ex:
            print(ex)
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail=str(ex)
            )
        if self.query:
            queries = self.query.split(';')
            for query in queries:
                cnxn.execute(query)

        if self.driver_name == 'sqlite3' and self.model_path:
            vector_path = util.get_installed_vector_path()
            ai_path = util.get_installed_ai_path()
            if vector_path and ai_path:
                cnxn.enable_load_extension(True)
                cnxn.load_extension(str(vector_path))
                cnxn.load_extension(ai_path)
                cnxn.enable_load_extension(False)

        return Connection(cnxn, self.driver)


class ODBC_Engine:
    """Connect to database"""

    def __init__(self, state, driver, db_name=None):
        cfg = state.cfg
        self.state = state.meta[cfg.host]
        self.name = cfg.system
        self.host = cfg.host
        self.db_name = db_name
        self.odbc_driver_name = self.name if self.name != 'mssql' else 'sql server'
        odbc_driver = self.get_driver()
        self.driver_name = 'pyodbc'
        self.driver = driver

        pattern = r'([\w\.-]+)(:\d+)?([/\\]\w+)?'
        match = re.search(pattern, cfg.host)

        if 'dbname' in driver.dbname:
            default_dbname = driver.dbname
        else:
            default_dbname = None

        config = Dict({
            'host': self.host,
            'port': None if match.group(2) is None else match.group(2)[1:],
            'sid': None if match.group(3) is None else match.group(3)[1:],
            'user': cfg.uid,
            'pass': cfg.pwd,
            'dbname': db_name or default_dbname,
            'path': (os.path.join(cfg.host, db_name)
                     if cfg.system in ('sqlite', 'duckdb') else None)
        })

        cnxn_string = driver.string.format(**config)
        cnxn_key_value_pairs = cnxn_string.split(';')
        cnxnstr = 'Driver={' + odbc_driver + '};'
        for key_value in cnxn_key_value_pairs:
            parts = key_value.split('=')
            if len(parts) > 1:
                value = parts[1]
                if value != 'None':
                    cnxnstr += key_value + ';'

        cnxnstr = cnxnstr[0:-1]
        self.cnxnstr = cnxnstr
        self.user = cfg.uid
        self.url = Dict({
            'username': cfg.uid,
            'database': (config.path if cfg.system == 'sqlite'
                         else db_name.split('.')[0] if db_name else None)
        })

    def connect(self):
        cnxn = pyodbc.connect(self.cnxnstr)
        pyodbc.lowercase = False
        return Connection(cnxn, self.driver)
        # return cnxn

    def get_driver(self):
        """Get ODBC driver"""
        drivers = [d for d in pyodbc.drivers() if self.odbc_driver_name in d.lower()]
        drivers.sort(reverse=True, key=lambda x: 'unicode' in x.lower())

        try:
            return drivers[0]
        except IndexError:
            raise HTTPException(
                status_code=501, detail=self.name + " ODBC driver missing"
            )
