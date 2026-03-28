import importlib
import os
import re
from addict import Dict
from fastapi import HTTPException
from contextlib import closing


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
        if importlib.util.find_spec(driver.name) is None:
            msg = 'Please install driver ' + driver.name
            raise HTTPException(
                status_code=404,
                detail=msg
            )

        self.driver = driver
        self.driver_module = importlib.import_module(driver.name, package=None)
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
                    self.connect_params[key] = value;
            else:
                self.cnxnstr = key_value

        self.url = Dict({
            'username': cfg.uid,
            'database': (config.path if cfg.system in ('sqlite', 'duckdb')
                         else db_name.split('.')[0] if db_name else None)
        })

    def connect(self):
        if len(self.connect_params):
            cnxn = self.driver_module.connect(**self.connect_params)
        else:
            cnxn = self.driver_module.connect(self.cnxnstr)
        if self.query:
            cnxn.execute(self.query)

        return Connection(cnxn, self.driver)

