import importlib
import os
import re
from addict import Dict
from fastapi import HTTPException
from pathlib import Path
from ruamel.yaml import YAML
from contextlib import closing


yaml = YAML()

with open(Path(Path(__file__).parent, "drivers.yml"), "r") as content:
    drivers = yaml.load(content)


class Connection:

    def __init__(self, cnxn):
        self._cnxn = cnxn

    def cursor(self):
        # Make shure all cursor objects run .close() when exiting `with` statements
        return closing(self._cnxn.cursor())

    def commit(self):
        return self._cnxn.commit()

    def close(self):
        return self._cnxn.close()


class Engine:

    def __init__(self, cfg, db_name=None):

        self.name = cfg.system
        self.db_name = db_name
        self.host = cfg.host
        self.driver_name = getattr(cfg, f'{cfg.system}_driver')
        if importlib.util.find_spec(self.driver_name) is None:
            msg = 'Please install driver ' + self.driver_name
            raise HTTPException(
                status_code=404,
                detail=msg
            )

        self.driver_module = importlib.import_module(self.driver_name, package=None)

        pattern = r'([\w\.-]+)(:\d+)?([/\\]\w+)?'
        match = re.search(pattern, cfg.host)

        config = Dict({
            'host': match.group(1),
            'port': None if match.group(2) is None else match.group(2)[1:],
            'sid': None if match.group(3) is None else match.group(3)[1:],
            'user': cfg.uid,
            'pass': cfg.pwd,
            'dbname': db_name,
            'path': os.path.join(cfg.host, db_name) if db_name else None
        })
        driver = Dict(drivers[self.driver_name])
        params = []
        for param in driver.system[self.name].params:
            params.append(config[param])

        cnxn_string = driver.system[self.name].string % tuple(params)
        cnxn_key_value_pairs = cnxn_string.split(';')
        self.connect_params = {}
        self.cnxnstr = None
        for key_value in cnxn_key_value_pairs:
            parts = key_value.split('=')
            if len(parts) > 1:
                key = parts[0].strip()
                value = parts[1]
                if value:
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
        driver = Dict(drivers[self.driver_name])
        if driver.system[self.name].query:
            cnxn.execute(driver.system[self.name].query)

        return Connection(cnxn)

