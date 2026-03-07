import os
from addict import Dict
import pyodbc
import re
from pathlib import Path
from fastapi import HTTPException
from ruamel.yaml import YAML

yaml = YAML()

with open(Path(Path(__file__).parent, "drivers.yml"), "r") as content:
    drivers = yaml.load(content)


class ODBC_Engine:
    """Connect to database"""

    def __init__(self, cfg, db_name=None):
        self.name = cfg.system
        self.host = cfg.host
        self.db_name = db_name
        self.odbc_driver_name = self.name if self.name != 'mssql' else 'sql server'
        odbc_driver = self.get_driver()
        self.driver_name = 'pyodbc'

        pattern = r'([\w\.-]+)(:\d+)?([/\\]\w+)?'
        match = re.search(pattern, cfg.host)

        driver = Dict(drivers['pyodbc'])

        if 'path' in driver.system[self.name].params:
            path = os.path.join(cfg.host, db_name)
        else:
            path = None

        if 'dbname' in driver.system[self.name].dbname: 
            default_dbname = driver.system[self.name].dbname
        else:
            default_dbname = None

        config = Dict({
            'host': self.host,
            'port': None if match.group(2) is None else match.group(2)[1:],
            'sid': None if match.group(3) is None else match.group(3)[1:],
            'user': cfg.uid,
            'pass': cfg.pwd,
            'dbname': db_name or default_dbname,
            'path': path
        })
        params = []
        for param in driver.system[self.name].params:
            params.append(config[param])

        cnxn_string = driver.system[self.name].string.format(**config)
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
            'database': (path if cfg.system == 'sqlite'
                         else db_name.split('.')[0] if db_name else None)
        })

    def connect(self):
        cnxn = pyodbc.connect(self.cnxnstr)
        pyodbc.lowercase = False
        return cnxn

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

