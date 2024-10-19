import os
from addict import Dict
import pyodbc
from fastapi import HTTPException
from starlette import status


class ODBC_Engine:
    """Connect to database"""

    def __init__(self, cfg, db_name=None):
        self.name = cfg.system
        self.host = cfg.host
        driver = self.get_driver()
        cnxnstr = 'Driver={' + driver + '};'
        if cfg.system == 'postgresql' and db_name is None:
            cnxnstr += 'Database=postgres;'
        elif (db_name and cfg.system != 'oracle'):
            path = db_name.split('.')
            cnxnstr += 'Database=' + path[0] + ';'
        if cfg.system == 'oracle':
            cnxnstr += "DBQ=" + cfg.host + ';'
        else:
            srv_parts = cfg.host.split(':')
            cnxnstr += 'Server=' + srv_parts[0] + ';'
            if len(srv_parts) == 2:
                cnxnstr += 'Port=' + srv_parts[1] + ';'
        cnxnstr += 'Uid=' + cfg.uid + ';Pwd=' + cfg.pwd + ';'
        pyodbc.lowercase = True
        if self.name == 'sql server':
            cnxnstr += 'ENCRYPT=no;MARS_Connection=yes;'
            pyodbc.lowercase = False
        if self.name == 'sqlite':
            pyodbc.lowercase = False
            path = os.path.join(cfg.host, db_name + '.db')
            cnxnstr = 'Driver=SQLite3;Database=' + path
            if os.path.exists(path):
                cnxn = pyodbc.connect(cnxnstr)
            else:
                raise HTTPException(
                    status_code=404, detail="Database not found"
                )
        else:
            try:
                cnxn = pyodbc.connect(cnxnstr)
            except Exception as e:
                print(e)
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid authentication"
                )
        cnxn.setencoding(encoding='utf8')
        self.cnxnstr = cnxnstr
        self.cursor = cnxn.cursor
        self.user = cfg.uid
        self.string = cnxnstr
        self.url = Dict({
            'username': cfg.uid,
            'database': db_name
        })

    def connect(self):
        cnxn = pyodbc.connect(self.cnxnstr)

        return cnxn

    def get_driver(self):
        """Get ODBC driver"""
        drivers = [d for d in pyodbc.drivers() if self.name in d.lower()]
        drivers.sort(reverse=True, key=lambda x: 'unicode' in x.lower())

        try:
            return drivers[0]
        except IndexError:
            raise HTTPException(
                status_code=501, detail=self.name + " ODBC driver missing"
            )

