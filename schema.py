import json
from config import config
import pyodbc

class Schema:
    def __init__(self, name):
        with open('schemas/' + name + '/schema.json') as myfile:
            data = myfile.read()

        schema = json.loads(data)

        self.name = name
        self.tables = schema["tables"]
        self.reports = schema.get("reports", [])
        self.contents = schema.get("contents", [])

    def get_db_name(self):
        sql = """
        select name
        from datatabase_
        where schema_ = ?
        """
        cnxnstr = config['db']['connection_string']
        urd_cnxn = pyodbc.connect(cnxnstr)
        cursor = urd_cnxn.cursor()

        base = cursor.execute(sql, self.name).fetchval()

        return base


    
