import re
from sqlalchemy import text
from settings import Settings
from addict import Dict


cfg = Settings()


def prepare(sql, params={}):
    params_prep = params
    if cfg.use_odbc:
        p = re.compile(r'(?<!:)\:[a-zA-ZæøåÆØÅ]\w*\b')
        placeholders = p.findall(sql)
        sql_prep = re.sub(r'(?<!:)\:\w*\b', '?', sql)
        if type(params) is not list:
            params_prep = []
            for ph in placeholders:
                key = ph[1:]
                val = params[key] 
                params_prep.append(val)
    else:
        sql_prep = text(sql)

    return sql_prep, params_prep


def to_rec(row):
    if cfg.use_odbc:
        # Fixes additional characters at end of column names
        # This happens with special unicode characters in column name
        cols = [col[0] if '\x00' not in col[0]
                else col[0][:col[0].index('\x00')]
                for col in row.cursor_description]
        return Dict(zip(cols, row))
    else:
        return Dict(dict(row._mapping))

