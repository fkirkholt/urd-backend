import re
from sqlalchemy import text
from settings import Settings
from addict import Dict


cfg = Settings()


def prepare(sql, params={}):
    params_prep = params
    if cfg.use_odbc:
        p = re.compile(r'(?<!:)\:\w*\b')
        placeholders = p.findall(sql)
        print('placeholders', placeholders)
        sql_prep = re.sub(r'(?<!:)\:\w*\b', '?', sql)
        if type(params) is not list:
            params_prep = []
            for ph in placeholders:
                key = ph[1:]
                val = params[key] 
                params_prep.append(val)
    else:
        sql_prep = text(sql)

    print('sql_prep', sql_prep)
    print('params_prep', params_prep)
    return sql_prep, params_prep


def to_rec(row):
    if cfg.use_odbc:
        cols = [col[0] for col in row.cursor_description]
        return Dict(zip(cols, row))
    else:
        return Dict(dict(row._mapping))

