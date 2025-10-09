import re
from sqlalchemy import text
from settings import Settings
from addict import Dict


cfg = Settings()


def prepare(sql, params={}):
    params_prep = params
    sql_prep = sql
    if cfg.use_odbc:
        if params:
            p = re.compile(r'(?<!:)\:[a-zA-ZæøåÆØÅ_]\w*\b')
            placeholders = p.findall(sql)
            sql_prep = re.sub(r'(?<!:)\:[a-zA-ZæøåÆØÅ_]\w*\b', '?', sql)
            if type(params) is not list:
                params_prep = []
                for ph in placeholders:
                    key = ph[1:]
                    val = params[key]
                    params_prep.append(val)
        else:
            params_prep = []
    else:
        if params:
            sql_prep = text(sql)
        else:
            sql_prep = text(re.sub(r'([\:])', r'\\\1', sql))

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


def format_fkey(fkey, cat, schema, tbl_name, pkey):
    fkey = Dict(fkey)
    fkey.base = cat
    fkey.table_name = tbl_name
    fkey.schema = schema
    if (
        pkey.columns and
        set(pkey.columns) <= set(fkey.constrained_columns)
    ):
        fkey.relationship = '1:1'
    else:
        fkey.relationship = '1:M'

    fkey.name = fkey.table_name + '_'
    fkey.name += '_'.join(fkey.constrained_columns)+'_fkey'

    fkey_col = fkey.constrained_columns[-1]
    ref_col = fkey.referred_columns[-1].strip('_')
    if fkey_col in [fkey.referred_table + '_' + ref_col,
                    fkey.referred_columns[-1]]:
        ref_table_alias = fkey.referred_table
    else:
        ref_table_alias = fkey_col.strip('_')

    return fkey, ref_table_alias

