import re
import time
from functools import wraps
from sqlalchemy import text
from settings import Settings
from addict import Dict


cfg = Settings()


def time_func(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        if (end - start) > 0.1:
            print(f"{func.__name__} took {end - start:.6f} seconds")
        return result
    return wrapper


def time_stream_generator(func):
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        async for item in func(*args, **kwargs):
            yield item
        end_time = time.time()
        print(f"{func.__name__} took {end_time - start_time:.4f} seconds")
    return wrapper


def to_rec(row, crsr, lowercase=False):
    # Fixes additional characters at end of column names
    # This happens with special unicode characters in column name
    cols = [col[0] if '\x00' not in col[0]
            else col[0][:col[0].index('\x00')]
            for col in crsr.description]
    if lowercase:
        cols = [col.lower() for col in cols]
    return Dict(zip(cols, row))


def format_fkey(fkey, pkey):
    fkey = Dict(fkey)
    if (
        pkey.columns and
        set(pkey.columns) <= set(fkey.constrained_columns)
    ):
        fkey.relationship = '1:1'
    else:
        fkey.relationship = '1:M'

    ref_table_alias = fkey.name

    return fkey, ref_table_alias


