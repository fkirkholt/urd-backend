import re
import time
import inspect
from functools import wraps
from settings import Settings
from addict import Dict


cfg = Settings()
indent = 2


def time_func(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        if (end - start) > 0.1:
            print(f"{func.__module__}.{func.__name__} took {end - start:.6f} seconds")
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


def log_caller(func):
    """A decorator to log the name of the caller function."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        global indent
        # Get the call stack
        stack = inspect.stack()
        # The frame record at index 1 is the caller of the current wrapper function
        caller_frame_record = stack[1]
        # The fourth element of the frame record (index 3) is the function name
        caller_name = caller_frame_record[3]
        caller_file = caller_frame_record[1].split('/')[-1]
        caller_lnr = caller_frame_record[2]
        caller_ref = caller_file + ':' + str(caller_lnr)

        print(f"{'>' * indent} Function '{func.__name__}' was called by '{caller_name}' in '{caller_ref}' ---")
        start = time.perf_counter()
        indent += 2
        result = func(*args, **kwargs)
        indent -= 2
        end = time.perf_counter()
        print(f"{'<' * indent} Function '{func.__name__}' finished executing in {end - start:.6f} seconds ---")
        return result
    return wrapper
