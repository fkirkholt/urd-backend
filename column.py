import time
from addict import Dict


def measure_time(func):
    def wrapper(*arg):
        t = time.time()
        res = func(*arg)
        if (time.time()-t) > 1:
            print("Time in", func.__name__,  str(time.time()-t), "seconds")
        return res

    return wrapper


class Column:
    def __init__(self, tbl, col):
        self.db = tbl.db
        self.tbl = tbl
        self.name = col.column_name
        self.nullable = col.nullable
        if 'column_size' in col or 'display_size' in col:
            self.size = col.get('column_size', col.display_size)
        if 'scale' in col:
            self.scale = col.scale
            self.precision = col.precision
        if 'auto_increment' in col:
            self.auto_increment = col.auto_increment
        self.default = col.column_def
        # Strip column size from type_name for sqlite3
        col.type_name = col.type_name.split('(')[0].strip()
        self.datatype = self.db.expr.to_urd_type(col.type_name)

    def get_fkey(self):
        """Get foreign key for primary key column"""
        col_fkey = None
        fkeys = self.tbl.get_fkeys()
        for fkey in fkeys.values():
            if (fkey.foreign[-1] == self.name):
                if (not col_fkey or len(fkey.foreign) < len(col_fkey.foreign)):
                    col_fkey = fkey

        return col_fkey

    def get_size(self):
        sql = f"""
        select max(length({self.name}))
        from {self.tbl.name}
        """

        return self.db.query(sql).fetchval()

    def create_index(self, col_type):
        if col_type not in ['blob', 'clob', 'text']:
            sql = f"""
            create index {self.tbl.name}_{self.name}_idx
            on {self.tbl.name}({self.name})
            """

            self.db.query(sql).commit()
        else:
            sql = f"""
            create index {self.tbl.name}_{self.name}_is_null_idx
            on {self.tbl.name}({self.name})
            where {self.name} is null
            """

            self.db.query(sql).commit()

    @measure_time
    def check_use(self):
        """Check ratio of columns that's not null"""
        if not self.tbl.rowcount:
            return 0

        sql = f"""
        select count(*) from {self.tbl.name}
        where {self.name} is null or {self.name} = ''
        """

        count = self.db.query(sql).fetchval()

        rowcount = self.tbl.rowcount
        use = (rowcount - count)/rowcount

        return use

    @measure_time
    def check_frequency(self):
        """Check if one value is used much more than others"""
        if not self.tbl.rowcount:
            return 0

        sql = f"""
        select max(count) from (
            select count(*) as count, {self.name} as value
            from {self.tbl.name}
            group by {self.name}
        ) t2
        """

        max_in_group = self.db.query(sql).fetchval()

        frequency = max_in_group/self.tbl.rowcount

        return frequency
