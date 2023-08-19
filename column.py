import time


class Column:
    def __init__(self, tbl, col):
        self.db = tbl.db
        self.tbl = tbl
        self.name = col.name
        self.nullable = col.nullable
        if hasattr(col.type, 'length'):
            self.size = col.type.length
        if hasattr(col.type, 'display_width'):
            self.size = col.type.display_width
        if hasattr(col.type, 'scale'):
            self.scale = col.type.scale
            self.precision = col.type.precision
        if 'auto_increment' in col:
            self.auto_increment = col.auto_increment
        self.default = col.column_def
        try:
            self.datatype = col.type.python_type.__name__
        except Exception as e:
            print(e)
            print(col.type)
            self.datatype = 'unknown'
        if self.datatype == 'int' and getattr(self, 'size', 0) == 1:
            self.datatype = 'bool'

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
