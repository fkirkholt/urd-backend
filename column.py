from addict import Dict
from datatype import Datatype
from sqlalchemy import text


class Column:

    def __init__(self, tbl, col):
        self.db = tbl.db
        self.tbl = tbl
        col = Dict(col)
        self.type = col.type
        for attr in col.keys():
            setattr(self, attr, col[attr])
        if hasattr(col.type, 'length'):
            self.size = col.type.length
        if hasattr(col.type, 'display_width'):
            self.size = col.type.display_width
        if hasattr(col.type, 'scale'):
            self.scale = col.type.scale
            self.precision = col.type.precision

    def get_size(self):
        sql = f"""
        select max(length({self.name}))
        from {self.tbl.name}
        """

        with self.db.engine.connect() as cnxn:
            return cnxn.execute(text(sql)).fetchval()

    def create_index(self, col_type):
        if col_type not in ['blob', 'clob', 'text']:
            sql = f"""
            create index {self.tbl.name}_{self.name}_idx
            on {self.tbl.name}({self.name})
            """
        else:
            sql = f"""
            create index {self.tbl.name}_{self.name}_is_null_idx
            on {self.tbl.name}({self.name})
            where {self.name} is null
            """

        with self.db.engine.connect() as cnxn:
            cnxn.execute(text(sql))
            cnxn.commit()

    def check_use(self):
        """Check ratio of columns that's not null"""
        if not self.tbl.rowcount:
            return 0

        sql = f"""
        select count(*) from {self.tbl.name}
        where {self.name} is null or {self.name} = ''
        """

        with self.db.engine.connect() as cnxn:
            count = cnxn.execute(text(sql)).first()[0]

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

        with self.db.engine.connect() as cnxn:
            max_in_group = cnxn.execute(text(sql)).first()[0]

        frequency = max_in_group/self.tbl.rowcount

        return frequency

    def get_def(self, dialect):
        """Get column definition"""
        size = self.size if hasattr(self, 'size') else None
        if hasattr(self, 'precision') and self.precision is not None:
            size = str(self.precision)
            if hasattr(self, 'scale') and self.scale is not None:
                size += "," + str(self.scale)
        datatype = Datatype(self.type.python_type.__name__, size)
        native_type = datatype.to_native_type(self.db.engine.name)
        coldef = f"    {self.name} {native_type}"
        if not self.nullable:
            coldef += " NOT NULL"
        if self.default:
            default = self.default
            if 'current_timestamp()' in default:
                default = default.replace('current_timestamp()',
                                          'CURRENT_TIMESTAMP')
            if 'curdate()' in default:
                default = default.replace('curdate()', 'CURRENT_DATE')
            if 'current_user()' in default:
                default = default.replace('current_user()', 'CURRENT_USER')
            if 'ON UPDATE' in default and dialect != 'mysql':
                default = default.split('ON UPDATE')[0]
            coldef += " DEFAULT " + default

        return coldef
