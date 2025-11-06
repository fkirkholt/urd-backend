from addict import Dict
from datatype import Datatype
from util import prepare
from expression import Expression


class Column:

    def __init__(self, tbl, col):
        self.db = tbl.db
        self.tbl = tbl
        self.size = None
        col = Dict(col)
        if col.default == 'NULL':
            col.default = None
        elif col.default:
            col.default = col.default.strip("'")
        self.type = col.type
        for attr in col.keys():
            setattr(self, attr, col[attr])
        if self.size == -1:
            self.size = None
        # Get size, scale and precision for odbc connection
        if type(col.type) is str and '(' in col.type:
            expr = Expression(self.db.engine.name)
            urd_type = expr.to_urd_type(col.type)
            size = col.type.split('(')[1].strip(')')
            if urd_type == 'str':
                self.size = int(size)
            else:
                size_parts = size.split(',')
                self.precision = int(size_parts[0])
                if len(size_parts) == 2:
                    self.scale = int(size_parts[1])
                else:
                    self.scale = 0
        elif type(col.type) is str and col.type == 'VARCHAR':
            # Set size of 'VARCHAR' columns in DuckDB, which doesn't
            # register any size on such columns
            self.size = 255
        # Get size, scale and precision for SQLAlchemy
        if hasattr(col.type, 'length'):
            self.size = col.type.length
        if hasattr(col.type, 'display_width'):
            self.size = col.type.display_width
        if hasattr(col.type, 'scale'):
            self.scale = col.type.scale or 0
            self.precision = col.type.precision

    def get_size(self):
        sql = f"""
        select max(length({self.name}))
        from {self.tbl.name}
        """

        with self.db.engine.connect() as cnxn:
            sql, _ = prepare(sql)
            return cnxn.execute(sql).fetcone()[0]

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
            sql, _ = prepare(sql)

            try:
                cnxn.execute(sql)
                cnxn.commit()
            except Exception as e:
                print(e)

    def check_use(self):
        """Check ratio of columns that's not null"""
        if not self.tbl.rowcount:
            return 0

        sql = f"""
        select count(*) from {self.tbl.name}
        where {self.name} is null or {self.name} = ''
        """

        with self.db.engine.connect() as cnxn:
            sql, _ = prepare(sql)
            count = cnxn.execute(sql).fetchone()[0]

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
            sql, _ = prepare(sql)
            max_in_group = cnxn.execute(sql).fetchone()[0]

        frequency = max_in_group/self.tbl.rowcount

        return frequency

    def get_def(self, dialect, blob_to_varchar=False, geometry_to_text=False):
        """Get column definition"""
        size = self.size if hasattr(self, 'size') else None
        expr = Expression(self.db.engine.name)
        urd_type = expr.to_urd_type(self.type)
        if urd_type == 'Decimal':
            size = str(self.precision)
            size += "," + (str(self.scale) if self.scale else '0')
        if type(self.type) is str:  # odbc engine
            datatype = Datatype(self.db.refl.expr.to_urd_type(self.type), size)
        else:
            datatype = Datatype(self.type.python_type.__name__, size)
        # Used to hold file path when exporting blobs as files
        if blob_to_varchar and datatype.type == 'bytes':
            native_type = 'varchar(200)'
        elif geometry_to_text and datatype.type == 'geometry':
            native_type = 'text'
        else:
            native_type = datatype.to_native_type(dialect)

        coldef = f"    {self.name} {native_type}"
        if not self.nullable:
            coldef += " NOT NULL"
        if self.default:
            default = self.default
            if 'current_timestamp()' in default:
                default = default.replace('current_timestamp()',
                                          'CURRENT_TIMESTAMP')
            elif 'curdate()' in default:
                default = default.replace('curdate()', 'CURRENT_DATE')
            elif 'current_user()' in default:
                default = default.replace('current_user()', 'CURRENT_USER')
            elif 'ON UPDATE' in default and dialect != 'mysql':
                default = default.split('ON UPDATE')[0]
            elif dialect == 'mysql' and default in ('CURRENT_DATE', 'CURRENT_USER'):
                default = '(' + default + ')'
            elif datatype == 'str':
                default = "'" + default + "'"

            coldef += " DEFAULT " + default

        return coldef
