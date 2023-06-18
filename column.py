import time
import simplejson as json
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

    def get_element(self):
        """ Get html element for input field """

        options = []
        # Decides what sort of input should be used
        if self.datatype == 'date':
            element = 'input[type=date]'
        elif self.datatype == 'boolean':
            if self.nullable:
                element = 'select'
                options = [
                    {
                        'value': 0,
                        'label': 'Nei'
                    },
                    {
                        'value': 1,
                        'label': 'Ja'
                    }
                ]
            else:
                element = 'input[type=checkbox]'
        elif self.datatype == 'binary' or (self.datatype == 'string' and (
                self.size == 0 or self.size >= 255)):
            element = "textarea"
        else:
            element = "input[type=text]"

        return element, options

    @measure_time
    def get_field(self):
        from table import Table
        fkeys = self.tbl.get_fkeys()
        pkey = self.tbl.get_pkey()

        element, options = self.get_element()

        field = Dict({
            'name': self.name,
            'datatype': self.datatype,
            'element': element,
            'nullable': self.nullable == 1,
            'label': self.db.get_label('field', self.name),
            'attrs': self.get_attributes(self.tbl.name, self.name)
        })

        fkey = self.tbl.get_fkey(self.name)
        if hasattr(self, 'size'):
            field.size = int(self.size)
        if getattr(self, 'scale', None):
            field.scale = int(self.scale)
            field.precision = int(self.precision)
        if element == "select" and len(options):
            field.options = options
        elif fkey:
            field.fkey = fkey
            field.element = 'select'
            ref_tbl = Table(self.db, field.fkey.table)

            # Decide what should be shown in options
            if field.fkey.table in self.db.user_tables:
                ref_pk = ref_tbl.get_pkey()

                if ref_tbl.is_hidden() is False:
                    field.expandable = True

                for index in ref_tbl.get_indexes().values():
                    if index.columns != ref_pk.columns and index.unique:
                        # Only last pk column is used in display value,
                        # other pk columns are usually foreign keys
                        cols = [f'"{self.name}".{col}' for col in index.columns
                                if col not in ref_pk.columns[0:-1]]
                        field.view = " || ', ' || ".join(cols)
                        if index.name.endswith("_sort_idx"):
                            break

                if 'column_view' not in field and 'view' in field:
                    field.column_view = field.view

        if (
            getattr(self, 'auto_increment', None) or (
                self.datatype in ['integer', 'decimal'] and
                len(pkey.columns) and
                self.name == pkey.columns[-1] and
                self.name not in fkeys
            )
        ):
            field.extra = "auto_increment"

        if (
            self.default and not getattr(self, 'auto_increment', None) and
            self.default != 'NULL'
        ):
            def_vals = self.default.split('::')
            default = def_vals[0]
            default = default.replace("'", "")

            # TODO: Sjekk om jeg trenger å endre current_timestamp()

            field.default = self.db.expr.replace_vars(default, self.db)
            if (field.default != default):
                field.default_expr = default

        return field

    def get_attributes(self, table_name, identifier):
        """Get description based on term"""
        attrs = self.db.get_html_attributes()
        column_ref = table_name + '.' + identifier
        attributes = None
        if column_ref in attrs.field:
            attributes = attrs.field[column_ref]
        elif identifier in attrs.field:
            attributes = attrs.field[identifier]

        return attributes

    def get_condition(self, field, fields=None):
        from table import Table

        fkeys = []
        for fkey in self.tbl.get_fkeys().values():
            if (field.name in fkey.foreign and fkey.foreign.index(field.name)):
                fkey.foreign_idx = fkey.foreign.index(field.name)
                fkey.length = len(fkey.foreign)
                fkeys.append(fkey)

        conditions = []
        params = []
        # Holds list over foreign keys, to check hierarchy
        fkeys_list = []
        if 'value' in field:
            for fkey in sorted(fkeys, key=lambda x: x['length']):
                fkeys_list.append(fkey.foreign)

                if fkey.foreign[:-1] in fkeys_list:
                    continue

                for idx, col in enumerate(fkey.foreign):
                    if col != field.name and fields[col].value:
                        cond = fkey.primary[idx] + " = ?"
                        conditions.append(cond)
                        params.append(fields[col].value)

        # Find possible field defining class
        ref_tbl = Table(self.db, field.fkey.table)
        indexes = ref_tbl.get_indexes()
        class_idx = indexes.get(ref_tbl.name + "_classification_idx", None)
        class_field = Dict({'options': []})
        if class_idx:
            class_field_name = class_idx.columns[0]
            fields = ref_tbl.get_fields()
            class_field = fields[class_field_name]

        # Tables with suffixes that's part of types
        # should just be shown when the specific type is chosen
        parts = self.tbl.name.split("_")
        suff_1 = parts[-1]
        suff_2 = '' if len(parts) == 1 else parts[-2]
        condition = None
        for class_ in [opt['value'] for opt in class_field.options]:
            if (suff_1.startswith(class_) or suff_2.startswith(class_)):
                conditions.append(class_field_name + ' = ?')
                params.append(class_)

        condition = " AND ".join(conditions) if len(conditions) else ''

        return condition, params

    @measure_time
    def get_options(self, field, condition, params):

        fkey = field.fkey
        pkey_col = fkey.primary[-1] if fkey else field.name
        from_table = fkey.table if fkey else self.tbl.name

        # Field that holds the value of the options
        value_field = f'"{field.name}".' + pkey_col

        condition = condition or '1=1'

        # Count records

        sql = f"""
        select count(*)
        from {self.db.schema or self.db.cat}."{from_table}" "{field.name}"
        where {condition}
        """

        count = self.db.query(sql, params).fetchval()

        if (count > 200):
            return False

        sql = f"""
        select {value_field} as value, {field.view or value_field} as label,
               {field.column_view or value_field} as coltext
        from   {self.db.schema or self.db.cat}."{from_table}" "{field.name}"
        where  {condition}
        order by {field.view or value_field}
        """

        rows = self.db.query(sql, params).fetchall()

        result = []
        for row in rows:
            colnames = [column[0] for column in row.cursor_description]
            result.append(dict(zip(colnames, row)))

        return result

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
