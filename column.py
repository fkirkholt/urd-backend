import time
import pypandoc
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
    def __init__(self, tbl, name):
        self.db = tbl.db
        self.tbl = tbl
        self.name = name

    def get_element(self, col, type_):
        """ Get html element for input field """

        options = []
        fkeys = self.tbl.get_fkeys()
        # Decides what sort of input should be used
        if type_ == 'date':
            element = 'input[type=date]'
        elif type_ == 'boolean':
            if col.nullable:
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
        elif self.name in fkeys:
            element = 'select'
            options = []
        elif type_ == 'binary' or (type_ == 'string' and (
                col.column_size == 0 or col.column_size > 255)):
            element = "textarea"
        else:
            element = "input[type=text]"

        return element, options

    @measure_time
    def get_field(self, col):
        from table import Table
        type_ = self.db.expr.to_urd_type(col.type_name)
        fkeys = self.tbl.get_fkeys()
        pkey = self.tbl.get_pkey()

        element, options = self.get_element(col, type_)

        field = Dict({
            'name': self.name,
            'datatype': type_,
            'element': element,
            'nullable': col.nullable == 1,
            'label': self.db.get_label(self.name),
            'attrs': self.db.get_attributes(self.tbl.name, self.name)
        })

        fkey = self.get_fkey()
        if 'column_size' in col:
            field.size = int(col.column_size)
        if 'scale' in col and col.scale:
            field.scale = int(col.scale)
            field.precision = int(col.precision)
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

                field.options = self.get_options(field)

        if (
            col.get('auto_increment', None) or (
                type_ in ['integer', 'decimal'] and len(pkey.columns) and
                self.name == pkey.columns[-1] and self.name not in fkeys
            )
        ):
            field.extra = "auto_increment"

        if (
            col.column_def and not col.auto_increment and
            col.column_def != 'NULL'
        ):
            def_vals = col.column_def.split('::')
            default = def_vals[0]
            default = default.replace("'", "")

            # TODO: Sjekk om jeg trenger å endre current_timestamp()

            field.default = self.db.expr.replace_vars(default)
            if (field.default != default):
                field.default_expr = default

        return field

    @measure_time
    def get_options(self, field, fields=None):
        from database import Database
        from table import Table, Grid

        fk = field.fkey
        pkey_col = fk.primary[-1]

        if fk.base == self.db.cat and fk.schema == self.db.schema:
            base = self.db
        else:
            base = Database(self.db.cnxn, fk.base or fk.schema)

        cand_tbl = Table(base, fk.table)
        grid = Grid(cand_tbl)

        # Field that holds the value of the options
        value_field = f'"{field.name}".' + pkey_col

        # Sorting
        cand_sort_columns = grid.get_sort_columns()
        sort_cols = [field.name + '.' + col for col in cand_sort_columns]

        order = "order by " + ', '.join(sort_cols) if len(sort_cols) else ''

        # Conditions
        conditions = []
        if 'filter' in fk:
            conditions.append("("+self.db.expr.replace_vars(fk.filter)+")")

        fkeys = []
        for fkey in self.tbl.get_fkeys().values():
            if (field.name in fkey.foreign and fkey.foreign.index(field.name)):
                fkey.foreign_idx = fkey.foreign.index(field.name)
                fkey.length = len(fkey.foreign)
                fkeys.append(fkey)

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
                        cond = pkey_col + ' in '
                        cond += '(select ' + fkey.primary[fkey.foreign_idx]
                        cond += ' from ' + fkey.table
                        cond += ' where ' + fkey.primary[idx] + " = ?)"
                        conditions.append(cond)
                        params.append(fields[col].value)

        condition = " AND ".join(conditions) if len(conditions) else ''

        # Count records

        sql = "select count(*)\n"
        sql += f'from {self.db.schema or self.db.cat}."{cand_tbl.name}" '
        sql += f'"{field.name}"\n'
        sql += f'where {condition}' if condition else ''

        count = self.db.query(sql, params).fetchval()

        if (count > 200):
            return False

        sql = "select " + value_field + " as value, "
        sql += "(" + (field.view or value_field) + ") as label, "
        sql += "(" + (field.column_view or value_field) + ") as coltext "
        sql += f'from {self.db.schema or self.db.cat}."{cand_tbl.name}" '
        sql += f'"{field.name}"\n'
        sql += f'where {condition}\n' if condition else ''
        sql += order

        rows = self.db.query(sql, params).fetchall()

        result = []
        for row in rows:
            colnames = [column[0] for column in row.cursor_description]
            result.append(dict(zip(colnames, row)))

        return result

    @measure_time
    def get_select(self, req):
        """Get options for searchable select"""
        search = None if 'q' not in req else req.q.replace("*", "%")

        view = req.get('view') or self.name
        col_view = req.get('column_view') or self.name

        conds = req.condition.split(" and ") if req.condition else []
        # ignore case
        if search:
            search = search.lower()
            conds.append(f"lower(cast({view} as char)) like '%{search}%'")

        if len(conds):
            cond = " and ".join(conds)
        else:
            cond = self.name + " IS NOT NULL"

        val_col = req.alias + "." + self.name

        sql = f"""
        select distinct {val_col} as value, {view} as label,
        {col_view} as coltext\n
        from {self.tbl.name} {req.alias}\n
        where {cond}\n
        order by {view}
        """

        rows = self.db.query(sql).fetchmany(int(req.limit))

        result = []
        for row in rows:
            result.append({'value': row.value, 'label': row.label})

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

    def convert(self, from_format, to_format):

        select = ', '.join(self.tbl.pkey.columns)

        sql = f"""
        select {select}, {self.name}
        from {self.tbl.name}
        """

        cursor = self.db.query(sql)
        rows = cursor.fetchall()
        colnames = [col[0] for col in cursor.description]
        cursor2 = self.db.cnxn.cursor()
        for row in rows:
            row = (dict(zip(colnames, row)))
            wheres = []
            params = []
            for key in self.tbl.pkey.columns:
                wheres.append(key + '=?')
                params.append(row[key])

            where = ', '.join(wheres)

            try:
                text = pypandoc.convert_text(row[self.name], to_format,
                                             format=from_format)
            except Exception as e:
                print('kunne ikke konvertere ' + params[-1])
                print(e.message)

            params.insert(0, text)

            sql = f"""
            update {self.tbl.name}
            set {self.name} = ?
            where {where}
            """

            cursor2.execute(sql, params)

        cursor2.commit()

        return 'success'
