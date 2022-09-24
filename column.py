import json
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

    @measure_time
    def get_field(self, col):
        from table import Table
        type_ = self.db.expr.to_urd_type(col.type_name)
        fkeys = self.tbl.get_fkeys()
        pkey = self.tbl.get_pkey()

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

        field = Dict({
            'name': self.name,
            'datatype': type_,
            'element': element,
            'nullable': col.nullable == True,
            'label': self.db.get_label(self.name),
            'attrs': self.db.get_attributes(self.tbl.name, self.name)
        })

        for fkey in fkeys.values():
            if fkey.foreign[-1] == field.name:
                if (not field.fkey or len(fkey.foreign) < len(field.fkey.foreign)):
                    field.fkey = fkey
                    field.element = 'select'

        if 'column_size' in col:
            field.size = int(col.column_size)
        if 'scale' in col and col.scale:
            field.scale = int(col.scale)
            field.precision = int(col.precision)
        if col.get('auto_increment', None):
            field.extra = "auto_increment"
        if element == "select" and len(options):
            field.options = options
        elif field.fkey:
            ref_tbl = Table(self.db, field.fkey.table)
            if field.fkey.table in self.db.user_tables:
                ref_pk = ref_tbl.get_pkey()

                if ref_tbl.is_hidden() is False:
                    field.expandable = True

                for index in ref_tbl.get_indexes().values():
                    if index.columns != ref_pk and index.unique:
                        # Only last pk column is used in display value,
                        # other pk columns are usually foreign keys
                        cols = [self.name+"."+col for col in index.columns if col not in ref_pk[0:-1]]
                        field.view = " || ', ' || ".join(cols)
                        if index.name.endswith("_sort_idx"):
                            break

                if 'column_view' not in field and 'view' in field:
                    field.column_view = field.view

                field.options = self.get_options(field)


        if (type_ in ['integer', 'decimal'] and len(pkey) and self.name == pkey[-1] and self.name not in fkeys):
            field.extra = "auto_increment"

        if col.column_def and not col.auto_increment and col.column_def != 'NULL':
            def_vals = col.column_def.split('::')
            default = def_vals[0]
            default = default.replace("'", "")

            #TODO: Sjekk om jeg trenger å endre current_timestamp()

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
        value_field = field.name + '.' + pkey_col

        # Sorting
        cand_sort_columns = grid.get_sort_columns()
        sort_fields = [field.name + '.' + col for col in cand_sort_columns]

        order = "order by " + ', '.join(sort_fields) if len(sort_fields) else ''

        # Conditions
        conditions = []
        if 'filter' in fk:
            conditions.append("("+self.db.expr.replace_vars(fk.filter)+")")

        fkeys = []
        for fkey in self.tbl.get_fkeys().values():
            if (field.name in fkey.foreign and fkey.foreign.index(field.name) > 0):
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
                        cond = pkey_col + ' in (select ' + fkey.primary[fkey.foreign_idx]
                        cond += ' from ' + fkey.table + ' where ' + fkey.primary[idx] + " = ?)"
                        conditions.append(cond)
                        params.append(fields[col].value)

        condition = "where " + " AND ".join(conditions) if len(conditions) else ''

        # Count records

        sql = "select count(*)\n"
        sql+= f"from {self.db.schema or self.db.cat}.{cand_tbl.name} {field.name}\n"
        sql+= condition

        count = self.db.query(sql, params).fetchval()

        if (count > 200):
            return False

        sql = "select " + value_field + " as value, "
        sql+= "(" + (field.view or value_field) + ") as label, "
        sql+= "(" + (field.column_view or value_field) + ") as coltext "
        sql+= f"from {self.db.schema or self.db.cat}.{cand_tbl.name} {field.name}\n"
        sql+= condition + "\n" + order

        rows = self.db.query(sql, params).fetchall()

        result = []
        for row in rows:
            colnames = [column[0] for column in row.cursor_description]
            result.append(dict(zip(colnames, row)))

        return result

    @measure_time
    def get_select(self, req):
        """Get options for searchable select"""
        search = None if not 'q' in req else req.q.replace("*", "%")

        view = req.get('view') or self.name
        col_view = req.get('column_view') or self.name

        conds = req.condition.split(" and ") if req.condition else []
        # ignore case
        if search:
            search = search.lower()
            conds.append(f"lower(cast({view} as varchar)) like '%{search}%'")

        cond = " and ".join(conds) if len(conds) else self.name + " IS NOT NULL"

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

    def get_size(self):
        sql = f"""
        select max(length({self.name}))
        from {self.tbl.name}
        """

        return self.db.query(sql).fetchval()

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

        select = ', '.join(self.tbl.pkey)

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
            for key in self.tbl.pkey:
                wheres.append(key + '=?')
                params.append(row[key])

            where = ', '.join(wheres)

            try:
                text = pypandoc.convert_text(row[self.name], to_format, format=from_format)
            except:
                print('kunne ikke konvertere ' + params[-1])

            params.insert(0, text)

            sql = f"""
            update {self.tbl.name}
            set {self.name} = ?
            where {where}
            """

            result = cursor2.execute(sql, params)

        cursor2.commit()

        return 'success'
