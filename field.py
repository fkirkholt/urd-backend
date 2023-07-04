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


class Field:

    def __init__(self, tbl, name):
        self.tbl = tbl
        self.db = tbl.db
        self.name = name

    @measure_time
    def get(self, col):
        fkeys = self.tbl.get_fkeys()
        pkey = self.tbl.get_pkey()
        type_ = self.db.expr.to_urd_type(col.type_name)

        element, options = self.get_element(col)

        field = Dict({
            'name': self.name,
            'datatype': type_,
            'element': element,
            'nullable': col.nullable == 1,
            'label': self.db.get_label('field', self.name),
            'attrs': self.get_attributes(self.tbl.name, self.name),
        })

        fkey = self.tbl.get_fkey(self.name)
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
            field.view = self.get_view(fkey)
        if (
            col.get('auto_increment', None) or (
                type_ in ['integer', 'decimal'] and
                len(pkey.columns) and
                self.name == pkey.columns[-1] and
                self.name not in fkeys
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

            field.default = self.db.expr.replace_vars(default, self.db)
            if (field.default != default):
                field.default_expr = default

        return field

    def get_element(self, col):
        """ Get html element for input field """

        options = []
        # Decides what sort of input should be used
        if col.datatype == 'date':
            element = 'input[type=date]'
        elif col.datatype == 'boolean':
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
        elif col.datatype == 'binary' or (col.datatype == 'string' and (
                col.size == 0 or col.size >= 255)):
            element = "textarea"
        else:
            element = "input[type=text]"

        return element, options

    def get_attributes(self, table_name, identifier):
        """Get description based on term"""
        attrs = self.db.get_html_attributes()
        column_ref = table_name + '.' + identifier
        attributes = {}
        if column_ref in attrs.field:
            attributes = attrs.field[column_ref]
        elif identifier in attrs.field:
            attributes = attrs.field[identifier]

        return attributes

    def get_condition(self, fields=None):
        from table import Table

        # Find all foreign keys that limit the possible values of the field.
        # These represents hierarchy, and usually linked selects.
        fkeys = []
        for fkey in self.tbl.get_fkeys().values():
            if (self.name in fkey.foreign and fkey.foreign.index(self.name)):
                fkey.foreign_idx = fkey.foreign.index(self.name)
                fkey.length = len(fkey.foreign)
                fkeys.append(fkey)

        # Get conditions for fetching options, based on
        # other fields representing hierarchy of linked selects
        conditions = []
        params = []
        # Holds list over foreign keys, to check hierarchy
        fkeys_list = []
        if fields:
            for fkey in sorted(fkeys, key=lambda x: x['length']):
                fkeys_list.append(fkey.foreign)

                if fkey.foreign[:-1] in fkeys_list:
                    continue

                for idx, col in enumerate(fkey.foreign):
                    if col != self.name and fields[col].value:
                        cond = fkey.primary[idx] + " = ?"
                        conditions.append(cond)
                        params.append(fields[col].value)

        # Find possible field defining class
        fkey = self.tbl.get_fkey(self.name)
        ref_tbl = Table(self.db, fkey.table)
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
    def get_options(self, condition, params):

        fkey = self.tbl.get_fkey(self.name)
        pkey_col = fkey.primary[-1] if fkey else self.name
        from_table = fkey.table if fkey else self.tbl.name

        # Field that holds the value of the options
        value_field = f'"{self.name}".' + pkey_col

        condition = condition or '1=1'

        # Count records

        sql = f"""
        select count(*)
        from {self.db.schema or self.db.cat}."{from_table}" "{self.name}"
        where {condition}
        """

        count = self.db.query(sql, params).fetchval()

        if (count > 200):
            return False

        self.view = self.get_view(fkey) if fkey else self.name

        sql = f"""
        select {value_field} as value, {self.view or value_field} as label
        from   {self.db.schema or self.db.cat}."{from_table}" "{self.name}"
        where  {condition}
        order by {self.view or value_field}
        """

        rows = self.db.query(sql, params).fetchall()

        result = []
        for row in rows:
            colnames = [column[0] for column in row.cursor_description]
            result.append(dict(zip(colnames, row)))

        return result

    def get_view(self, fkey):
        """ Decide what should be shown in options """
        from table import Table
        ref_tbl = Table(self.db, fkey.table)
        ref_pk = ref_tbl.get_pkey()
        view = self.name + '.' + ref_pk.columns[-1]

        if fkey.table in self.db.user_tables:

            if ref_tbl.is_hidden() is False:
                self.expandable = True

            for index in ref_tbl.get_indexes().values():
                if index.columns != ref_pk.columns and index.unique:
                    # Only last pk column is used in display value,
                    # other pk columns are usually foreign keys
                    cols = [f'"{self.name}".{col}' for col in index.columns
                            if col not in ref_pk.columns[0:-1]]
                    view = " || ', ' || ".join(cols)
                    if index.name.endswith("_sort_idx"):
                        break

        return view
