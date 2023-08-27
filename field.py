from addict import Dict


class Field:

    def __init__(self, tbl, name):
        self.tbl = tbl
        self.db = tbl.db
        self.name = name

    def get(self, col):
        self.element, type_ = self.get_element(col)

        attrs = Dict()
        if type_:
            attrs['type'] = type_

        html_attrs = self.get_attributes(self.tbl.name, self.name)
        if 'data-type' in html_attrs:
            attrs['data-type'] = html_attrs['data-type']
        if 'data-format' in html_attrs:
            attrs['data-format'] = html_attrs['data-format']

        field = Dict({
            'name': self.name,
            'datatype': col.datatype,
            'element': self.element,
            'nullable': col.nullable == 1,
            'label': self.db.get_label(self.name),
            'attrs': attrs
        })

        fkey = self.tbl.get_fkey(self.name)
        if hasattr(col, 'size'):
            field.size = col.size
        if hasattr(col, 'scale'):
            field.scale = col.scale
            field.precision = col.precision
        if fkey:
            field.fkey = fkey
            field.element = 'select'
            field.view = self.get_view(fkey)
            field.expandable = self.expandable or False
        if (
            hasattr(col, 'auto_increment') or (
                col.datatype in ['int', 'Decimal'] and
                len(self.tbl.pkey.columns) and
                self.name == self.tbl.pkey.columns[-1] and
                self.name not in self.tbl.fkeys
            )
        ):
            field.extra = "auto_increment"

        if col.default:
            field.default = self.db.expr.replace_vars(col.default, self.db)
            if (field.default != col.default):
                field.default_expr = col.default

        return field

    def get_element(self, col):
        """ Get html element for input field """

        type_ = None
        # Decides what sort of input should be used
        if col.datatype == 'date':
            element = 'input'
            type_ = 'date'
        elif col.datatype == 'bool':
            element = 'input'
            type_ = 'checkbox'
        elif col.datatype == 'bytes' or (col.datatype == 'str' and (
                col.size is None or col.size >= 255)):
            element = "textarea"
        else:
            element = "input"
            type_ = 'text'

        return element, type_

    def get_attributes(self, table_name, identifier):
        """Get description based on term"""
        attrs = self.db.html_attrs
        ref = f'{self.element}[data-table="{table_name}"][name="{identifier}"]'
        attributes = {}
        if ref in attrs:
            attributes = attrs[ref]
        elif f'input[name="{identifier}"]' in attrs:
            attributes = attrs[f'input[name="{identifier}"]']

        return attributes

    def get_condition(self, fields=None):

        # Find all foreign keys that limit the possible values of the field.
        # These represents hierarchy, and usually linked selects.
        fkeys = []
        for fkey in self.tbl.fkeys.values():
            if (
                self.name in fkey.constrained_columns and
                fkey.constrained_columns.index(self.name)
            ):
                fkey.foreign_idx = fkey.constrained_columns.index(self.name)
                fkey.length = len(fkey.constrained_columns)
                fkeys.append(fkey)

        # Get conditions for fetching options, based on
        # other fields representing hierarchy of linked selects
        conditions = []
        params = {}
        # Holds list over foreign keys, to check hierarchy
        fkeys_list = []
        if fields:
            for fkey in sorted(fkeys, key=lambda x: x['length']):
                fkeys_list.append(fkey.constrained_columns)

                if fkey.constrained_columns[:-1] in fkeys_list:
                    continue

                for idx, col in enumerate(fkey.constrained_columns):
                    if col != self.name and fields[col].value:
                        colname = fkey.referred_columns[idx]
                        cond = f"{colname} = :{colname}"
                        conditions.append(cond)
                        params[colname] = fields[col].value

        condition = " AND ".join(conditions) if len(conditions) else ''

        return condition, params

    def get_options(self, condition, params):

        fkey = self.tbl.get_fkey(self.name)
        pkey_col = fkey.referred_columns[-1] if fkey else self.name
        from_table = fkey.referred_table if fkey else self.tbl.name

        # Field that holds the value of the options
        value_field = f'"{self.name}".' + pkey_col

        condition = condition or '1=1'

        # Count records

        sql = f"""
        select count(*)
        from {self.db.schema}."{from_table}" "{self.name}"
        where {condition}
        """

        count = self.db.query(sql, params).first()[0]

        if (count > 200):
            return False

        self.view = self.get_view(fkey) if fkey else self.name

        sql = f"""
        select distinct {value_field} as value, {self.view or value_field} as label
        from   {self.db.schema}."{from_table}" "{self.name}"
        where  {condition}
        order by {self.view or value_field}
        """

        options = self.db.query(sql, params).all()

        # Return list of recular python dicts so that it can be
        # json serialized and put in cache
        return [dict(row._mapping) for row in options]

    def get_view(self, fkey):
        """ Decide what should be shown in options """
        if hasattr(self, 'view'):
            return self.view
        from table import Table

        ref_tbl = Table(self.db, fkey.referred_table)
        self.view = self.name + '.' + ref_tbl.pkey.columns[-1]

        if fkey.referred_table in self.db.user_tables:

            if ref_tbl.is_hidden() is False:
                self.expandable = True

            for index in ref_tbl.indexes.values():
                if index.columns != ref_tbl.pkey.columns and index.unique:
                    # Only last pk column is used in display value,
                    # other pk columns are usually foreign keys
                    cols = [f'"{self.name}".{col}' for col in index.columns
                            if col not in ref_tbl.pkey.columns[0:-1]]
                    self.view = " || ', ' || ".join(cols)
                    if index.name.endswith("_sort_idx"):
                        break

        return self.view
