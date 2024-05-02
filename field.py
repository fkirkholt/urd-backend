from datetime import date, datetime
from addict import Dict
from sqlalchemy import text


class Field:

    def __init__(self, tbl, name):
        self.tbl = tbl
        self.db = tbl.db
        self.name = name

    def get(self):
        field = {key: val for key, val in vars(self).items()
                 if key not in ['db', 'tbl']}

        return Dict(field)

    def set_attrs_from_col(self, col):
        try:
            self.datatype = col.type.python_type.__name__
        except Exception:
            self.datatype = ('int' if str(col.type).startswith('YEAR')
                             else 'unknown')

        if hasattr(col, 'size'):
            self.size = col.size
        if hasattr(col, 'scale'):
            self.scale = col.scale
            self.precision = col.precision

        if self.datatype == 'int' and getattr(self, 'size', 0) == 1:
            self.datatype = 'bool'

        self.element, type_ = self.get_element()

        attrs = Dict()
        if type_:
            attrs['type'] = type_

        html_attrs = self.get_attributes(self.tbl.name, self.name)
        if 'data-type' in html_attrs:
            attrs['data-type'] = html_attrs['data-type']
            self.datatype = html_attrs['data-type']
        if 'data-format' in html_attrs:
            attrs['data-format'] = html_attrs['data-format']
        if 'data-href' in html_attrs:
            attrs['data-href'] = html_attrs['data-href']

        self.nullable = (col.nullable == 1)
        self.label = self.db.get_label(self.name)
        self.attrs = attrs

        smallints = ['TINYINT', 'SMALLINT', 'MEDIUMINT']

        fkey = self.tbl.get_fkey(self.name)
        if fkey:
            self.fkey = fkey
            self.element = 'select'
            self.view = self.get_view(fkey) or self.name
            self.expandable = getattr(self, 'expandable', False)
            ref_col = fkey.referred_columns[-1].strip('_')
            if col.name in [fkey.referred_table + '_' + ref_col,
                            fkey.referred_columns[-1]]:
                self.label = self.db.get_label(fkey.referred_table)
        if (
            (hasattr(col, 'autoincrement') and col.autoincrement) or (
                (self.datatype == 'int' and str(col.type) not in smallints) and
                len(self.tbl.pkey.columns) and
                col.name == self.tbl.pkey.columns[-1] and
                col.name not in self.tbl.fkeys
            )
        ):
            self.extra = "auto_increment"

        if col.default and not hasattr(col, 'autoincrement'):
            def_vals = col.default.split('::')
            default = def_vals[0]
            self.default = self.replace_vars(default)
            self.default = self.default.replace("'", "")
        else:
            self.default = col.default

    def get_element(self):
        """ Get html element for input field """

        type_ = None
        # Decides what sort of input should be used
        if self.datatype == 'date':
            element = 'input'
            type_ = 'date'
        elif self.datatype == 'bool':
            element = 'input'
            type_ = 'checkbox'
        elif self.datatype == 'bytes' or (self.datatype == 'str' and (
                self.size is None or self.size >= 255)):
            element = "textarea"
        else:
            element = "input"
            type_ = 'text'

        return element, type_

    def get_attributes(self, table_name, colname):
        """Get description based on term"""
        attrs = self.db.html_attrs
        selector_1 = f'[data-field="{table_name}.{colname}"]'
        selector_2 = f'label[data-field="{table_name}.{colname}"]'
        attributes = {}
        if selector_1 in attrs:
            attributes = attrs[selector_1]
        elif selector_2 in attrs:
            attributes = attrs[selector_2]

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

        if fkey and fkey.referred_table in self.db.tablenames:
            from_table = fkey.referred_table
            pkey_col = fkey.referred_columns[-1]
        else:
            from_table = self.tbl.name
            pkey_col = self.name

        # Field that holds the value of the options
        value_field = f'{self.name}.' + pkey_col

        condition = condition or '1=1'

        # Count records

        sql = f"""
        select count(*)
        from {self.db.schema}.{from_table} {self.name}
        where {condition}
        """

        with self.db.engine.connect() as cnxn:
            count = cnxn.execute(text(sql), params).first()[0]

        if (count > 200):
            return False

        view = None if not fkey else self.get_view(fkey)
        self.view = view if view else self.name

        sql = f"""
        select distinct {value_field} as value,
               {self.view or value_field} as label
        from   {self.db.schema}.{from_table} {self.name}
        where  {condition}
        order by {self.view or value_field}
        """

        with self.db.engine.connect() as cnxn:
            options = cnxn.execute(text(sql), params).all()

        # Return list of recular python dicts so that it can be
        # json serialized and put in cache
        return [dict(row._mapping) for row in options]

    def get_view(self, fkey):
        """ Decide what should be shown in options """
        if hasattr(self, 'view'):
            return self.view
        from table import Table

        self.view = None

        if fkey.referred_table in self.db.tablenames:

            ref_tbl = Table(self.db, fkey.referred_table)
            self.view = self.name + '.' + ref_tbl.pkey.columns[-1]

            if ref_tbl.is_hidden() is False:
                self.expandable = True

            for index in ref_tbl.indexes.values():
                if index.columns != ref_tbl.pkey.columns and index.unique:
                    # Only last pk column is used in display value,
                    # other pk columns are usually foreign keys
                    cols = [f'{self.name}.{col}' for col in index.columns
                            if col not in ref_tbl.pkey.columns[0:-1]]
                    if self.db.engine.name == 'oracle':
                        self.view = " || ', ' || ".join(cols)
                    else:
                        self.view = "concat_ws(', ', " + ', '.join(cols) + ")" 
                    if index.name.endswith("_sort_idx"):
                        break

        return self.view

    def replace_vars(self, expr):
        if "curdate" in expr.lower():
            expr = date.today().strftime("%Y-%m-%d")
        elif "current_date" in expr.lower():
            expr = date.today().strftime("%Y-%m-%d")
        elif "current_timestamp" in expr.lower():
            expr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        elif "current_user" in expr.lower():
            expr = self.db.user.name

        return expr
