import json
from addict import Dict

class Column:
    def __init__(self, tbl, name):
        self.db = tbl.db
        self.tbl = tbl
        self.name = name

    def get_field(self, col):
        from table import Table
        type_ = self.db.expr.to_urd_type(col.type_name)
        foreign_keys = self.tbl.get_fkeys()
        pkey = self.tbl.get_primary_key()

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
        elif self.name in foreign_keys:
            element = 'select'
            options = []
        elif type_ == 'binary' or (type_ == 'string' and (col.column_size > 255)):
            element = "textarea"
        else:
            element = "input[type=text]"

        field = Dict({
            'name': self.name,
            'datatype': type_,
            'element': element,
            'nullable': col.nullable == True,
            'label': self.db.get_label(self.name),
            'description': self.db.get_description(self.name)
        })

        for fkey in foreign_keys.values():
            if fkey.foreign[-1] == field.name:
                if (not field.foreign_key or len(fkey.foreign) < len(field.foreign_key.foreign)):
                    field.foreign_key = fkey
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
        elif field.foreign_key:
            ref_tbl = Table(self.db, field.foreign_key.table)
            if field.foreign_key.table in self.db.user_tables:
                ref_pk = ref_tbl.get_primary_key()

                if ref_tbl.is_hidden() is False:
                    field.expandable = True

                for index in ref_tbl.get_indexes().values():
                    if index.columns != ref_pk and index.unique:
                        # Only last pk column is used in display value,
                        # other pk columns are usually foreign keys
                        cols = [self.name+"."+col for col in index.columns if col not in ref_pk[0:-1]]
                        field.view = " || ' - ' || ".join(cols)
                        if index.name.endswith("_sort_idx"):
                            break

                if 'column_view' not in field and 'view' in field:
                    field.column_view = field.view
                field.options = self.get_options(field)
        if (type_ in ['integer', 'decimal'] and len(pkey) and self.name == pkey[-1] and self.name not in foreign_keys):
            field.extra = "auto_increment"

        if col.column_def and not col.auto_increment:
            def_vals = col.column_def.split('::')
            default = def_vals[0]
            default = default.replace("'", "")

            #TODO: Sjekk om jeg trenger å endre current_timestamp()

            field.default = self.db.expr.replace_vars(default)
            if (field.default != default):
                field.default_expr = default

        return field

    def get_options(self, field, fields=None):
        from database import Database
        from table import Table, Grid

        fk = field.foreign_key
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
        # Holder liste over fremmednøkler, for å sjekke hierarki
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
        cursor = self.db.cnxn.cursor()

        sql = "select count(*)\n"
        sql+= f"from {self.db.schema or self.db.cat}.{cand_tbl.name} {field.name}\n"
        sql+= condition

        count = cursor.execute(sql, params).fetchval()

        if (count > 200):
            return False

        sql = "select " + value_field + " as value, "
        sql+= "(" + (field.view or value_field) + ") as label, "
        sql+= "(" + (field.column_view or value_field) + ") as coltext "
        sql+= f"from {self.db.schema or self.db.cat}.{cand_tbl.name} {field.name}\n"
        sql+= condition + "\n" + order

        rows = cursor.execute(sql, params).fetchall()

        result = []
        colnames = [column[0] for column in cursor.description]
        for row in rows:
            result.append(dict(zip(colnames, row)))

        return result

    def get_select(self, req):
        #TODO: Kan jeg ikke hente noe fra backend istenfor å få alt servert fra frontend? Altfor mange parametre!
        search = None if not 'q' in req else req.q.replace("*", "%")

        if 'key' in req:
            key = json.loads(req.key)
            col = key[-1]
        else:
            col = self.get_primary_key()[-1]

        view = req.get('view') or col
        col_view = req.get('column_view') or col

        conds = req.condition.split(" and ") if req.condition else []
        # ignore case
        if search:
            search = search.lower()
            conds.append(f"lower(cast({view} as varchar)) like '%{search}%'")

        cond = " and ".join(conds) if len(conds) else col + " IS NOT NULL"

        val_col = req.alias + "." + col

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
