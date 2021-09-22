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
            'description': None #TODO
        })

        if 'column_size' in col:
            field.size = int(col.column_size)
        if 'scale' in col and col.scale:
            field.scale = int(col.scale)
        if col.get('auto_increment', None):
            field.extra = "auto_increment"
        if element == "select" and len(options):
            field.options = options
        elif self.name in foreign_keys:
            fk = foreign_keys[self.name]
            field.foreign_key = fk
            ref_tbl = Table(self.db, fk.table)
            ref_pk = ref_tbl.get_primary_key()

            if (ref_tbl.get_type() == "data"):
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

        return field

    def get_options(self, field, fields=None):
        from database import Database
        from table import Table, Grid

        fk = self.tbl.get_fkey(field.name)
        base = Database(self.db.cnxn, fk.base or fk.schema)
        cand_tbl = Table(base, fk.table)
        grid = Grid(cand_tbl)

        # List of fields
        kodefelter = [field.name + '.' + name for name in fk.primary]

        # Field that holds the value of the options
        value_field = kodefelter[-1]

        # Sorting
        cand_sort_columns = grid.get_sort_columns()
        sort_fields = [field.name + '.' + col for col in cand_sort_columns]

        order = "order by " + ', '.join(sort_fields) if len(sort_fields) else ''

        # Conditions
        conditions = []
        if 'filter' in fk:
            conditions.append("("+self.db.expr.replace_vars(fk.filter)+")")

        if fk.schema == 'urd' and 'schema_' in cand_tbl.fields:
            admin_schemas = "'" + "', '".join(self.db.get_user_admin_schemas()) + "'"
            conditions.append(f"schema_ in ({admin_schemas})")

        # Adds condition if this select depends on other selects
        if 'value' in field and len(fk.foreign) > 1:
            for idx, key in enumerate(fk.foreign):
                if key != field.name and fields[key].value:
                    conditions.append(fk.primary[idx] + " = '" + str(fields[key].value) + "'")

        condition = "where " + " AND ".join(conditions) if len(conditions) else ''

        # Count records
        cursor = self.db.cnxn.cursor()

        sql = "select count(*)\n"
        sql+= f"from {self.db.schema or self.db.cat}.{cand_tbl.name} {field.name}\n"
        sql+= condition

        count = cursor.execute(sql).fetchval()

        if (count > 200):
            return False

        sql = "select " + value_field + " as value, "
        sql+= "(" + (field.view or value_field) + ") as label, "
        sql+= "(" + (field.column_view or value_field) + ") as coltext "
        sql+= f"from {self.db.schema or self.db.cat}.{cand_tbl.name} {field.name}\n"
        sql+= condition + "\n" + order

        rows = cursor.execute(sql).fetchall()

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
