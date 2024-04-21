import os
import hashlib
from field import Field
from addict import Dict
from datetime import datetime
from sqlalchemy import text
from column import Column


class Record:
    def __init__(self, db, tbl, pkey_vals):
        self.db = db
        self.tbl = tbl
        self.pk = self.format_pkey(pkey_vals)
        self.cache = Dict()

    def format_pkey(self, pkey_vals):
        """Return pkey values where floats are strings. Needed by pyodbc"""
        formatted_pkey = {}
        for key, value in pkey_vals.items():
            if type(value) is float:
                value = str(value)
            formatted_pkey[key] = value

        return formatted_pkey

    def get(self):
        values = self.get_values()
        displays = self.get_display_values()

        new = True if not values else False
        if new:
            values = self.pk

        fields = Dict()

        for field in self.tbl.fields.values():
            fld = Field(self.tbl, field.name)
            field.value = values.get(field.name, None)
            field.text = None if not displays else displays.get(field.name, None)
            if field.name == 'password':
                field.value = '****'
                field.text = '****'
            if 'editable' not in field:
                field.editable = True

            if (
                'fkey' in field and
                field.fkey.referred_table in self.db.tablenames
            ):
                condition, params = fld.get_condition(fields=fields)
                field.options = fld.get_options(condition, params)

            fields[field.name] = field

        return Dict({
            'base_name': self.db.identifier,
            'table_name': self.tbl.name,
            'pkey': self.pk,
            'fields': fields,
            'new': new,
            'loaded': True
        })

    def get_relation_count(self):
        from database import Database
        from table import Table, Grid

        values = None if len(self.pk) == 0 else self.get_values()

        relations = {}
        for key, rel in self.tbl.relations.items():
            if self.db.engine.name == 'postgresql':
                base_name = rel.schema
            else:
                base_name = rel.base or rel.schema

            if rel.schema == self.db.schema:
                db = self.db
            else:
                db = Database(self.db.engine, base_name, self.db.user.name)

            tbl_rel = Table(db, rel.table)
            columns = db.refl.get_columns(rel.table, db.schema)
            tbl_rel.cols = {col['name']: Dict(col) for col in columns}

            if rel.table not in self.db.tablenames:
                continue

            # Find index used
            rel.index = self.get_relation_idx(tbl_rel, rel)
            if not rel.index:
                continue

            grid = Grid(tbl_rel)
            grid2 = Grid(tbl_rel)  # Used to count inherited records

            # todo: filtrate on highest level

            # Add condition to fetch only rows that link to record
            conds = Dict()
            count_null_conds = 0
            show_if = None

            for i, colname in enumerate(rel.constrained_columns):
                val = None if len(self.pk) == 0 else values[rel.referred_columns[i]]
                col = Column(self.tbl, tbl_rel.cols[colname])

                mark = tbl_rel.view + '_' + colname
                expr = f'{tbl_rel.view}.{colname} = :{mark}'
                if (
                    col.nullable and
                    colname != rel.constrained_columns[0] and
                    rel.referred_columns == list(self.pk.keys()) and
                    rel.index.unique is True
                ):
                    expr = f'{tbl_rel.view}.{colname} IS NULL'
                    grid2.cond.prep_stmnts.append(expr)
                    count_null_conds += 1
                else:
                    grid2.cond.prep_stmnts.append(expr)
                    grid2.cond.params[mark] = val

                grid.cond.prep_stmnts.append(expr)
                grid.cond.params[mark] = val
                conds[colname] = val

                if (colname[0] == '_' or colname[0:6] == 'const_') and col.default:
                    show_if = {rel.referred_columns[i]: col.default}

            count_records = grid.get_rowcount() if len(self.pk) else 0

            count_inherited = 0
            if count_null_conds:
                count_inherited = grid2.get_rowcount()

            relation = Dict({
                'count_records': count_records + count_inherited,
                'count_inherited': count_inherited,
                'name': rel.table,
                'conditions': grid.get_client_conditions(),
                'conds': conds,
                'base_name': rel.base,
                'schema_name': rel.schema,
                'relationship': rel.relationship,
                'delete_rule': rel.delete_rule
            })
            if relation.relationship == '1:1' and relation.count_records:
                rec = Record(self.db, tbl_rel, conds)
                relation.records = [rec.get()]

            relation.show_if = show_if

            relations[key] = relation

        return relations

    def get_relation_idx(self, tbl_rel, rel):
        rel_idx = None
        slice_obj = slice(0, len(rel.constrained_columns))
        for index in tbl_rel.indexes.values():
            if index.columns[slice_obj] == rel.constrained_columns:
                rel_idx = index
                if index.unique:
                    break

        return rel_idx

    def get_relation(self, alias: str):
        from database import Database
        from table import Table, Grid
        rel = self.tbl.get_relation(alias)
        if self.db.engine.name == 'postgresql':
            base_name = rel.base + '.' + rel.schema
        else:
            base_name = rel.base or rel.schema
        db = Database(self.db.engine, base_name, self.db.user.name)
        tbl_rel = Table(db, rel.table)
        grid = Grid(tbl_rel)
        tbl_rel.limit = 500  # TODO: should have pagination in stead
        tbl_rel.offset = 0

        # Find index used
        rel.index = self.get_relation_idx(tbl_rel, rel)

        # todo: filter

        # Add condition to fetch only rows that link to record
        conds = Dict()

        if not self.pk:
            values = {col: None for col in rel.referred_columns}
        else:
            values = self.get_values()
        for idx, col in enumerate(rel.constrained_columns):
            val = None if len(self.pk) == 0 else values[rel.referred_columns[idx]]
            mark = tbl_rel.view + '_' + col
            grid.cond.params[mark] = val
            if (
                len(self.pk) and tbl_rel.fields[col].nullable and
                col != rel.constrained_columns[0] and
                rel.referred_columns == list(self.pk.keys()) and
                rel.index.unique is True
            ):
                expr = (f'({tbl_rel.view}.{col} = :{mark} or '
                        f'{tbl_rel.view}.{col} is null)')
                grid.cond.prep_stmnts.append(expr)
            else:
                expr = f'{tbl_rel.view}.{col} = :{mark}'
                grid.cond.prep_stmnts.append(expr)
            conds[col] = val

        relation = grid.get()
        relation.conds = conds
        relation.relationship = rel.relationship

        for idx, col in enumerate(rel.constrained_columns):
            relation.fields[col].default = values[rel.referred_columns[idx]]
            relation.fields[col].defines_relation = True

        return relation

    def get_value(self, colname):
        if self.cache.get('vals', None):
            return self.cache.vals[colname]
        values = self.get_values()
        return values[colname]

    def get_values(self):
        if self.cache.get('vals', None):
            return self.cache.vals
        conds = [f"{key} = :{key}" for key in self.pk]
        cond = " and ".join(conds)
        params = {key: val for key, val in self.pk.items()}

        sql = f"""
        select * from {self.db.schema}.{self.tbl.view}\n
        where {cond}
        """

        with self.db.engine.connect() as cnxn:
            row = cnxn.execute(text(sql), params).mappings().fetchone()
        self.cache.vals = row

        return self.cache.vals

    def get_display_values(self):
        displays = {}

        for key, field in self.tbl.fields.items():
            if 'view' in field:
                displays[key] = f"({field.view}) as {key}"

        if len(displays) == 0:
            return Dict()

        select = ', '.join(displays.values())

        conds = [f"{self.tbl.view}.{key} = :{key}" for key in self.pk]
        cond = " and ".join(conds)

        sql = "select " + select + "\n"
        sql += f"from {self.db.schema}.{self.tbl.view}\n"
        sql += '\n'.join(self.tbl.joins) + "\n"
        sql += " where " + cond

        with self.db.engine.connect() as cnxn:
            row = cnxn.execute(text(sql), self.pk).mappings().fetchone()

        return row

    def get_children(self):
        from table import Grid
        grid = Grid(self.tbl)
        grid.user_filtered = True
        rec = self.get()

        rel = [rel for rel in self.tbl.relations.values()
               if rel.table == self.tbl.name][0]

        for idx, colname in enumerate(rel.referred_columns):
            foreign = rel.constrained_columns[idx]
            primary = rel.referred_columns[idx]
            value = rec.fields[colname].value
            mark = rel.table + '_' + foreign
            expr = f'"{rel.table}"."{foreign}" = :{mark}'
            grid.cond.prep_stmnts.append(expr)
            grid.cond.params[mark] = value
            expr = f'"{rel.table}"."{foreign}" != "{rel.table}"."{primary}"'
            grid.cond.prep_stmnts.append(expr)

        relation = grid.get()

        return relation['records']

    def get_file_path(self):
        filepath_idx_name = self.tbl.name + '_filepath_idx'
        filepath_idx = self.tbl.indexes.get(filepath_idx_name, None)
        select = " || '/' || ".join(filepath_idx.columns)
        conds = [f"{key} = :{key}" for key in self.pk]
        cond = " and ".join(conds)
        schema = self.db.schema

        sql = f"""
        select {select} as path from {schema}.{self.tbl.name}\n
        where {cond}
        """

        with self.engine.connect() as cnxn:
            row = cnxn.execute(text(sql), self.pk).first()

        return os.path.normpath(row.path)

    def insert(self, values):
        # todo: Get values for auto and auto_update fields

        # Get autoinc values for primary keys
        # Supports simple and compound primary keys
        for colname in self.tbl.pkey.colnames:
            if colname in values:
                self.pk[colname] = values[colname]
        inc_col = self.tbl.pkey.columns[-1]
        if (
            inc_col not in values and
            self.tbl.fields[inc_col].extra == "auto_increment"
        ):
            s = slice(0, len(self.tbl.pkey.columns) - 1)
            cols = self.tbl.pkey.columns[s]

            conditions = []
            params = {}
            for col in cols:
                conditions.append(f"{col} = :{col}")
                params[col] = values[col]

            sql = f"select case when max({inc_col}) is null then 1 "
            sql += f"else max({inc_col}) +1 end from {self.tbl.name} "
            sql += "" if not len(cols) else "where " + " and ".join(conditions)

            with self.db.engine.connect() as cnxn:
                values[inc_col] = cnxn.execute(text(sql), params).first()[0]
            self.pk[inc_col] = values[inc_col]

        # Array of values to be inserted
        inserts = {}

        for key, value in values.items():

            if value == "":
                value = None

            if str(value).upper() in ['CURRENT_TIMESTAMP']:
                value = datetime.now()

            if key == 'password':
                value = hashlib.sha256(value.encode('utf-8')).hexdigest()

            inserts[key] = value

        sql = f"""
        insert into "{self.tbl.view}" ({','.join(inserts.keys())})
        values ({', '.join([f":{key}" for key in inserts])})
        """

        with self.db.engine.connect() as conn:
            conn.execute(text(sql), inserts)
            conn.commit()

        return self.pk

    def set_fk_values(self, relations):
        """Set value of fk of relations after autincrement pk"""
        for rel in relations.values():
            for rel_rec in rel.records:
                for idx, colname in enumerate(rel.constrained_columns):
                    if colname not in rel_rec.values:
                        pk_col = rel.referred_columns[idx]
                        rel_rec.values[colname] = self.pk[pk_col]

    def update(self, values):
        set_values = {}
        # todo: get values for auto update fields
        for field in self.tbl.fields.values():
            fld = Field(self.tbl, field.name)
            if field.get('extra', None) == "auto_update":
                set_values[field.name] = fld.replace_vars(field.default)

        for key, value in values.items():
            if value == "":
                value = None

            if key == 'password':
                value = hashlib.sha256(value.encode('utf-8')).hexdigest()

            set_values[key] = value

        sets = [f"{key} = :{key}" for key, val in set_values.items()]
        set_str = ",\n".join(sets)

        wheres = [f"{key} = :pk{i}" for i, key in enumerate(self.pk)]
        where_str = " and ".join(wheres)
        where_vals = {f"pk{i}": val for i, val in enumerate(self.pk.values())}
        params = set_values | where_vals

        sql = f"""
        update {self.tbl.view}\n
        set {set_str}\n
        where {where_str}
        """

        with self.db.engine.connect() as conn:
            result = conn.execute(text(sql), params)
            conn.commit()

        # Update primary key
        for key, value in values.items():
            if key in self.pk:
                self.pk[key] = value

        return result

    def delete(self):
        """ Deletes a record.

        Deletion of subordinate records are handled by the database
        with ON DELETE CASCADE on the foreign key
        """

        wheres = [f"{key} = :{key}" for key in self.pk]
        where_str = " and ".join(wheres)

        sql = f"""
        delete from {self.tbl.view}
        where {where_str}
        """

        with self.db.engine.connect() as conn:
            result = conn.execute(text(sql), self.pk)
            conn.commit()

        return result
