import os
from field import Field
from addict import Dict
from datetime import datetime


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
            if type(value) == float:
                value = str(value)
            formatted_pkey[key] = value

        return formatted_pkey

    def get(self):
        values = self.get_values()
        displays = self.get_display_values()

        new = True if not values else False
        if new:
            values = self.pk

        fields = {}

        fields = self.tbl.get_fields()

        for field in fields.values():
            fld = Field(self.tbl, field.name)
            field.value = values.get(field.name, None)
            field.text = displays.get(field.name, None)
            if 'editable' not in field:
                field.editable = True

            if 'fkey' in field and field.fkey.table in self.db.user_tables:
                condition, params = fld.get_condition(fields=fields)
                field.options = fld.get_options(condition, params)

            fields[field.name] = field

        return Dict({
            'base_name': self.db.name,
            'table_name': self.tbl.name,
            'pkey': self.pk,
            'fields': fields,
            'new': new,
            'loaded': True
        })

    def get_relation_count(self):
        from database import Database
        from table import Table, Grid

        indexes = self.tbl.get_indexes()
        class_idx = indexes.get(self.tbl.name + "_classification_idx", None)
        class_field = Dict({'options': []})
        if class_idx:
            class_field_name = class_idx.columns[0]
            fields = self.tbl.get_fields()
            class_field = fields[class_field_name]

        relations = {}
        for key, rel in self.tbl.get_relations().items():
            if self.db.cnxn.system == 'postgres':
                base_name = rel.base + '.' + rel.schema
            else:
                base_name = rel.base or rel.schema
            if rel.base == self.db.cat and rel.schema == self.db.schema:
                db = self.db
            else:
                db = Database(self.db.cnxn, base_name)
            tbl_rel = Table(db, rel.table)
            if rel.table not in self.db.user_tables:
                continue

            # Find index used
            rel.index = self.get_relation_idx(tbl_rel, rel)
            if not rel.index:
                continue

            tbl_rel.fields = tbl_rel.get_fields()
            grid = Grid(tbl_rel)
            grid2 = Grid(tbl_rel)  # Used to count inherited records

            # todo: filtrate on highest level

            # Don't get values for new records that's not saved
            if hasattr(self, 'pk') and len(set(self.pk)):
                rec_values = self.get_values() or self.pk

            # Add condition to fetch only rows that link to record
            conds = Dict()
            count_null_conds = 0

            for idx, col in enumerate(rel.foreign):
                ref_key = rel.primary[idx]
                val = None if len(self.pk) == 0 else rec_values[ref_key]
                if (
                    tbl_rel.fields[col].nullable and
                    col != rel.foreign[0] and
                    rel.primary == list(self.pk.keys()) and
                    rel.index.unique is True
                ):
                    grid2.add_cond(expr=f'"{tbl_rel.view}"."{col}"',
                                   operator="IS NULL")
                    count_null_conds += 1
                else:
                    grid2.add_cond(f'"{tbl_rel.view}"."{col}"', "=", val)

                grid.add_cond(f'"{tbl_rel.view}"."{col}"', "=", val)
                conds[col] = val

            count_records = grid.get_rowcount() if len(self.pk) else 0

            count_inherited = 0
            if count_null_conds:
                count_inherited = grid2.get_rowcount()

            tbl_rel.pkey = tbl_rel.get_pkey()
            if set(tbl_rel.pkey.columns) <= set(rel.foreign):
                # if pkey is same as, or a subset of, fkey
                relationship = "1:1"
            else:
                relationship = "1:M"

            relation = Dict({
                'count_records': count_records + count_inherited,
                'count_inherited': count_inherited,
                'name': rel.table,
                'conditions': grid.get_client_conditions(),
                'conds': conds,
                'base_name': rel.base,
                'schema_name': rel.schema,
                'relationship': relationship,
                'delete_rule': rel.delete_rule
            })

            # Tables with suffixes that's part of types
            # should just be shown when the specific type is chosen
            parts = tbl_rel.name.split("_")
            suff_1 = parts[-1]
            suff_2 = '' if len(parts) == 1 else parts[-2]
            show_if = None
            for class_ in [opt['value'] for opt in class_field.options]:
                if (suff_1.startswith(class_) or suff_2.startswith(class_)):
                    show_if = {class_field_name: class_}

            relation.show_if = show_if

            relations[key] = relation

        return relations

    def get_relation_idx(self, tbl_rel, rel):
        rel_idx = None
        slice_obj = slice(0, len(rel.foreign))
        rel_indexes = tbl_rel.get_indexes()
        for index in rel_indexes.values():
            if index.columns[slice_obj] == rel.foreign:
                rel_idx = index
                if index.unique:
                    break

        return rel_idx

    def get_relation(self, alias: str):
        from database import Database
        from table import Table, Grid
        rel = self.tbl.get_relation(alias)
        if self.db.cnxn.system == 'postgres':
            base_name = rel.base + '.' + rel.schema
        else:
            base_name = rel.base or rel.schema
        db = Database(self.db.cnxn, base_name)
        tbl_rel = Table(db, rel.table)
        grid = Grid(tbl_rel)
        tbl_rel.limit = 500  # TODO: should have pagination in stead
        tbl_rel.offset = 0
        tbl_rel.fields = tbl_rel.get_fields()
        tbl_rel.pkey = tbl_rel.get_pkey()

        # Find index used
        rel.index = self.get_relation_idx(tbl_rel, rel)

        # todo: filter

        # Don't get values for new records that's not saved
        if hasattr(self, 'pk') and len(set(self.pk)):
            rec_values = self.get_values() or self.pk

        # Add condition to fetch only rows that link to record
        conds = Dict()
        pkey_vals = {}
        for idx, col in enumerate(rel.foreign):
            ref_key = rel.primary[idx]
            val = None if len(self.pk) == 0 else rec_values[ref_key]
            if (
                len(self.pk) and tbl_rel.fields[col].nullable and
                col != rel.foreign[0] and
                rel.primary == list(self.pk.keys()) and
                rel.index.unique is True
            ):
                grid.add_cond(expr=f'("{tbl_rel.view}"."{col}" = ? or "'
                              f'{tbl_rel.view}"."{col}" is null)', value=val)
            else:
                grid.add_cond(f'"{tbl_rel.view}"."{col}"', "=", val)
            conds[col] = val
            pkey_vals[col] = val
            # grid.add_cond(f"coalesce({rel.table}.{col}, '-')",
            #               "IN", [val, '-'])

        relation = grid.get()
        relation.conds = conds

        # Don't get values for new records that's not saved
        if hasattr(self, 'pk') and len(set(self.pk)):
            rec_values = self.get_values()

        values = [None if len(self.pk) == 0 else rec_values[key]
                  for key in rel.primary]

        for idx, col in enumerate(rel.foreign):
            relation.fields[col].default = values[idx]
            relation.fields[col].defines_relation = True

        tbl_rel.pkey = tbl_rel.get_pkey()

        # If foreign key columns contains primary key
        if set(tbl_rel.pkey.columns) <= set(rel.foreign):
            rec = Record(self.db, tbl_rel, pkey_vals)
            relation.records = [rec.get()]
            relation.relationship = "1:1"
        else:
            relation.relationship = "1:M"

        return relation

    def get_value(self, colname):
        if self.cache.get('vals', None):
            return self.cache.vals[colname]
        values = self.get_values()
        return values[colname]

    def get_values(self):
        if self.cache.get('vals', None):
            return self.cache.vals
        conds = [f"{key} = ?" for key in self.pk]
        cond = " and ".join(conds)
        params = [val for val in self.pk.values()]

        sql = f"""
        select * from {self.db.schema or self.db.cat}."{self.tbl.view}"\n
        where {cond}
        """
        cursor = self.db.cnxn.cursor()
        row = cursor.execute(sql, params).fetchone()
        colnames = [col[0] for col in cursor.description]

        if not row:
            return Dict()

        self.cache.vals = Dict(zip(colnames, row))
        return self.cache.vals

    def get_display_values(self):
        displays = {}

        join = self.tbl.get_join()

        for key, field in self.tbl.get_fields().items():
            if 'view' in field:
                displays[key] = f"({field.view}) as {key}"

        if len(displays) == 0:
            return Dict()

        select = ', '.join(displays.values())

        conds = [f"{self.tbl.view}.{key} = ?" for key in self.pk]
        cond = " and ".join(conds)
        params = [val for val in self.pk.values()]

        sql = "select " + select + "\n"
        sql += f"from {self.db.schema or self.db.cat}.{self.tbl.view}\n"
        sql += join + "\n"
        sql += " where " + cond

        cursor = self.db.cnxn.cursor()
        row = cursor.execute(sql, params).fetchone()
        colnames = [column[0] for column in cursor.description]

        if not row:
            return Dict()

        return Dict(zip(colnames, row))

    def get_children(self):
        from table import Grid
        grid = Grid(self.tbl)
        grid.user_filtered = True
        rec = self.get()

        relations = self.tbl.get_relations().values()
        rel = [rel for rel in relations if rel.table == self.tbl.name][0]

        for idx, colname in enumerate(rel.primary):
            foreign = rel.foreign[idx]
            value = rec.fields[colname].value
            grid.add_cond(f'"{rel.table}"."{foreign}"', "=", value)

        relation = grid.get()

        return relation['records']

    def get_file_path(self):
        indexes = self.tbl.get_indexes()
        filepath_idx = indexes.get(self.tbl.name + "_filepath_idx", None)
        select = " || '/' || ".join(filepath_idx.columns)
        conds = [f"{key} = ?" for key in self.pk]
        cond = " and ".join(conds)
        schema = self.db.schema or self.db.cat

        sql = f"""
        select {select} as path from {schema}.{self.tbl.name}\n
        where {cond}
        """
        cursor = self.db.cnxn.cursor()
        row = cursor.execute(sql, list(self.pk.values())).fetchone()

        return os.path.normpath(row.path)

    def insert(self, values):
        fields = self.tbl.get_fields()

        # todo: Get values for auto and auto_update fields

        # Get autoinc values for primary keys
        # Supports simple and compound primary keys
        pkey = self.tbl.get_pkey()
        for colname in pkey.colnames:
            if colname in values:
                self.pk[colname] = values[colname]
        inc_col = pkey.columns[-1]
        if (
            inc_col not in values and
            fields[inc_col].extra == "auto_increment"
        ):
            s = slice(0, len(pkey.columns) - 1)
            cols = pkey.columns[s]

            conditions = []
            params = []
            for col in cols:
                conditions.append(f"{col} = ?")
                params.append(values[col])

            sql = f"select case when max({inc_col}) is null then 1 "
            sql += f"else max({inc_col}) +1 end from {self.tbl.name} "
            sql += "" if not len(cols) else "where " + " and ".join(conditions)

            values[inc_col] = self.db.query(sql, params).fetchval()
            self.pk[inc_col] = values[inc_col]

        # Array of values to be inserted
        inserts = {}

        for key, value in values.items():

            if value == "":
                value = None

            if str(value).upper() in ['CURRENT_TIMESTAMP']:
                value = datetime.now()

            inserts[key] = value

        sql = f"""
        insert into "{self.tbl.view}" ({','.join(inserts.keys())})
        values ({', '.join(["?" for key in inserts])})
        """

        self.db.query(sql, list(inserts.values())).commit()

        return self.pk

    def set_fk_values(self, relations):
        """Set value of fk of relations after autincrement pk"""
        for rel in relations.values():
            for rel_rec in rel.records:
                for idx, colname in enumerate(rel.foreign):
                    if colname not in rel_rec.values:
                        pk_col = rel.primary[idx]
                        rel_rec.values[colname] = self.pk[pk_col]

    def update(self, values):
        set_values = {}
        # todo: get values for auto update fields
        for field in self.tbl.get_fields().values():
            if field.get('extra', None) == "auto_update":
                set_values[field.name] = \
                    self.db.expr.replace_vars(field.default, self.db)

        for key, value in values.items():
            if value == "":
                value = None

            set_values[key] = value

        sets = [f"{key} = ?" for key, val in set_values.items()]
        set_str = ",\n".join(sets)
        params = set_values.values()

        wheres = [f"{key} = ?" for key in self.pk]
        where_str = " and ".join(wheres)
        params = list(params) + list(self.pk.values())

        sql = f"""
        update {self.tbl.view}\n
        set {set_str}\n
        where {where_str}
        """

        result = self.db.query(sql, params).commit()

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

        wheres = [f"{key} = ?" for key in self.pk]
        where_str = " and ".join(wheres)

        sql = f"""
        delete from {self.tbl.view}
        where {where_str}
        """

        result = self.db.query(sql, list(self.pk.values())).commit()

        return result
