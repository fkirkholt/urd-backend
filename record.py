import os
import hashlib
from field import Field
from addict import Dict
from datetime import datetime
from column import Column
from util import prepare, to_rec


class Record:
    def __init__(self, db, tbl, pkey_vals):
        self._db = db
        self._tbl = tbl
        self.base_name = db.identifier
        self.table_name = tbl.name
        self.pkey = self.format_pkey(pkey_vals)
        self._cache = Dict()

    def format_pkey(self, pkey_vals):
        """Return pkey values where floats are strings. Needed by pyodbc"""
        formatted_pkey = {}
        for key, value in pkey_vals.items():
            if type(value) is float:
                value = str(value)
            formatted_pkey[key] = value

        return formatted_pkey

    def get(self):

        return Dict({
            'base_name': self._db.identifier,
            'table_name': self._tbl.name,
            'pkey': self.pkey,
            'fields': self.fields,
            'new': True if not self.get_values() else False,
            'loaded': True
        })

    @property
    def fields(self):
        if hasattr(self, '_fields'):
            return self._fields

        values = self.get_values() or self.pkey
        displays = self.get_display_values()  # TODO: Skal denne være property?

        self._fields = Dict()

        for field in self._tbl.fields.values():
            fld = Field(self._tbl, field.name)
            field.value = values.get(field.name, None)
            field.text = None if not displays else displays.get(field.name, None)
            if field.name == 'password':
                field.value = '****'
                field.text = '****'
            if 'editable' not in field:
                field.editable = True

            if (
                'fkey' in field and
                field.fkey.referred_table in self._db.tablenames
            ):
                conditions = []
                params = {}
                for idx, col in enumerate(field.fkey.constrained_columns):
                    if col != field.name and self._fields[col].value:
                        colname = field.fkey.referred_columns[idx]
                        cond = f"{colname} = :{colname}"
                        conditions.append(cond)
                        params[colname] = self._fields[col].value

                condition = " AND ".join(conditions) if len(conditions) else ''
                field.options = fld.get_options(condition, params)

            self._fields[field.name] = field

        return self._fields

    def get_relation_count(self):
        from database import Database
        from table import Table, Grid

        # values of primary key columns
        values = None if len(self.pkey) == 0 else self.get_values()

        relations = {}
        for key, rel in self._tbl.relations.items():
            if self._db.engine.name == 'postgresql':
                base_name = rel.schema
            else:
                base_name = rel.base or rel.schema

            if rel.schema == self._db.schema:
                db = self._db
            else:
                db = Database(self._db.engine, base_name, self._db.user.name)

            tbl_rel = Table(db, rel.table_name)
            columns = db.refl.columns(db.schema, rel.table_name)
            tbl_rel.cols = {col['name']: Dict(col) for col in columns}

            if rel.table_name not in self._db.tablenames:
                continue

            # Find index used
            rel.index = self.get_relation_idx(tbl_rel, rel)
            if not rel.index:
                continue

            grid = Grid(tbl_rel)

            # todo: filtrate on highest level

            # Add condition to fetch only rows that link to record
            conds = Dict()
            count_null_conds = 0
            show_if = None

            for i, colname in enumerate(rel.constrained_columns):
                val = None if len(self.pkey) == 0 else values[rel.referred_columns[i]]
                col = Column(self._tbl, tbl_rel.cols[colname])

                mark = tbl_rel.view.rstrip('_') + '_' + colname.lstrip('_')
                grid.cond.params[mark] = val
                if (
                    len(self.pkey) and col.nullable and
                    colname != rel.constrained_columns[0] and
                    rel.referred_columns == list(self.pkey.keys()) and
                    rel.index.unique is True
                ):
                    expr = (f'({tbl_rel.view}.{colname} = :{mark} or '
                            f'{tbl_rel.view}.{colname} is null)')
                else:
                    expr = f'{tbl_rel.view}.{colname} = :{mark}'

                grid.cond.prep_stmnts.append(expr)
                conds[colname] = val

                if (colname[0] == '_' or colname[0:6] == 'const_') and col.default:
                    show_if = {rel.referred_columns[i]: col.default}

            grid.is_relation = True
            count_records = grid.get_rowcount() if len(self.pkey) else 0

            relation = Dict({
                'count_records': count_records,
                'name': rel.table_name,
                'conditions': grid.get_client_conditions(),
                'conds': conds,
                'base_name': rel.base,
                'schema_name': rel.schema,
                'relationship': rel.relationship,
                'delete_rule': rel.delete_rule
            })

            # Add record for 1:1 relations to make it possible to
            # mark the record to be deleted in frontend without expanding
            # it to get the record from backend
            if relation.relationship == '1:1' and relation.count_records:
                rec = Record(self._db, tbl_rel, conds)
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
        rel = self._tbl.get_relation(alias)
        if self._db.engine.name == 'postgresql':
            base_name = rel.base + '.' + rel.schema
        else:
            base_name = rel.base or rel.schema
        db = Database(self._db.engine, base_name, self._db.user.name)
        tbl_rel = Table(db, rel.table_name)
        grid = Grid(tbl_rel)
        tbl_rel.limit = 500  # TODO: should have pagination in stead
        tbl_rel.offset = 0

        # Find index used
        rel.index = self.get_relation_idx(tbl_rel, rel)

        # todo: filter

        # Add condition to fetch only rows that link to record
        conds = Dict()

        if not self.pkey:
            values = {col: None for col in rel.referred_columns}
        else:
            values = self.get_values()
        for idx, col in enumerate(rel.constrained_columns):
            val = None if len(self.pkey) == 0 else values[rel.referred_columns[idx]]
            mark = tbl_rel.view.rstrip('_') + '_' + col.lstrip('_')
            grid.cond.params[mark] = val
            if (
                len(self.pkey) and tbl_rel.fields[col].nullable and
                col != rel.constrained_columns[0] and
                rel.referred_columns == list(self.pkey.keys()) and
                rel.index.unique is True
            ):
                expr = (f'({tbl_rel.view}.{col} = :{mark} or '
                        f'{tbl_rel.view}.{col} is null)')
                grid.cond.prep_stmnts.append(expr)
            else:
                expr = f'{tbl_rel.view}.{col} = :{mark}'
                grid.cond.prep_stmnts.append(expr)
            conds[col] = val

        grid.is_relation = True
        relation = grid.get()
        relation.conds = conds
        relation.relationship = rel.relationship

        for idx, col in enumerate(rel.constrained_columns):
            relation.fields[col].default = values[rel.referred_columns[idx]]
            relation.fields[col].defines_relation = True

        return relation

    def get_value(self, colname):
        if self._cache.get('vals', None):
            return self._cache.vals[colname]
        values = self.get_values()
        return values[colname]

    def get_values(self):
        if self._cache.get('vals', None):
            return self._cache.vals
        conds = [f"{key} = :{key}" for key in self.pkey if self.pkey[key] is not None]
        conds = conds + [f"{key} is null" for key in self.pkey if self.pkey[key] is None]
        cond = " and ".join(conds)
        params = {key: val for key, val in self.pkey.items() if self.pkey[key] is not None}

        selects = []
        for key, field in self._tbl.fields.items():
            if field.datatype == 'bytes' and self._db.engine.name == 'mssql':
                selects.append(f"cast(datalength({field.name}) as varchar) + ' bytes' as {field.name}")
                continue
            elif field.datatype == 'bytes':
                selects.append(f"length({field.name}) || ' bytes' as {field.name}")
                continue
            elif field.datatype == 'geometry':
                selects.append(f"{field.name}.ToString() as {field.name}")
                continue
            selects.append(field.name)

        select = ', '.join(selects)

        sql = f"""
        select {select} from {self._db.schema}.{self._tbl.view}\n
        where {cond}
        """

        with self._db.engine.connect() as cnxn:
            sql, params = prepare(sql, params)
            row = cnxn.execute(sql, params).fetchone()
        self._cache.vals = to_rec(row)

        return self._cache.vals

    def get_display_values(self):
        displays = {}

        for key, field in self._tbl.fields.items():
            if 'view' in field:
                displays[key] = f"({field.view}) as {key}"

        if len(displays) == 0:
            return Dict()

        select = ', '.join(displays.values())

        conds = [f"{self._tbl.view}.{key} = :{key}" for key in self.pkey]
        cond = " and ".join(conds)

        sql = "select " + select + "\n"
        sql += f"from {self._db.schema}.{self._tbl.view}\n"
        sql += '\n'.join(self._tbl.joins.values()) + "\n"
        sql += " where " + cond

        with self._db.engine.connect() as cnxn:
            sql, params = prepare(sql, self.pkey)
            row = cnxn.execute(sql, params).fetchone()

        return to_rec(row)

    def get_children(self):
        from table import Grid
        grid = Grid(self._tbl)

        rel = [rel for rel in self._tbl.relations.values()
               if rel.table_name == self._tbl.name][0]

        for idx, colname in enumerate(rel.referred_columns):
            foreign = rel.constrained_columns[idx]
            primary = rel.referred_columns[idx]
            value = self.fields[colname].value
            mark = rel.table_name.rstrip('_') + '_' + foreign.lstrip('_')
            expr = f'"{rel.table_name}"."{foreign}" = :{mark}'
            grid.cond.prep_stmnts.append(expr)
            grid.cond.params[mark] = value

        grid.is_relation = True
        relation = grid.get()

        return relation['records']

    def get_file_path(self, colname):
        if colname:
            filepath_idx_name = self._tbl.name + '_' + colname + '_filepath_idx'
        else:
            filepath_idx_name = self._tbl.name + '_filepath_idx'
        filepath_idx = self._tbl.indexes.get(filepath_idx_name, None)
        select = " || '/' || ".join(filepath_idx.columns)
        conds = [f"{key} = :{key}" for key in self.pkey]
        cond = " and ".join(conds)
        schema = self._db.schema

        sql = f"""
        select {select} as path from {schema}.{self._tbl.name}\n
        where {cond}
        """

        with self._db.engine.connect() as cnxn:
            sql, params = prepare(sql, self.pkey)
            row = cnxn.execute(sql, params).fetchone()

        return os.path.normpath(row.path)

    def insert(self, values):
        # todo: Get values for auto and auto_update fields

        # Get autoinc values for primary keys
        # Supports simple and compound primary keys
        for colname in self._tbl.pkey.columns:
            if colname in values:
                self.pkey[colname] = values[colname]
        inc_col = self._tbl.pkey.columns[-1]
        if (
            inc_col not in values and
            self._tbl.fields[inc_col].extra == "auto_increment"
        ):
            s = slice(0, len(self._tbl.pkey.columns) - 1)
            cols = self._tbl.pkey.columns[s]

            conditions = []
            params = {}
            for col in cols:
                conditions.append(f"{col} = :{col}")
                params[col] = values[col]

            sql = f"select case when max({inc_col}) is null then 1 "
            sql += f"else max({inc_col}) +1 end from {self._tbl.name} "
            sql += "" if not len(cols) else "where " + " and ".join(conditions)

            with self._db.engine.connect() as cnxn:
                sql, params = prepare(sql, params)
                values[inc_col] = cnxn.execute(sql, params).fetchone()[0]
            self.pkey[inc_col] = values[inc_col]

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
        insert into {self._tbl.view} ({','.join(inserts.keys())})
        values ({', '.join([f":{key}" for key in inserts])})
        """

        with self._db.engine.connect() as cnxn:
            sql, inserts = prepare(sql, inserts)
            cnxn.execute(sql, inserts)
            cnxn.commit()

        return self.pkey

    def set_fk_values(self, relations):
        """Set value of fk of relations after autincrement pk"""
        for rel in relations.values():
            for rel_rec in rel.records:
                for idx, colname in enumerate(rel.constrained_columns):
                    if colname not in rel_rec.values:
                        pk_col = rel.referred_columns[idx]
                        rel_rec.values[colname] = self.pkey[pk_col]

    def update(self, values):
        set_values = {}
        # todo: get values for auto update fields
        for field in self._tbl.fields.values():
            fld = Field(self._tbl, field.name)
            if field.get('extra', None) == "auto_update" and field.default:
                set_values[field.name] = fld.replace_vars(field.default)

        for key, value in values.items():
            if value == "":
                value = None

            if key == 'password':
                value = hashlib.sha256(value.encode('utf-8')).hexdigest()

            set_values[key] = value

        sets = [f"{key} = :{key}" for key, val in set_values.items()]
        set_str = ",\n".join(sets)

        wheres = [f"{key} = :pk{i}" for i, key in enumerate(self.pkey)
                  if self.pkey[key] is not None]
        wheres = wheres + [f"{key} is null" for key in self.pkey if self.pkey[key] is None]
        where_str = " and ".join(wheres)
        where_vals = {f"pk{i}": val for i, val in enumerate(self.pkey.values())
                      if val is not None}
        params = set_values | where_vals

        sql = f"""
        update {self._tbl.view}\n
        set {set_str}\n
        where {where_str}
        """

        with self._db.engine.connect() as cnxn:
            sql, params = prepare(sql, params)
            cnxn.execute(sql, params)
            cnxn.commit()

        # Update primary key
        for key, value in values.items():
            if key in self.pkey:
                self.pkey[key] = value

        return 1

    def delete(self):
        """ Deletes a record.

        Deletion of subordinate records are handled by the database
        with ON DELETE CASCADE on the foreign key
        """

        wheres = [f"{key} = :{key}" for key in self.pkey]
        where_str = " and ".join(wheres)

        sql = f"""
        delete from {self._tbl.view}
        where {where_str}
        """

        with self._db.engine.connect() as cnxn:
            sql, params = prepare(sql, self.pkey)
            cnxn.execute(sql, params)
            cnxn.commit()

        return 1
