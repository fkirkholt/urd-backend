from addict import Dict
from expression import Expression
from util import prepare, to_rec


class Reflection:

    def __init__(self, engine, catalog):
        self.engine = engine
        self.expr = Expression(engine.name)
        self.cat = catalog
        self._fkeys = None

    def get_schema_names(self):
        """Get all schemata in database"""
        if hasattr(self, '_schema_names'):
            return self._schema_names
        self._schema_names = []

        if self.engine.name == 'sqlite':
            sql = "PRAGMA database_list"
            with self.engine.connect() as cnxn:
                sql, _ = prepare(sql)
                rows = cnxn.execute(sql).fetchall()
                for row in rows:
                    self._schema_names.append(row[1])

        if self.engine.name == 'postgres':
            # sql = self.expr.schemata()
            with self.engine.connect() as cnxn:
                rows = cnxn.execute(sql).fetchall()
                for row in rows:
                    self._schema_names.append(row.schema_name)

        return self._schema_names

    def tables(self, schema, table=None):
        if hasattr(self, '_tbl_names'):
            return self._tbl_names

        self._tables = Dict()
        with self.engine.connect() as cnxn:
            sql = self.expr.user_tables()
            sql, params = prepare(sql, {'schema': schema, 'table': table})
            rows = cnxn.execute(sql, params).fetchall()

        for row in rows:
            self._tables[row.table_name].name = row.table_name
            self._tables[row.table_name].type = row.table_type
            self._tables[row.table_name].comment = row.remarks

        return self._tables

    def get_view_names(self, schema):
        if hasattr(self, '_view_names'):
            return self._view_names
        self._view_names = []
        with self.engine.connect() as cnxn:
            sql = self.expr.view_names()
            if sql:
                sql, _ = prepare(sql)
                rows = cnxn.execute(sql).fetchall()
            elif self.engine.name in ('mysql', 'mariadb'):
                cursor = cnxn.cursor()
                rows = cursor.tables(catalog=schema).fetchall()
            else:
                cursor = cnxn.cursor()
                rows = cursor.tables(catalog=self.cat, schema=schema).fetchall()
            self._view_names = [row.table_name for row in rows
                                if row.table_type.lower() == 'view']

        return self._view_names

    def pkeys(self, schema, table=None):
        with self.engine.connect() as cnxn:
            sql = self.expr.pkeys()
            sql, params = prepare(sql, {'schema': schema, 'table': table})
            rows = cnxn.execute(sql, params).fetchall()

        pkeys = Dict()
        for row in rows:
            pkeys[row.table_name].table_name = row.table_name
            pkeys[row.table_name].pkey_name = row.pk_name
            if 'constrained_columns' not in pkeys[row.table_name]:
                pkeys[row.table_name].constrained_columns = []
            pkeys[row.table_name].constrained_columns.append(row.column_name)

        return pkeys[table] if table else pkeys

    def columns(self, schema, table=None):
        """ Return all columns in schema by reflection """
        if hasattr(self, '_columns'):
            return self._columns
        with self.engine.connect() as cnxn:
            if self.expr.columns():
                sql = self.expr.columns()
                sql, params = prepare(sql, {'schema': schema, 'table': table})
                rows = cnxn.execute(sql, params).fetchall()
            else:
                crsr = cnxn.cursor()
                rows = crsr.columns(catalog=self.cat, schema=schema,
                                    table=table)

        self._columns = Dict()
        for row in rows:
            col = Dict()
            col.table_name = row.table_name
            col.schema_name = schema
            col.name = row.column_name
            col.type = row.type_name
            col.nullable = row.nullable
            col.default = row.column_def
            col.size = int(row.column_size) if row.column_size else None
            col.precision = col.size
            col.scale = int(row.decimal_digits) if 'decimal_digits' in row else None
            if row.table_name not in self._columns:
                self._columns[row.table_name] = []
            self._columns[row.table_name].append(col)

        return self._columns[table] if table else self._columns


    def fkeys(self, schema, fk_table=None, pk_table=None):
        if self._fkeys:
            return self._fkeys
        all_fkeys = Dict()
        fkeys = Dict()
        with self.engine.connect() as cnxn:
            params = {
                'schema': schema,
                'table': fk_table,
            }
            sql, params = prepare(self.expr.fkeys(), params)
            for row in cnxn.execute(sql, params):
                name = row.fk_name
                tblname = row.fktable_name
                if 'constrained_columns' not in fkeys[tblname][name]:
                    fkeys[tblname][name].constrained_columns = []
                    fkeys[tblname][name].referred_columns = []
                fkeys[tblname][name].name = row.fk_name
                fkeys[tblname][name].table_name = row.fktable_name
                if 'fktable_schem' in row:
                    fkeys[tblname][name].schema = row.fktable_schem
                else:
                    fkeys[tblname][name].schema = schema
                fkeys[tblname][name].constrained_columns.append(row.fkcolumn_name)
                fkeys[tblname][name].referred_columns.append(row.pkcolumn_name)
                if 'pktable_schem' in row:
                    fkeys[tblname][name].referred_schema = row.pktable_schem
                else:
                    fkeys[tblname][name].referred_schema = schema
                fkeys[tblname][name].referred_table = row.pktable_name
            for tblname in fkeys:
                all_fkeys[tblname] = fkeys[tblname].values() 

        self._fkeys = all_fkeys

        if len(self._fkeys):
            return all_fkeys if (fk_table is None) else all_fkeys[fk_table]

        columns = self.columns(schema)
        if fk_table or pk_table is None:
            table_names = [fk_table] if fk_table else self.tables(schema).keys()
            for fk_tbl_name in table_names:
                for fk_col in columns[fk_tbl_name]:
                    for pk_tbl_name in table_names:
                        if (pk_tbl_name.rstrip('_') + '_') not in fk_col.name:
                            continue
                        for pk_col in columns[pk_tbl_name]:
                            if fk_col.name == pk_col.name and fk_tbl_name == pk_tbl_name:
                                continue
                            fkey = self.fkey_from_colname(fk_col, pk_col, fkeys[fk_tbl_name])
                            if fkey:
                                fkeys[fk_tbl_name][fkey.name] = fkey
        else:
            for pk_col in columns[pk_table]:
                for fk_tbl_name in columns:
                    for fk_col in columns[fk_tbl_name]:
                        if fk_col.name == pk_col.name and fk_tbl_name == pk_tbl_name:
                            continue
                        fkey = self.fkey_from_colname(fk_col, pk_col, fkeys[pk_tbl_name])
                        fkeys[pk_tbl_name][fkey.name] = fkey

        for tblname in fkeys:
            all_fkeys[tblname] = fkeys[tblname].values()

        return all_fkeys if (fk_table is None) else all_fkeys[fk_table]

    def fkey_from_colname(self, fk_col, pk_col, fkeys):
        fkey = Dict()
        if fk_col.name == pk_col.name and fk_col.table_name == pk_col.table_name:
            # Hvis dette er samme kolonnen
            return None
        ref = (pk_col.table_name + '_' + pk_col.name).replace('__', '_').rstrip('_')
        if fk_col.name.endswith(ref) or fk_col.name == pk_col.name:
            prefix = fk_col.name.replace(ref, '').rstrip('_')
            prefix = '_' + prefix if prefix else ''
            # Genererer navn til fremmednøkkelen
            fk_name = fk_col.table_name + '_' + pk_col.table_name + prefix + '_fkey'
            
            if fk_name not in fkeys:
                fkey.constrained_columns = []
                fkey.referred_columns = []
                fkey.name = fk_name
                fkey.table_name = fk_col.table_name
                fkey.schema = fk_col.schema_name
            else:
                fkey = fkeys[fk_name]
            fkey.constrained_columns.append(fk_col.name)
            fkey.referred_columns.append(pk_col.name)
            fkey.referred_schema = pk_col.schema_name
            fkey.referred_table = pk_col.table_name

            return fkey
        else:
            return None


    def indexes(self, schema, table=None):
        with self.engine.connect() as cnxn:
            sql = self.expr.indexes()
            sql, params = prepare(sql, {'schema': schema, 'table': table})
            rows = cnxn.execute(sql, params).fetchall()

        indexes = Dict()
        for row in rows:
            rec = to_rec(row)
            name = rec.index_name
            indexes[row.table_name][name].name = name
            indexes[row.table_name][name].unique = not rec.non_unique
            if 'column_names' in rec:
                indexes[rec.table_name][name].column_names = rec.column_names
            if 'column_names' not in indexes[rec.table_name][name]:
                indexes[rec.table_name][name].column_names = []
            if 'column_name' in rec:
                indexes[rec.table_name][name].column_names.append(rec.column_name)

        all_indexes = Dict()
        for tbl_name in indexes:
            all_indexes[tbl_name] = []
            for key, idx in indexes[tbl_name].items():
                all_indexes[tbl_name].append(idx)

        return all_indexes if not table else all_indexes[table] 

    def get_view_definition(self, tbl_name, schema):
        sql = self.expr.view_definition()
        view_def = None
        with self.engine.connect() as cnxn:
            crsr = cnxn.cursor()
            if self.engine.name in ['oracle', 'sqlite']:
                view_def = crsr.execute(sql, tbl_name).fetchone()[0]
            elif sql:
                params = [schema, tbl_name]
                view_def = crsr.execute(sql, params).fetchone()[0]

        if self.engine.name == 'oracle':
            view_def = 'create view ' + tbl_name + ' as\n' + view_def

        return view_def
