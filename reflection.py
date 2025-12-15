import yaml
import simplejson as json
from addict import Dict
from expression import Expression
from util import to_rec, log_caller


class Reflection:

    def __init__(self, engine, catalog):
        self.engine = engine
        self.expr = Expression(engine)
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
                crsr = cnxn.cursor()
                crsr.execute(sql)
                rows = crsr.fetchall()
                for row in rows:
                    self._schema_names.append(row[1])

        if self.engine.name == 'postgresql':
            sql = self.expr.schemata()
            with self.engine.connect() as cnxn:
                crsr = cnxn.cursor()
                crsr.execute(sql)
                rows = crsr.fetchall()
                for row in rows:
                    self._schema_names.append(row[0])

        return self._schema_names

    def tables(self, schema, table=None):
        if hasattr(self, '_tbl_names'):
            return self._tbl_names

        self._tables = Dict()
        with self.engine.connect() as cnxn:
            sql = self.expr.user_tables()
            sql, params = self.expr.prepare(sql, {'schema_name': schema, 'table_name': table})
            crsr = cnxn.cursor()
            crsr.execute(sql, params)
            rows = crsr.fetchall()

            for row in rows:
                rec = to_rec(row, crsr, lowercase=True)
                self._tables[rec.table_name].name = rec.table_name
                self._tables[rec.table_name].type = rec.table_type.lower()
                self._tables[rec.table_name].comment = rec.remarks

        return self._tables

    def pkeys(self, schema, table=None):
        with self.engine.connect() as cnxn:
            sql = self.expr.pkeys()
            sql, params = self.expr.prepare(sql, {'schema_name': schema, 'table_name': table})
            crsr = cnxn.cursor()
            crsr.execute(sql, params)
            rows = crsr.fetchall()

            pkeys = Dict()
            for row in rows:
                rec = to_rec(row, crsr, lowercase=True)
                pkeys[rec.table_name].table_name = rec.table_name
                pkeys[rec.table_name].pkey_name = rec.pk_name
                if 'column_names' in rec:
                    if type(rec.column_names) is str:
                        rec.column_names = yaml.safe_load(rec.column_names)
                    pkeys[rec.table_name].constrained_columns = rec.column_names
                if 'constrained_columns' not in pkeys[rec.table_name]:
                    pkeys[rec.table_name].constrained_columns = []
                if 'column_name' in rec:
                    if (
                        hasattr(row, '_mapping') and self.engine.name == 'oracle'
                        and rec.column_name == rec.column_name.upper()
                    ):
                        rec.column_name = rec.column_name.lower()
                    pkeys[rec.table_name].constrained_columns.append(rec.column_name)

        return pkeys[table] if table else pkeys

    def columns(self, schema, table=None):
        """ Return all columns in schema by reflection """
        # if hasattr(self, '_columns'):
        #     return self._columns
        with self.engine.connect() as cnxn:
            sql = self.expr.columns()
            sql, params = self.expr.prepare(sql, {'schema_name': schema, 'table_name': table})
            crsr = cnxn.cursor()
            crsr.execute(sql, params)
            rows = crsr.fetchall()

            self._columns = Dict()
            for row in rows:
                rec = to_rec(row, crsr, lowercase=True)
                if (type(rec.column_def) is bytes):
                    # default value CURRENT_TIMESTAMP is returned as bytes in mysql
                    rec.column_def = rec.column_def.decode('utf8')
                col = Dict()
                col.table_name = rec.table_name
                col.schema_name = schema
                col.name = rec.column_name
                if (
                    hasattr(row, '_mapping') and self.engine.name == 'oracle'
                    and col.name == col.name.upper()
                ):
                    col.name = col.name.lower()
                col.type = rec.type_name
                col.nullable = rec.nullable
                col.default = rec.column_def
                col.size = rec.column_size if 'column_size' in rec else None
                col.precision = col.size
                col.scale = rec.decimal_digits if 'decimal_digits' in rec else None
                if rec.table_name not in self._columns:
                    self._columns[rec.table_name] = []
                self._columns[rec.table_name].append(col)

        return self._columns[table] if table else self._columns

    def fkeys(self, schema, fk_table=None, pk_table=None):
        all_fkeys = Dict()
        fkeys = Dict()
        with self.engine.connect() as cnxn:
            crsr = cnxn.cursor()
            params = {
                'schema_name': schema,
                'table_name': fk_table,
            }
            sql, params = self.expr.prepare(self.expr.fkeys(), params)
            crsr.execute(sql, params)
            rows = crsr.fetchall()
            for row in rows:
                rec = to_rec(row, crsr, lowercase=True)
                name = rec.fk_name
                tblname = rec.fktable_name
                if 'fkcolumn_names' in rec:
                    if type(rec.fkcolumn_names) is str:
                        rec.fkcolumn_names = yaml.safe_load(rec.fkcolumn_names)
                        rec.pkcolumn_names = yaml.safe_load(rec.pkcolumn_names)
                    fkeys[tblname][name].constrained_columns = rec.fkcolumn_names
                    fkeys[tblname][name].referred_columns = rec.pkcolumn_names
                if 'constrained_columns' not in fkeys[tblname][name]:
                    fkeys[tblname][name].constrained_columns = []
                    fkeys[tblname][name].referred_columns = []
                fkeys[tblname][name].name = rec.fk_name
                fkeys[tblname][name].table_name = rec.fktable_name
                if 'fktable_schem' in rec:
                    fkeys[tblname][name].schema = rec.fktable_schem
                else:
                    fkeys[tblname][name].schema = schema
                if 'fkcolumn_name' in rec:
                    fkeys[tblname][name].constrained_columns.append(rec.fkcolumn_name)
                    fkeys[tblname][name].referred_columns.append(rec.pkcolumn_name)
                if 'pktable_schem' in rec:
                    fkeys[tblname][name].referred_schema = rec.pktable_schem
                else:
                    fkeys[tblname][name].referred_schema = schema
                fkeys[tblname][name].referred_table = rec.pktable_name
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
            params = {'schema_name': schema, 'table_name': table}
            sql, params = self.expr.prepare(sql, params)
            crsr = cnxn.cursor()
            crsr.execute(sql, params)
            rows = crsr.fetchall()

            indexes = Dict()
            for row in rows:
                rec = to_rec(row, crsr, lowercase=True)
                name = rec.index_name
                indexes[rec.table_name][name].name = name
                indexes[rec.table_name][name].unique = not rec.non_unique
                if 'column_names' in rec:
                    if type(rec.column_names) is str:
                        rec.column_names = yaml.safe_load(rec.column_names)
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
            if sql:
                params = {'schema_name': schema, 'table_name': tbl_name}
                sql, params = self.expr.prepare(sql, params)
                crsr.execute(sql, params)
                view_def = crsr.fetchone()[0]

        return view_def
