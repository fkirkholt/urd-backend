from addict import Dict
from expression import Expression


class Reflection:

    def __init__(self, engine, catalog):
        self.engine = engine
        self.expr = Expression(engine.name)
        self.cat = catalog
        self.fkeys = None

    def get_schema_names(self):
        """Get all schemata in database"""
        if hasattr(self, '_schema_names'):
            return self._schema_names
        self._schema_names = []

        if self.engine.name == 'sqlite':
            sql = "PRAGMA database_list"
            with self.engine.connect() as cnxn:
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

    def get_table_names(self, schema):
        if hasattr(self, '_tbl_names'):
            return self._tbl_names
        self._tbl_names = []
        with self.engine.connect() as cnxn:
            cursor = cnxn.cursor()
            if self.engine.name in ('mysql', 'mariadb'):
                rows = cursor.tables(catalog=schema).fetchall()
            else:
                rows = cursor.tables(catalog=self.cat, schema=schema).fetchall()
            self._tbl_names = [row.table_name for row in rows if row.table_type == 'TABLE']

        return self._tbl_names

    def get_multi_table_comment(self, schema):
        if hasattr(self, '_comments'):
            return self._comments
        self._comments = Dict()
        with self.engine.connect() as cnxn:
            cursor = cnxn.cursor()
            if self.engine.name in ('mysql', 'mariadb'):
                rows = cursor.tables(catalog=schema).fetchall()
            else:
                rows = cursor.tables(catalog=self.cat, schema=schema).fetchall()
            for row in rows:
                self._comments[schema, row.table_name].text = row.remarks

        return self._comments

    def get_view_names(self, schema):
        if hasattr(self, '_view_names'):
            return self._view_names
        self._view_names = []
        with self.engine.connect() as cnxn:
            cursor = cnxn.cursor()
            if self.engine.name in ('mysql', 'mariadb'):
                rows = cursor.tables(catalog=schema).fetchall()
            else:
                rows = cursor.tables(catalog=self.cat, schema=schema).fetchall()
            self._view_names = [row.table_name for row in rows
                                if row.table_type == 'VIEW']

        return self._view_names

    def get_multi_pk_constraint(self, schema):
        if hasattr(self, '_pkeys'):
            return self._pkeys
        tbls = self.get_table_names(schema)
        self._pkeys = Dict()
        with self.engine.connect() as cnxn:
            crsr = cnxn.cursor()
            for tbl_name in tbls:
                if self.engine.name in ['sqlite']:
                    # Wrong order for pkeys using cursor.primaryKeys
                    sql = self.expr.pkey(tbl_name)
                    rows = cnxn.execute(sql).fetchall()
                elif self.engine.name in ('mysql', 'mariadb'):
                    rows = crsr.primaryKeys(table=tbl_name, catalog=schema)
                else:
                    rows = crsr.primaryKeys(table=tbl_name,
                                            catalog=self.cat,
                                            schema=schema)
                for row in rows:
                    self._pkeys[(schema, tbl_name)].table_name = tbl_name
                    self._pkeys[(schema, tbl_name)].pkey_name = row.pk_name
                    if 'constrained_columns' not in self._pkeys[(schema, tbl_name)]:
                        self._pkeys[(schema, tbl_name)].constrained_columns = []
                    self._pkeys[(schema, tbl_name)].constrained_columns.append(row.column_name)
        return self._pkeys

    def get_multi_columns(self, schema):
        """ Return all columns in schema by reflection """
        if hasattr(self, '_columns'):
            return self._columns
        with self.engine.connect() as cnxn:
            cursor = cnxn.cursor()
            if self.engine.name == 'oracle':
                # cursor.columns doesn't work for all types of oracle columns
                sql = self.expr.columns()
                rows = cursor.execute(sql, schema, None, None).fetchall()
            elif self.engine.name in ('mysql', 'mariadb'):
                rows = cursor.columns(catalog=schema).fetchall()
            else:
                rows = cursor.columns(catalog=self.cat, schema=schema).fetchall()

        self._columns = Dict()
        colnames = [column[0] for column in cursor.description]
        for row in rows:
            row = Dict(zip(colnames, row))
            col = Dict()
            col.name = row.column_name
            col.type = row.type_name
            col.nullable = row.nullable
            col.default = row.column_def
            col.size = row.column_size
            if (schema, row.table_name) not in self._columns:
                self._columns[(schema, row.table_name)] = []
            self._columns[(schema, row.table_name)].append(col)

        return self._columns

    def get_columns(self, tbl_name, schema):
        """ Return all columns in table by reflection """
        if not hasattr(self, '_columns'):
            _ = self.get_multi_columns(schema)

        return self._columns[(schema, tbl_name)]

    def get_foreign_keys(self, tbl_name, schema):
        if self.fkeys is None:
            self.fkeys = self.get_multi_foreign_keys(schema)

        return self.fkeys[schema, tbl_name]

    def get_multi_foreign_keys(self, schema):
        if self.fkeys:
            return self.fkeys
        all_fkeys = Dict()
        with self.engine.connect() as cnxn:
            crsr = cnxn.cursor()
            if self.engine.name in ['mysql', 'mariadb', 'oracle', 'postgres']:
                sql = self.expr.fkeys()
                fkeys = Dict()
                print('sql', sql)
                for row in crsr.execute(sql, schema or cat):
                    fkey = Dict()
                    key = (schema, row.fktable_name)
                    if key not in all_fkeys:
                        all_fkeys[key] = Dict()
                    tblname = row.fktable_name
                    name = row.fk_name
                    if 'constrained_columns' not in fkeys[tblname][name]:
                        fkeys[tblname][name].constrained_columns = []
                        fkeys[tblname][name].referred_columns = []
                    fkeys[tblname][name].name = row.fk_name
                    fkeys[tblname][name].constrained_columns.append(row.fkcolumn_name)
                    fkeys[tblname][name].referred_columns.append(row.pkcolumn_name)
                    fkeys[tblname][name].referred_schema = row.pktable_schema
                    fkeys[tblname][name].referred_table = row.pktable_name
                for tblname in fkeys:
                    all_fkeys[schema, tblname] = fkeys[tblname].values() 
            else:
                tbl_names = self.get_table_names(schema)
                for tbl_name in tbl_names:
                    fkeys = Dict()
                    fk_nbr = 0
                    rows = crsr.foreignKeys(catalog=self.cat, schema=schema, foreignTable=tbl_name).fetchall()
                    for row in rows:
                        if not row.fk_name:
                            if row.key_seq == 1:
                                fk_nbr += 1
                                name = tbl_name + '_fk' + str(fk_nbr)
                        else:
                            name = row.fk_name
                        fkeys[name].name = name
                        if not 'constrained_columns' in fkeys[name]:
                            fkeys[name].constrained_columns = []
                            fkeys[name].referred_columns = []
                        fkeys[name].constrained_columns.append(row.fkcolumn_name)
                        fkeys[name].referred_columns.append(row.pkcolumn_name)
                        fkeys[name].referred_schema = row.pktable_schem
                        fkeys[name].referred_table = row.pktable_name
                    all_fkeys[schema, tbl_name] = fkeys.values()

        self.fkeys = all_fkeys

        return all_fkeys

    def get_multi_indexes(self, schema):
        crsr = self.engine.connect().cursor()
        indexes = Dict()
        if self.engine.name in ['mysql', 'mariadb', 'oracle']:
            sql = self.expr.indexes()
            for row in crsr.execute(sql, self.cat or schema):
                name = row.index_name

                indexes[row.table_name][name].name = name
                indexes[row.table_name][name].unique = not row.non_unique
                if 'column_names' not in indexes[row.table_name][name]:
                    indexes[row.table_name][name].column_names = []
                indexes[row.table_name][name].column_names.append(row.column_name)
        else:
            tbls = crsr.tables(catalog=self.cat, schema=schema).fetchall()
            for tbl in tbls:
                tbl_name = tbl.table_name
                for row in crsr.statistics(tbl_name):
                    name = row.index_name
                    indexes[tbl_name][name].name = name
                    indexes[tbl_name][name].unique = not row.non_unique
                    if 'column_names' not in indexes[tbl_name][name]:
                        indexes[tbl_name][name].column_names = []
                    indexes[tbl_name][name].column_names.append(row.column_name)

        all_indexes = Dict()
        for tbl_name in indexes:
            all_indexes[schema, tbl_name] = []
            for key, idx in indexes[tbl_name].items():
                all_indexes[schema, tbl_name].append(idx)
        return all_indexes

    def get_view_definition(self, tbl_name, schema):
        crsr = self.engine.connect().cursor()
        sql = self.expr.view_definition()
        view_def = None
        if self.engine.name == 'sqlite':
            view_def = crsr.execute(sql, tbl_name).fetchone()[0]
        elif sql:
            params = [schema, tbl_name]
            view_def = crsr.execute(sql, params).fetchone()[0]

        return view_def
