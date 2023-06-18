"""Module for handling tables"""
import time
import pypandoc
from addict import Dict
from record import Record
from column import Column
from expression import Expression
from grid import Grid


def measure_time(func):
    def wrapper(*arg):
        t = time.time()
        res = func(*arg)
        if (time.time()-t) > 1:
            print("Time in", func.__name__,  str(time.time()-t), "seconds")
        return res

    return wrapper


class Table:
    """Contains methods for getting metadata for table"""

    def __init__(self, db, tbl_name):
        self.db = db
        self.name = tbl_name
        self.label = db.get_label('table', tbl_name)
        self.cache = Dict()
        if hasattr(db, 'tables'):
            for key, val in db.tables[tbl_name].items():
                self.cache[key] = val

    def get_type(self, main_type=None):
        """Return type of table"""
        if not self.cache.get('type', None):
            self.init_type(main_type)

        return self.cache.type

    def init_type(self, main_type=None):
        """Find type. One of 'table', 'list', 'xref', 'view' """
        if (self.db.attrs.get("cache", None) and not self.db.config):
            self.cache.type = self.db.attrs.cache.tables[self.name].type
            return self.cache.type

        crsr = self.db.cnxn.cursor()

        if not main_type:
            tbl = crsr.tables(catalog=self.db.cat, schema=self.db.schema,
                              table=self.name).fetchone()
            self.type_ = tbl.table_type.lower()

        pkey = self.get_pkey()
        # Data type of primary key column
        type_ = None

        if pkey and len(pkey.data_types):
            type_ = self.db.expr.to_urd_type(pkey.data_types[0])
        elif pkey and len(pkey.columns) and pkey.columns != ['rowid']:
            pkey_col_name = pkey.columns[0]
            pkey_col = crsr.columns(catalog=self.db.cat,
                                    schema=self.db.schema,
                                    table=self.name,
                                    column=pkey_col_name).fetchone()
            pkey_col.type_name = pkey_col.type_name.split('(')[0]
            type_ = self.db.expr.to_urd_type(pkey_col.type_name)

        tbl_type = main_type

        if (self.name[-5:] == "_list" or self.name[-6:] == "_liste"):
            tbl_type = "list"
        elif self.name[-5:] in ("_xref", "_link"):
            tbl_type = "xref"
        elif self.name[-4:] == "_ext":
            tbl_type = "ext"
        elif type_ == 'string':
            tbl_type = "list"

        self.cache.type = tbl_type

        return tbl_type

    def user_privileges(self):
        """Return privileges of database user"""
        privileges = Dict({
            'select': 1,
            'insert': 1,
            'update': 1,
            'delete': 1
        })
        sql = self.db.expr.table_privileges()
        if sql:
            params = [self.db.cnxn.user, self.name]
            rows = self.db.query(sql, params).fetchall()
            for row in rows:
                if row.privilege_type == 'SELECT':
                    privileges.select = 1
                elif row.privilege_type == 'INSERT':
                    privileges.insert = 1
                elif row.privilege_type == 'UPDATE':
                    privileges['update'] = 1
                elif row.privilege_type == 'DELETE':
                    privileges.delete = 1

        return privileges

    @measure_time
    def count_rows(self):
        sql = f'select count(*) from "{self.name}"'
        return self.db.query(sql).fetchval()

    def is_hidden(self):
        """Decide if this is a hidden table"""
        if (
            self.name[0:1] == "_" or
            self.name[0:5] == "meta_"
        ):
            hidden = True
        else:
            hidden = False

        return hidden

    def get_indexes(self):
        """Return all table indexes"""
        if not self.cache.get('indexes', None):
            self.init_indexes()

        return self.cache.indexes

    def get_fkeys(self):
        """Return all foreign keys of table"""
        if 'fkeys' not in self.cache:
            self.init_fkeys()

        return self.cache.fkeys

    def get_fkey(self, name):
        """Return single foreign key based on key name or last column"""
        if not self.cache.get('fkeys', None):
            self.init_fkeys()

        if name in self.cache.fkeys:
            return self.cache.fkeys[name]
        else:
            col_fkey = None
            for fkey in self.cache.fkeys.values():
                if (fkey.foreign[-1] == name):
                    if (
                        not col_fkey or
                        len(fkey.foreign) < len(col_fkey.foreign)
                    ):
                        col_fkey = fkey

            return col_fkey

    def get_fields(self):
        """Return all fields of table"""
        if not self.cache.get('fields', None):
            self.init_fields()

        return self.cache.fields

    @measure_time
    def get_pkey(self):
        """Return primary key of table"""
        if self.cache.pkey:
            return self.cache.pkey
        if self.db.cnxn.system == 'sqlite3':
            sql = self.db.expr.pkey(self.name)
            rows = self.db.query(sql)
        else:
            cursor = self.db.cnxn.cursor()
            rows = cursor.primaryKeys(table=self.name, catalog=self.db.cat,
                                      schema=self.db.schema)
        pkey = Dict()
        pkey.table_name = self.name

        for row in rows:
            pkey.pkey_name = row.pk_name
            if 'columns' not in pkey:
                pkey.columns = []
            pkey.columns.append(row.column_name)

        if (not pkey.columns and self.db.system == 'sqlite3'):
            pkey.columns = ['rowid']
            pkey.data_type = ['integer']
        return pkey

    def get_parent_fk(self):
        """Return foreign key defining hierarchy"""
        # Find relation to child records
        relations = self.get_relations()
        rel = [rel for rel in relations.values() if rel.table == self.name][0]
        fkey = self.get_fkey(rel.name)

        return fkey

    def get_join(self):
        """Return all joins to table as single string"""
        if self.cache.get('join', None):
            return self.cache.join
        joins = []
        fkeys = self.get_fkeys()
        aliases = []

        for key, fkey in fkeys.items():
            if fkey.table not in self.db.user_tables:
                continue

            alias = fkey.foreign[-1]

            # In seldom cases there might be two foreign keys ending
            # in same column
            if alias in aliases:
                alias = alias + '2'

            aliases.append(alias)

            # Get the ON statement in the join
            ons = [f'"{alias}"."{fkey.primary[idx]}" = "{self.name}"."{col}"'
                   for idx, col in enumerate(fkey.foreign)]
            on_list = ' AND '.join(ons)

            namespace = self.db.schema or self.db.cat
            joins.append(f'left join {namespace}."{fkey.table}" "{alias}" '
                         f'on {on_list}')

        self.cache.join = "\n".join(joins)

        return self.cache.join

    def get_relation(self, alias):
        """Return single relation"""
        if not self.cache.get('relations', None):
            self.init_relations()

        return self.cache.relations[alias]

    def get_relations(self):
        """Return all 'has many' relations of table"""
        if not self.cache.get('relations', None):
            self.init_relations()

        return self.cache.relations

    def get_rel_tbl_names(self):
        tbl_names = []
        for rel in self.cache.relations.values():
            tbl_names.append(rel.table)

        return tbl_names

    def get_csv(self, columns):
        selects = {}
        for colname in columns:
            selects[colname] = self.name + '.' + colname

        grid = Grid(self)
        records = grid.get_values(selects)

        content = ';'.join(columns) + '\n'

        for rec in records:
            values = []
            for col, val in rec.items():
                if type(val) == str:
                    val = val.replace('"', '""')
                    val = "'" + val + "'"
                elif val is None:
                    val = ''
                else:
                    val = str(val)
                values.append(val)
            content += ';'.join(values) + '\n'

        return content

    def save(self, records: list):
        """Save new and updated records in table"""
        from database import Database
        result = Dict()
        for rec in records:
            rec = Dict(rec)
            record = Record(self.db, self, rec.prim_key)
            if rec.method == 'delete':
                record.delete()
            elif rec.method == 'post':
                pkey = record.insert(rec['values'])

                # Must get autoinc-value for selected record to get
                # correct offset when reloading table after saving
                if 'selected' in rec:
                    result.selected = pkey

            elif rec.method == "put":
                if rec['values']:
                    record.update(rec['values'])

            # Iterates over all the relations to the record
            for key, rel in rec.relations.items():

                if rel.base_name == self.db.name:
                    rel_db = self.db
                else:
                    rel_db = Database(self.db.cnxn, rel.base_name)

                rel_table = Table(rel_db, rel.table_name)

                # Set value of fkey columns to matched colums of record
                for rel_rec in rel.records:
                    if 'values' not in rel_rec:
                        continue
                    for idx, col in enumerate(rel.foreign):
                        pkcol = rel.primary[idx]
                        rel_rec['values'][col] = record.get_value(pkcol)

                        # Primary keys of relation may be updated by
                        # cascade if primary keys of record is updated
                        if col in rel_rec.prim_key:
                            rel_rec.prim_key[col] = record.get_value(pkcol)

                rel_table.save(rel.records)

        return result

    @measure_time
    def init_fkeys(self):
        """Store foreign keys in table object"""
        if (self.db.attrs.get("cache", None) and not self.db.config):
            self.cache.fkeys = self.db.attrs.cache.tables[self.name].fkeys
            return
        cursor = self.db.cnxn.cursor()
        keys = Dict()
        fk_nbr = 0

        for row in cursor.foreignKeys(foreignTable=self.name,
                                      foreignCatalog=self.db.cat,
                                      foreignSchema=self.db.schema):
            if row.pktable_name not in self.db.user_tables:
                continue

            if not row.fk_name:
                if row.key_seq == 1:
                    fk_nbr += 1
                    name = self.name + '_fk' + str(fk_nbr)
            else:
                name = row.fk_name
            if name not in keys:
                keys[name] = Dict({
                    'name': name,
                    'table': row.pktable_name,
                    'base': row.pktable_cat or None,
                    'schema': row.pktable_schem,
                    'delete_rule': row.delete_rule,
                    'update_rule': row.update_rule,
                    'foreign': [],
                    'primary': []
                })
            keys[name].foreign.append(row.fkcolumn_name)
            keys[name].primary.append(row.pkcolumn_name)

            if (self.db.system == 'sqlite3'):
                keys[name].schema = 'main'

        self.cache.fkeys = keys

    @measure_time
    def get_columns(self, column=None):
        """ Return all columns in table by reflection """
        cursor = self.db.cnxn.cursor()
        if self.db.cnxn.system == 'oracle':
            # cursor.columns doesn't work for all types of oracle columns
            sql = self.db.expr.columns()
            cols = cursor.execute(sql, self.db.schema, self.name,
                                  column).fetchall()
        else:
            cols = cursor.columns(table=self.name, catalog=self.db.cat,
                                  schema=self.db.schema,
                                  column=column).fetchall()

        result = []
        colnames = [column[0] for column in cursor.description]
        for col in cols:
            result.append(Dict(zip(colnames, col)))

        return result

    @measure_time
    def init_fields(self):
        """Store Dict of fields in table object"""
        if (self.db.attrs.get("cache", None) and not self.db.config):
            self.cache.fields = self.db.attrs.cache.tables[self.name].fields
            return

        fields = Dict()
        indexes = self.get_indexes()
        indexed_cols = []
        for key, index in indexes.items():
            indexed_cols.append(index.columns[0])
        pkey = self.get_pkey()
        cols = self.get_columns()
        # contents = None if not self.db.attrs.cache \
        #     else self.db.attrs.cache.contents

        for col in cols:
            cname = col.column_name

            column = Column(self, col)
            field = column.get_field()
            if field.fkey:
                condition, params = column.get_condition(field)
                field.options = column.get_options(field, condition, params)

            # Get info about column use if user has chosen this option
            if (
                self.db.config and self.db.config.column_use and
                cname not in pkey.columns and
                not self.name.startswith('meta_')
                # table not in group named '...'
                # and (
                #     not contents['...'] or ('tables.' + self.name) not in \
                #     contents['...'].subitems.values()
                # )
            ):
                if cname not in indexed_cols:
                    column.create_index(col.type_name)

                # Find if column is (largely) empty
                field.use = column.check_use()

                # if (field.use and field.datatype == "string"):
                #     field.size = column.get_size()
                #     if field.size < 256:
                #         field.element = "input[type=text]"

                if col.type_name not in ['blob', 'clob', 'text']:
                    field.frequency = column.check_frequency()

            fields[cname] = field

        updated_idx = indexes.get(self.name + "_updated_idx", None)
        if updated_idx:
            for col in updated_idx.columns:
                fields[col].extra = "auto_update"
                fields[col].editable = False
            if len(updated_idx.columns) == 2:
                col = updated_idx.columns[1]
                fields[col].default = self.db.cnxn.user
        created_idx = indexes.get(self.name + "_created_idx", None)
        if created_idx:
            for col in created_idx.columns:
                fields[col].extra = "auto"
                fields[col].editable = False
            if len(created_idx.columns) == 2:
                col = created_idx.columns[1]
                fields[col].default = self.db.cnxn.user

        self.cache.fields = fields

    @measure_time
    def init_indexes(self):
        """Store Dict of indexes as attribute of table object"""
        if self.db.attrs.get("cache", None) and not self.db.config:
            self.cache.indexes = \
                self.db.attrs.cache.tables[self.name].indexes
            return
        cursor = self.db.cnxn.cursor()
        indexes = Dict()

        for row in cursor.statistics(table=self.name, catalog=self.db.cat,
                                     schema=self.db.schema):
            name = row.index_name
            # Sometimes rows not part of index is returned
            if name is None:
                continue

            if name not in indexes:
                indexes[name] = Dict({
                    'name': name,
                    'unique': not row.non_unique,
                    'columns': []
                })

            indexes[name].columns.append(row.column_name)

        self.cache.indexes = indexes

    def init_relations(self):
        """Store Dict of 'has many' relations as attribute of table object"""
        if hasattr(self.db, 'relations'):
            self.cache.relations = self.db.relations[self.name]
            return
        if self.db.attrs.get("cache", None) and not self.db.config:
            self.cache.relations = \
                self.db.attrs.cache.tables[self.name].relations
            return
        cursor = self.db.cnxn.cursor()
        relations = Dict()
        fktable_name = None

        for row in cursor.foreignKeys(table=self.name, catalog=self.db.cat,
                                      schema=self.db.schema):
            delete_rules = ["cascade", "restrict", "set null", "no action",
                            "set default"]
            if (fktable_name != row.fktable_name):
                fktable_name = row.fktable_name
                fk_nbr = 0
            if not row.fk_name:
                if row.key_seq == 1:
                    fk_nbr += 1
                    name = row.fktable_name + '_fk' + str(fk_nbr)
            else:
                name = row.fk_name

            if name not in relations:
                relations[name] = Dict({
                    'name': name,
                    'table': row.fktable_name,
                    'base': row.fktable_cat or self.db.name,
                    'schema': row.fktable_schem,
                    'delete_rule': delete_rules[row.delete_rule],
                    'foreign': [],
                    'primary': []
                })

            relations[name].foreign.append(row.fkcolumn_name)
            relations[name].primary.append(row.pkcolumn_name)

        # find how much the relation is used
        if self.db.config.column_use:
            for name, relation in relations.items():
                fkey_col = relation.foreign[-1]

                sql = f"""
                select count(distinct({fkey_col})) from {relation.table}
                """

                count = self.db.query(sql).fetchval()

                relations[name].use = count/self.rowcount

        self.cache.relations = relations

    def export_ddl(self, system):
        """Return ddl for table"""
        pkey = self.get_pkey()
        ddl = f"create table {self.name} (\n"
        coldefs = []
        for col in self.get_fields().values():
            expr = Expression(system)
            size = col.size
            if 'scale' in col:
                size = str(col.precision) + "," + str(col.scale)
            datatype = expr.to_native_type(col.datatype, size)
            coldef = f"    {col.name} {datatype}"
            if not col.nullable:
                coldef += " NOT NULL"
            if col.default:
                default = col.default if not col.default_expr \
                    else col.default_expr
                if (
                    col.datatype in ['string', 'date'] and
                    default[0:8] != 'CURRENT_'
                ):
                    coldef += " DEFAULT '" + default + "'"
                else:
                    coldef += " DEFAULT " + default
            coldefs.append(coldef)
        ddl += ",\n".join(coldefs)
        if (pkey and pkey.columns != ['rowid']):
            ddl += ",\n" + "    primary key (" + ", ".join(pkey.columns) + ")"

        for fkey in self.get_fkeys().values():
            ddl += ",\n    foreign key (" + ", ".join(fkey.foreign) + ") "
            ddl += f"references {fkey.table}(" + ", ".join(fkey.primary) + ")"
        ddl += ");\n\n"

        for idx in self.get_indexes().values():
            if idx.columns == pkey.columns:
                continue
            ddl += "create "
            if idx.unique:
                ddl += "unique "
            ddl += f"index {idx.name} on {self.name} ("
            ddl += ",".join(idx.columns) + ");\n"

        return ddl

    def export_records(self, select_recs: bool):
        """Export records as sql

        Parameters:
        select_recs: If records should be selected from existing database
        """
        insert = '\n'
        sql = f"select * from {self.name}"
        cursor = self.db.cnxn.cursor()
        cursor.execute(sql)
        colnames = [column[0] for column in cursor.description]

        if select_recs:
            db_name = self.db.name.split('.')[0]
            insert += f'insert into {self.name}\n'
            insert += 'select ' + ', '.join(colnames)
            insert += f' from {db_name}.{self.name};\n\n'
        else:
            for row in cursor:
                insert += f'insert into {self.name} values ('
                row = Dict(zip(colnames, row))
                for colname, val in row.items():
                    if (self.name == 'meta_data' and colname == 'cache'):
                        val = ''
                    if type(val) is str:
                        val = "'" + val.replace("'", "''") + "'"
                    elif val is None:
                        val = 'null'
                    insert += str(val) + ','
                insert = insert[:-1] + ");\n"

        return insert

    def convert(self, colname, from_format, to_format):

        select = ', '.join(self.tbl.pkey.columns)

        sql = f"""
        select {select}, {colname}
        from {self.tbl.name}
        """

        cursor = self.db.query(sql)
        rows = cursor.fetchall()
        colnames = [col[0] for col in cursor.description]
        cursor2 = self.db.cnxn.cursor()
        for row in rows:
            row = (dict(zip(colnames, row)))
            wheres = []
            params = []
            for key in self.tbl.pkey.columns:
                wheres.append(key + '=?')
                params.append(row[key])

            where = ', '.join(wheres)

            try:
                text = pypandoc.convert_text(row[colname], to_format,
                                             format=from_format)
            except Exception as e:
                print('kunne ikke konvertere ' + params[-1])
                print(e.message)

            params.insert(0, text)

            sql = f"""
            update {self.tbl.name}
            set {self.name} = ?
            where {where}
            """

            cursor2.execute(sql, params)

        cursor2.commit()

        return 'success'


