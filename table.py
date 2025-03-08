"""Module for handling tables"""
import datetime
import pypandoc
import os
from addict import Dict
from record import Record
from column import Column
from field import Field
from grid import Grid
from util import prepare, to_rec
from sqlglot import parse_one, exp
from settings import Settings
from expression import Expression

cfg = Settings()

class Table:
    """Contains methods for getting metadata for table"""

    def __init__(self, db, tbl_name, alias=None):
        self.db = db
        self.name = tbl_name
        self.label = db.get_label(tbl_name)
        self.view = tbl_name
        if tbl_name + '_view' in db.tablenames:
            self.view = tbl_name + '_view'
        self.grid_view = self.view
        if tbl_name + '_grid' in db.tablenames:
            self.grid_view = tbl_name + '_grid'
        self.alias = alias or self.view
        self.fts = False

    def get(self):
        grid = Grid(self)

        tbl_names = self.db.user.tables(self.db.cat, self.db.schema)
        view_names = self.db.refl.get_view_names(self.db.schema)

        self.main_type = 'table' if self.name in (tbl_names) else 'view'

        hidden = self.name[0:1] == "_" or self.name == 'html_attributes'

        # Hides table if user has marked the table to be hidden
        if 'hidden' in self.db.config.tables[self.name]:
            if hidden != self.db.config.tables[self.name].hidden:
                hidden = self.db.config.tables[self.name].hidden
            else:
                del self.db.config.tables[self.name].hidden
                if not self.db.config.tables[self.name]:
                    del self.db.config.tables[self.name]

        # Change table type if set in config
        if 'type' in self.db.config.tables[self.name]:
            if self.type != self.db.config.tables[self.name].type:
                self.type = self.db.config.tables[self.name].type
            else:
                del self.db.config.tables[self.name].type
                if not self.db.config.tables[self.name]:
                    del self.db.config.tables[self.name]

        view = self.name
        if self.name + '_view' in view_names:
            view = self.name + '_view'

        if self.db.engine.name == 'sqlite' or self.name not in self.db.comments:
            comment = None
        else:
            comment = self.db.comments[self.name]

        return Dict({
            'name': self.name,
            'type': self.type,
            'view': view,
            'icon': None,
            'label': self.db.get_label(self.name),
            'rowcount': (None if not self.db.config.update_cache
                         else self.rowcount),
            'pkey': self.pkey,
            'description': comment,
            'fkeys': self.fkeys,
            # Get more info about relations for cache, including use
            'relations': self.relations,
            'indexes': self.indexes,
            'hidden': hidden,
            # fields are needed only when creating cache
            'fields': (None if not self.db.config.update_cache
                       else self.fields),
            'grid': None if not self.db.config.update_cache else {
                'columns': grid.columns
            }
        })


    @property
    def type(self):
        """Return type of table"""
        if not hasattr(self, '_type'):
            self.init_type()

        return self._type

    def init_type(self):
        """Find type. One of 'table', 'list', 'xref', 'ext', 'view' """
        if (self.db.cache and not self.db.config.update_cache):
            self._type = self.db.cache.tables[self.name].type
            return self._type

        if not hasattr(self, 'main_type'):
            tbl_names = self.db.refl.get_table_names(self.db.schema)
            self.main_type = 'table' if self.name in tbl_names else 'view'

        if self.main_type == 'view':
            self._type = 'view'
            return 'view'

        self._type = 'list'
        for colname in self.pkey.columns:
            # Data type of primary key column
            pkey_col = None
            pkey_col_type = None
            pkey_col_length = None
            # Find data type for last pkey column
            if (
                self.pkey and len(self.pkey.columns) and
                self.pkey.columns != ['rowid']
            ):
                # colname = self.pkey.columns[-1]
                cols = self.db.columns[self.name]
                for col in cols:
                    if col['name'] == colname:
                        pkey_col = Column(self, col)
                        break
                if type(pkey_col.type) is str:
                    pkey_col_type = self.db.refl.expr.to_urd_type(pkey_col.type)
                else:
                    pkey_col_type = pkey_col.type.python_type.__name__
                if hasattr(pkey_col, 'size'):
                    pkey_col_length = pkey_col.size

            # self._type = self.main_type

            # if (self.name[-5:] == "_list" or self.name[-6:] == "_liste"):
            #     self._type = "list"
            smallints = ['tinyint', 'smallint', 'mediumint']
            if self.name[-5:] in ("_xref", "_link"):
                self._type = "xref"
            elif self.name[-4:] == "_ext":
                self._type = "ext"
            elif (
                (pkey_col_type == 'str' and not pkey_col_length or
                 pkey_col_type == 'str' and pkey_col_length >= 10) or (
                    pkey_col and (
                        ('int' in str(pkey_col.type).lower() and
                         str(pkey_col.type).lower() not in smallints) or
                        ('numeric' in str(pkey_col.type).lower() and
                         pkey_col.precision - pkey_col.scale >= 8) or
                        'date' in str(pkey_col.type).lower() 
                    )
                )
            ):
                self._type = "data"

        all_fkey_columns = set()
        for fkey in self.fkeys.values():
            # An fkey with same columns as primary key designates an
            # extension table. But the fkey can also have invisible columns
            # used to control if the table should be displayed as relation
            cols = [col for col in fkey.constrained_columns
                    if not (col.startswith('_') or col.startswith('const_'))]
            if cols == self.pkey.columns:
                self._type = "ext"
                break
            elif set(cols) < set(self.pkey.columns):
                all_fkey_columns.update(set(cols))

        if set(self.pkey.columns) <= all_fkey_columns:
            self._type = "xref"

        return self._type

    @property
    def rowcount(self):
        if not hasattr(self, '_rowcount'):
            sql, _ = prepare(f'select count(*) from "{self.name}"')
            with self.db.engine.connect() as cnxn:
                self._rowcount = cnxn.execute(sql).fetchone()[0]

        return self._rowcount

    def is_hidden(self):
        """Decide if this is a hidden table"""
        if (
            self.name[0:1] == "_" or
            self.name[0:5] == "html_"
        ):
            hidden = True
        else:
            hidden = False

        return hidden

    @property
    def indexes(self):
        """Return all table indexes"""
        if not hasattr(self, '_indexes'):
            self.init_indexes()

        return self._indexes

    @property
    def fkeys(self):
        if not hasattr(self, '_fkeys'):
            self.init_fkeys()

        return self._fkeys

    def get_fkey(self, name):
        """Return single foreign key based on key name or last column"""
        if not hasattr(self, '_fkeys'):
            self.init_fkeys()

        if name in self._fkeys:
            return self._fkeys[name]
        else:
            col_fkey = None
            for fkey in self._fkeys.values():
                if (fkey.constrained_columns[-1] == name):
                    if (
                        not col_fkey or
                        # If more than one fkey meets the requirement,
                        # choose the one with fewest columns
                        len(fkey.constrained_columns) <
                        len(col_fkey.constrained_columns)
                    ):
                        col_fkey = fkey

            return col_fkey

    @property
    def fields(self):
        """Return all fields of table"""
        if not hasattr(self, '_fields'):
            if (self.db.cache and not self.db.config.update_cache):
                self._fields = self.db.cache.tables[self.name].fields
            else:
                self.init_fields()

        return self._fields

    @property
    def pkey(self):
        """Return primary key of table"""
        if hasattr(self, '_pkey'):
            return self._pkey
        if (self.db.cache and not self.db.config.update_cache):
            self._pkey = self.db.cache.tables[self.name].pkey
            return self._pkey

        self._pkey = self.db.pkeys[self.name]

        if (
            not self._pkey.columns and
            self.db.engine.name == 'sqlite' and
            self.type != 'view'
        ):
            self._pkey.columns = ['rowid']
            self._pkey.name = self.name + '_rowid'

        if (not self._pkey.columns):
            attrs = self.db.html_attrs
            selector = f'table[data-name="{self.name}"]'
            if attrs[selector]['data-pkey']:
                self._pkey.name = self.name + '_pkey'
                self._pkey.columns = attrs[selector]['data-pkey']

        return self._pkey

    def get_parent_fk(self):
        """Return foreign key defining hierarchy"""
        # Find relation to child records
        rel = [rel for rel in self.relations.values()
               if rel.table_name == self.name][0]
        fkey = self.get_fkey(rel.name)

        return fkey

    @property
    def joins(self):
        """Return all joins to table as single string"""
        if hasattr(self, '_joins'):
            return self._joins
        joins = []

        for key, fkey in self.fkeys.items():
            if fkey.referred_table not in self.db.tablenames:
                continue

            # Don't get joins for foreign keys defining 1:1-relations
            # when the table itself is a foreign key join. These have an alias
            # that is made from the referencing table and column 
            if (
                self.alias != self.view and
                set(fkey.constrained_columns) < set(self.pkey.columns)
            ):
                continue

            # Get the ON statement in the join
            ons = [f'{fkey.ref_table_alias}.{fkey.referred_columns[idx]} = '
                   f'{self.alias}.{col}'
                   for idx, col in enumerate(fkey.constrained_columns)]
            on_list = ' AND '.join(ons)

            joins.append(f'left join {self.db.schema}.{fkey.referred_table} '
                         f'{fkey.ref_table_alias} on {on_list}')

            # Join with 1:1 relation carrying access code
            fkey_table = Table(self.db, fkey.referred_table)
            access_idx = fkey_table.get_access_code_idx()
            if access_idx and access_idx.table_name != fkey_table.name:
                for key, rel_fkey in fkey_table.relations.items():
                    if rel_fkey.table_name == access_idx.table_name:
                        if rel_fkey.table_name == self.name:
                            continue
                        ons = [f"{rel_fkey.table_name}.{rel_fkey.constrained_columns[idx]} = "
                               f"{fkey.ref_table_alias}.{col}"
                               for idx, col in enumerate(rel_fkey.referred_columns)]
                        on_list = ' AND '.join(ons)
                        joins.append(f"left join {self.db.schema}.{rel_fkey.table_name} "
                                     f"on {on_list}")

        for key, fkey in self.relations.items():
            if fkey.relationship == '1:1':
                prefix = fkey.referred_table.rstrip('_') + '_'
                alias = fkey.table_name.replace(prefix, '')
                ons = [f"{alias}.{fkey.constrained_columns[idx]} = "
                       f"{self.view}.{col}"
                       for idx, col in enumerate(fkey.referred_columns)]
                on_list = ' AND '.join(ons)
                joins.append(f"left join {self.db.schema}.{fkey.table_name} {alias} "
                             f"on {on_list}")

                rel_tbl = Table(self.db, fkey.table_name, alias=alias)
                for join in rel_tbl.joins:
                    # Don't add the join defining the 1:1 relation
                    if f'left join {self.db.schema}.{self.name} ' not in join:
                        joins.append(join)

        if (self.name + '_grid') in self.db.tablenames:
            join_view = "join " + self.grid_view + " on "
            ons = [f'{self.grid_view}.{col} = {self.view}.{col}'
                   for col in self.pkey.columns]
            join_view += ' AND '.join(ons) + "\n"
        else:
            join_view = ""

        joins.append(join_view)

        if self.fts and self.name + '_fts' in self.db.tablenames:
            join = f"join {self.name}_fts fts on fts.rowid = {self.name}.rowid\n"
            joins.append(join)
        
        self._joins = joins

        return self._joins

    def get_relation(self, alias):
        """Return single relation"""
        if not hasattr(self, 'relations'):
            self.init_relations()

        return self.relations[alias]

    @property
    def relations(self):
        """Return all 'has many' relations of table"""
        if not hasattr(self, '_relations'):
            self.init_relations()

        return self._relations

    def get_access_code_idx(self):
        idx_name = self.name.rstrip('_') + '_access_code_idx'

        # Check if access index is set on main table
        if idx_name in self.indexes:
            idx = self.indexes[idx_name]
            idx.table_name = self.name
            idx.table_alias = self.view
            return idx

        # Check if access index is set on extension table
        for key, rel in self.relations.items():
            rel_table = Table(self.db, rel.table_name)
            prefix = rel.referred_table.rstrip('_') + '_'
            alias = rel.table_name.replace(prefix, '')

            # accept index name based on main table
            if rel.relationship == '1:1' and idx_name in rel_table.indexes:
                idx = rel_table.indexes[idx_name]
                idx.table_name = rel_table.name
                idx.table_alias = alias
                return idx

            # accept index name based on relation table
            idx_name_rel = rel.table_name.rstrip('_') + '_access_code_idx'
            if rel.relationship == '1:1' and idx_name_rel in rel_table.indexes:
                idx = rel_table.indexes[idx_name_rel]
                idx.table_name = rel_table.name
                idx.table_alias = alias
                return idx

        return None

    def get_rel_tbl_names(self):
        tbl_names = []
        for rel in self.relations.values():
            tbl_names.append(rel.table_name)

        return tbl_names

    def write_tsv(self, filepath, columns = None):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        blobcolumns = []
        selects = {}
        for fieldname in self.fields:
            field = self.fields[fieldname]
            if field.datatype == 'bytes':
                foldername = self.name + '.' + field.name
                path = os.path.join(os.path.dirname(filepath), '../documents', foldername)
                os.makedirs(path, exist_ok=True)
                blobcolumns.append(field.name)
            if not columns or field.name in columns:
                selects[field.name] = field.name
                if field.datatype == 'geometry':
                    selects[field.name] = f"{field.name}.ToString() as {field.name}"

        select = ', '.join(selects.values())

        file = open(filepath, 'w')
        sql = f"select {select} from " + self.name
        with self.db.engine.connect() as cnxn:
            sql, _ = prepare(sql)
            rows = cnxn.execute(sql)
            n = 0
            for row in rows:
                n += 1
                rec = to_rec(row)
                if n == 1:
                    file.write('\t'.join(rec.keys()) + '\n')
                values = []
                num_files = 0
                for col, val in rec.items():
                    if col in blobcolumns:
                        num_files += 1
                        dir = os.path.dirname(filepath)
                        if self.pkey:
                            pkey_vals = []
                            for pkey_col in self.pkey.columns:
                                pkey_vals.append(str(rec[pkey_col]))
                            filename = '-'.join(pkey_vals) + '.data'
                        else:
                            filename = str(num_files) + '.data'

                        foldername = self.name + '.' + col
                        path = os.path.join(dir, '../documents', foldername, filename)
                        if val is not None:
                            with open(path, 'wb') as blobfile:
                                blobfile.write(val)
                            val = 'documents/' + foldername + '/' + filename
                    if type(val) is bool:
                        val = int(val)
                    if type(val) is str:
                        val = val.replace('\t', ' ')
                        val = val.replace('\r\n', ' ')
                        val = val.replace('\r', ' ')
                        val = val.replace('\n', ' ')
                    elif val is None:
                        val = ''
                    else:
                        val = str(val)
                    values.append(val)
                file.write('\t'.join(values) + '\n')
            file.close()
            if n == 0:
                os.remove(filepath)


    def save(self, records: list):
        """Save new and updated records in table"""
        from database import Database
        result = Dict()
        for rec in records:
            rec = Dict(rec)
            record = Record(self.db, self, rec.prim_key)
            if rec.method == 'delete' and rec.prim_key:
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

                if rel.schema == self.db.schema:
                    rel_db = self.db
                else:
                    schema = rel.schema or rel.base_name
                    rel_db = Database(self.db.engine, schema, self.db.user.name)

                rel_table = Table(rel_db, rel.table_name)

                # Set value of fkey columns to matched colums of record
                for rel_rec in rel.records:
                    if 'values' not in rel_rec:
                        continue
                    for idx, col in enumerate(rel.constrained_columns):
                        pkcol = rel.referred_columns[idx]
                        rel_rec['values'][col] = record.get_value(pkcol)

                        # Primary keys of relation may be updated by
                        # cascade if primary keys of record is updated
                        if col in rel_rec.prim_key:
                            rel_rec.prim_key[col] = record.get_value(pkcol)

                rel_table.save(rel.records)

        return result

    def init_fkeys(self):
        """Store foreign keys in table object"""
        if (self.db.cache and not self.db.config.update_cache):
            self._fkeys = self.db.cache.tables[self.name].fkeys
            return
        self._fkeys = self.db.fkeys[self.name]

    def init_fields(self):
        """Store Dict of fields in table object"""
        fields = Dict()
        indexed_cols = []
        for key, index in self.indexes.items():
            # Bug in SQLAlchemy's get_multi_indexes so that an index
            # without columns can be returned
            if len(index.columns):
                indexed_cols.append(index.columns[0])

        # contents = None if not self.db.cache \
        #     else self.db.cache.contents

        if not cfg.use_odbc and self.db.engine.name == 'sqlite':
            # Must get native column types for sqlite, to find if
            # there is a column defined as json
            expr = Expression(self.db.engine.name)
            sql = expr.columns(self.name)
            with self.db.engine.connect() as cnxn:
                sql, _ = prepare(sql)
                rows = cnxn.execute(sql).fetchall()
                cols = [to_rec(row) for row in rows]
        else:
            cols = self.db.refl.get_columns(self.name, self.db.schema)

        for col in cols:
            col = Dict(col)

            column = Column(self, col)
            field = Field(self, col.name)
            field.set_attrs_from_col(column)
            if hasattr(field, 'fkey'):
                field.options = field.get_options('', {})

            if (
                field.name in indexed_cols and not hasattr(field, 'options') and
                field.datatype == 'str' and
                field.attrs.get('data-format', None) != 'ISO 8601'
            ):
                # Make the field show up as autocomplete
                field.attrs['type'] = 'search'

            # Get info about column use if user has chosen this option
            if (
                self.db.config and self.db.config.column_use and
                col.name not in self.pkey.columns and
                not self.name.startswith('meta_') and
                self.type != 'view'
                # table not in group named '...'
                # and (
                #     not contents['...'] or ('tables.' + self.name) not in \
                #     contents['...'].subitems.values()
                # )
            ):
                if col.name not in indexed_cols:
                    column.create_index(col.type_name)

                # Find if column is (largely) empty
                field.use = column.check_use()

                # if (field.use and field.datatype == "str"):
                #     field.size = column.get_size()
                #     if field.size < 256:
                #         field.element = "input[type=text]"

                if col.type_name not in ['blob', 'clob', 'text']:
                    field.frequency = column.check_frequency()

            fields[col.name] = field.get()

        updated_idx = self.indexes.get(self.name + "_updated_idx", None)
        if updated_idx:
            for col in updated_idx.columns:
                fields[col].extra = "auto_update"
                fields[col].editable = False
            if len(updated_idx.columns) == 2:
                col = updated_idx.columns[1]
                fields[col].default = self.db.user.name
        created_idx = self.indexes.get(self.name + "_created_idx", None)
        if created_idx:
            for col in created_idx.columns:
                fields[col].extra = "auto"
                fields[col].editable = False
            if len(created_idx.columns) == 2:
                col = created_idx.columns[1]
                fields[col].default = self.db.user.name

        self._fields = fields

    def init_indexes(self):
        """Store Dict of indexes as attribute of table object"""
        if self.db.cache and not self.db.config.update_cache:
            self._indexes = self.db.cache.tables[self.name].indexes
            return

        self._indexes = self.db.indexes[self.name]

    def init_relations(self):
        """Store Dict of 'has many' relations as attribute of table object"""
        table_name = self.name
        if self.type == 'view':
            try:
                view_def = (self.db.refl
                            .get_view_definition(self.name, self.db.schema))
            except Exception as e:
                view_def = None
                print('Error getting view definition. Check permission')
                print(e)

            if view_def:
                # get dialect for SQLGlot
                dialect = self.db.engine.name
                if dialect == 'mssql':
                    dialect = 'tsql'
                elif dialect == 'postgresql':
                    dialect = 'postgres'
                elif dialect == 'mariadb':
                    dialect = 'mysql'

                table = (parse_one(view_def, read=dialect)
                         .find(exp.From)
                         .find(exp.Table))
                if table:
                    table.pkey = self.db.pkeys[table.name]
                    if (
                        table.pkey and self.pkey and
                        table.pkey.columns == self.pkey.columns
                    ):
                        table_name = table.name
        if hasattr(self.db, 'relations') and not self.db.config.update_cache:
            self._relations = self.db.relations[table_name]
            return
        if self.db.cache and not self.db.config.update_cache:
            self._relations = self.db.cache.tables[table_name].relations
            return

        relations = self.db.relations[table_name]

        # find how much the relation is used
        if self.db.config.column_use:
            for name, relation in relations.items():
                fkey_col = relation.constrained_columns[-1]

                sql, _ = prepare(f"""
                select count(distinct({fkey_col})) from {relation.table_name}
                """)

                with self.db.engine.connect() as cnxn:
                    sql, _ = prepare(sql)
                    count = cnxn.execute(sql).fetchone()[0]

                relations[name].use = count/self.rowcount if self.rowcount > 0 else 0

        self._relations = relations

    def export_ddl(self, dialect, no_fkeys):
        """Return ddl for table"""
        ddl = f"\ncreate table {self.name} (\n"
        coldefs = []
        cols = self.db.refl.get_columns(self.name, self.db.schema)
        for col in cols:
            col = Dict(col)
            column = Column(self, col)
            coldef = column.get_def(dialect, blob_to_varchar=True,
                                    geometry_to_text=True)
            coldefs.append(coldef)

            if type(column.type) is str:  # odbc engine
                datatype = self.db.refl.expr.to_urd_type(col.type)
            else:
                try:
                    datatype = col.type.python_type.__name__
                except Exception:
                    datatype = ('int' if str(col.type).startswith('YEAR')
                                else 'unknown')
                    print('type not recognized', col.type)

            if datatype == 'bytes':
                self.indexes[f'{self.name}_{column.name}_filepath_idx'] = Dict({
                    'name': f'{self.name}_{column.name}_filepath_idx',
                    'columns': [column.name],
                    'unique': False
                })
        ddl += ",\n".join(coldefs)
        if (self.pkey.columns and self.pkey.columns != ['rowid']):
            ddl += f",\n    primary key ({', '.join(self.pkey.columns)})"

        if not no_fkeys:
            for fkey in self.fkeys.values():
                ddl += ",\n    foreign key ("
                ddl += ", ".join(fkey.constrained_columns) + ") "
                ddl += f"references {fkey.referred_table}("
                ddl += ", ".join(fkey.referred_columns) + ")"
        ddl += ");\n\n"

        return ddl

    def get_indexes_ddl(self):
        ddl = ''
        index_written = False
        for idx in self.indexes.values():
            if idx.unique and idx.columns == self.pkey.columns:
                continue
            ddl += "create "
            if idx.unique:
                ddl += "unique "
            # Only mysql has index within table namespace
            idx_name = idx.name
            if idx.name == '_'.join(idx.columns):
                idx_name = self.name + '_' + idx.name
            ddl += f"index {idx_name} on {self.name}("
            ddl += ",".join(idx.columns) + ");\n"
            index_written = True

        if index_written:
            ddl += '\n'

        return ddl

    def write_inserts(self, file, dialect, select_recs, fkey=None):
        """Export records as sql

        Parameters:
        select_recs: If records should be selected from existing database
        """

        insert = ''
        if fkey and self.db.engine.name in ['mysql', 'postgresql', 'sqlite']:
            cols = self.db.refl.get_columns(self.name, self.db.schema)
            colnames = []
            for col in cols:
                colnames.append(col['name'])

            select = ', '.join(colnames)
            fkey_cc = fkey['constrained_columns'][0]
            fkey_rc = fkey['referred_columns'][0]
            sql = f"""
            with recursive tbl_data as (
                select {self.name}.*, 1 as level
                from {self.name}
                where {fkey_cc} is null

                union all

                select this.*, prior.level + 1
                from tbl_data prior
                inner join {self.name} this
                   on this.{fkey_cc} = prior.{fkey_rc}
            )
            select {select}
            from tbl_data
            order by level
            """
        else:
            sql = f"select * from {self.name}"

        with self.db.engine.connect() as cnxn:
            sql, _ = prepare(sql)
            rows = cnxn.execute(sql)

            sql, _ = prepare('select count(*) from ' + self.name)
            rowcount = cnxn.execute(sql).fetchone()[0]

            if dialect == 'oracle':
                insert += f'insert into {self.name}\n'
            else:
                insert += f'insert into {self.name} values '
            i = 0
            # Insert time grows exponentially with number of inserts
            # per `insert all` in Oracle after a certain value.
            # This value is around 50 for version 19c
            max = 50 if dialect == 'oracle' else 1000;
            for row in rows:
                rec = to_rec(row)
                if (i > 0 and i % max == 0):
                    if dialect == 'oracle':
                        if i % 10000 == 0:
                            insert += 'commit;\n\n'
                            insert += f'prompt {i} of {rowcount}\n'
                        insert += f'insert into {self.name}\n'
                        insert += 'select '
                    else:
                        insert += f'insert into {self.name} values ('
                elif dialect == 'oracle':
                    insert += 'select '
                else:
                    insert += '('
                i += 1
                for colname, val in rec.items():
                    col = self.fields[colname]
                    if (self.name == 'meta_data' and colname == 'cache'):
                        val = ''
                    if type(val) is str:
                        val = "'" + val.strip().replace("'", "''") + "'"
                    elif isinstance(val, datetime.date):
                        val = "'" + str(val) + "'"
                    elif (col.datatype == 'bool' and dialect == 'oracle'):
                        if val is False:
                            val = 0
                        elif val is True:
                            val = 1
                    elif val is None:
                        val = 'null'
                    insert += str(val) + ','
                if i % max == 0 or i == rowcount:
                    if dialect == 'oracle':
                        insert = insert[:-1] + ' from dual;\n\n'
                    else:
                        insert = insert[:-1] + ');\n\n'
                elif dialect == 'oracle':
                    insert = insert[:-1] + ' from dual union all\n'
                else:
                    insert = insert[:-1] + "),\n"
                file.write(insert)
                insert = ''
            if dialect == 'oracle':
                pass
            elif i == 0:
                insert = '\n'.join(insert.split('\n')[:-1])
                file.write(insert)

        return 'success'

    def convert(self, colname, from_format, to_format):

        select = ', '.join(self.pkey.columns)

        sql = f"""
        select {select}, {colname}
        from {self.name}
        """

        with self.db.engine.connect() as cnxn:
            sql, _ = prepare(sql)
            rows = cnxn.execute(sql).fetchall()
        for row in rows:
            rec = to_rec(row)
            if rec[colname] is None:
                continue
            wheres = []
            params = {}
            for key in self.pkey.columns:
                wheres.append(key + '= :' + key)
                params[key] = rec[key]

            where = ', '.join(wheres)

            try:
                value = pypandoc.convert_text(rec[colname], to_format,
                                              format=from_format)
            except Exception as e:
                print('kunne ikke konvertere ' + params[-1])
                print(e.message)

            params[colname] = value

            sql = f"""
            update {self.name}
            set {colname} = :{colname}
            where {where}
            """

            with self.db.engine.connect() as cnxn:
                sql, params = prepare(sql, params)
                cnxn.execute(sql, params)
                cnxn.commit()

        return 'success'
