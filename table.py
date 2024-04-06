"""Module for handling tables"""
import datetime
import pypandoc
from addict import Dict
from record import Record
from column import Column
from field import Field
from grid import Grid
from sqlglot import parse_one, exp
from sqlalchemy import text


class Table:
    """Contains methods for getting metadata for table"""

    def __init__(self, db, tbl_name):
        self.db = db
        self.name = tbl_name
        self.label = db.get_label(tbl_name)
        self.view = tbl_name
        if tbl_name + '_view' in db.tablenames:
            self.view = tbl_name + '_view'
        self.grid_view = self.view
        if tbl_name + '_grid' in db.tablenames:
            self.grid_view = tbl_name + '_grid'

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

        # Data type of primary key column
        pkey_col = None
        pkey_col_type = None
        pkey_col_length = None

        # Find data type for first pkey column
        if (
            self.pkey and len(self.pkey.columns) and
            self.pkey.columns != ['rowid']
        ):
            colname = self.pkey.columns[0]
            cols = self.db.columns[self.name]
            for col in cols:
                if col['name'] == colname:
                    pkey_col = Dict(col)
                    break
            pkey_col_type = pkey_col.type.python_type.__name__
            if hasattr(pkey_col.type, 'length'):
                pkey_col_length = pkey_col.type.length

        self._type = self.main_type

        if (self.name[-5:] == "_list" or self.name[-6:] == "_liste"):
            self._type = "list"
        elif self.name[-5:] in ("_xref", "_link"):
            self._type = "xref"
        elif self.name[-4:] == "_ext":
            self._type = "ext"
        elif (
            (pkey_col_type == 'str' and pkey_col_length and pkey_col_length < 10) or (
                pkey_col and (
                    'TINYINT' in str(pkey_col.type) or
                    'SMALLINT' in str(pkey_col.type) or
                    'MEDIUMINT' in str(pkey_col.type) or (
                        'NUMERIC' in str(pkey_col.type) and
                        (pkey_col.type.scale is not None or pkey_col.type.precision < 10)
                    )
                )
            )
        ):
            self._type = "list"

        for fkey in self.fkeys.values():
            if fkey.constrained_columns == self.pkey.columns:
                self._type = "ext"
                break

        return self._type

    def is_subordinate(self):
        subordinate = False
        for colname in self.pkey.columns:
            if self.get_fkey(colname):
                subordinate = True

        return subordinate

    def count_rows(self):
        sql = f'select count(*) from "{self.name}"'
        with self.db.engine.connect() as cnxn:
            return cnxn.execute(text(sql)).first()[0]

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

        if (not self._pkey.columns and self.db.engine.name == 'sqlite'):
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
               if rel.table == self.name][0]
        fkey = self.get_fkey(rel.name)

        return fkey

    @property
    def joins(self):
        """Return all joins to table as single string"""
        if hasattr(self, '_joins'):
            return self._joins
        joins = []
        aliases = []

        for key, fkey in self.fkeys.items():
            if fkey.referred_table not in self.db.tablenames:
                continue

            alias = fkey.constrained_columns[-1]

            # In seldom cases there might be two foreign keys ending
            # in same column
            if alias in aliases:
                alias = alias + '2'

            aliases.append(alias)

            # Get the ON statement in the join
            ons = [f'{alias}.{fkey.referred_columns[idx]} = '
                   f'{self.view}.{col}'
                   for idx, col in enumerate(fkey.constrained_columns)]
            on_list = ' AND '.join(ons)

            joins.append(f'left join {self.db.schema}.{fkey.referred_table} '
                         f'{alias} on {on_list}')

        self._joins = "\n".join(joins)

        if (self.name + '_grid') in self.db.tablenames:
            join_view = "join " + self.grid_view + " on "
            ons = [f'{self.grid_view}.{col} = {self.view}.{col}'
                   for col in self.pkey.columns]
            join_view += ' AND '.join(ons) + "\n"
        else:
            join_view = ""

        self._joins += "\n" + join_view

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

    def get_rel_tbl_names(self):
        tbl_names = []
        for rel in self.relations.values():
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
                if type(val) is str:
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

                if rel.schema == self.db.schema:
                    rel_db = self.db
                else:
                    rel_db = Database(self.db.engine, rel.schema, self.db.user.name)

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
        cols = self.db.refl.get_columns(self.name, self.db.schema)
        # contents = None if not self.db.cache \
        #     else self.db.cache.contents

        for col in cols:
            col = Dict(col)

            column = Column(self, col)
            field = Field(self, col.name)
            field.set_attrs_from_col(column)
            if hasattr(field, 'fkey'):
                condition, params = field.get_condition()
                field.options = field.get_options(condition, params)

            # Get info about column use if user has chosen this option
            if (
                self.db.config and self.db.config.column_use and
                col.name not in self.pkey.columns and
                not self.name.startswith('meta_')
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
            view_def = (self.db.refl
                        .get_view_definition(self.name, self.db.schema))

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

                sql = f"""
                select count(distinct({fkey_col})) from {relation.table}
                """

                with self.db.engine.connect() as cnxn:
                    count = cnxn.execute(text(sql)).first()[0]

                relations[name].use = count/self.rowcount if self.rowcount > 0 else 0

        self._relations = relations

    def export_ddl(self, dialect):
        """Return ddl for table"""
        ddl = f"create table {self.name} (\n"
        coldefs = []
        cols = self.db.refl.get_columns(self.name, self.db.schema)
        for col in cols:
            column = Column(self, col)
            coldef = column.get_def(dialect)
            coldefs.append(coldef)
        ddl += ",\n".join(coldefs)
        if (self.pkey.columns and self.pkey.columns != ['rowid']):
            ddl += f",\n    primary key ({', '.join(self.pkey.columns)})"

        for idx in self.indexes.values():
            if not idx.unique:
                continue
            if idx.columns == self.pkey.columns:
                continue

            # Only mysql has index within table namespace
            idx_name = idx.name
            if idx.name == '_'.join(idx.columns):
                idx_name = self.name + '_' + idx.name

            ddl += f",\n    constraint {idx_name} unique ("
            ddl += ",".join(idx.columns) + ")"

        for fkey in self.fkeys.values():
            ddl += ",\n    foreign key ("
            ddl += ", ".join(fkey.constrained_columns) + ") "
            ddl += f"references {fkey.referred_table}("
            ddl += ", ".join(fkey.referred_columns) + ")"
        ddl += ");\n\n"

        for idx in self.indexes.values():
            if idx.unique:
                continue
            ddl += "create "
            # Only mysql has index within table namespace
            idx_name = idx.name
            if idx.name == '_'.join(idx.columns):
                idx_name = self.name + '_' + idx.name
            ddl += f"index {idx_name} on {self.name} ("
            ddl += ",".join(idx.columns) + ");\n"

        return ddl

    def export_records(self, select_recs: bool, fkey: dict):
        """Export records as sql

        Parameters:
        select_recs: If records should be selected from existing database
        """
        insert = '\n'
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
            rows = cnxn.execute(text(sql)).mappings()

        if select_recs:
            insert += f'insert into {self.name}\n'
            insert += 'select ' + ', '.join(rows.keys())
            insert += f' from {self.db.schema}.{self.name};\n\n'
        else:
            for row in rows:
                insert += f'insert into {self.name} values ('
                for colname, val in row.items():
                    col = self.fields[colname]
                    if (self.name == 'meta_data' and colname == 'cache'):
                        val = ''
                    if type(val) is str:
                        val = "'" + val.replace("'", "''") + "'"
                    elif isinstance(val, datetime.date):
                        val = "'" + str(val) + "'"
                    elif (col.datatype == 'bool' and type(val) is int):
                        val = 'FALSE' if val == 0 else 'TRUE'
                    elif val is None:
                        val = 'null'
                    insert += str(val) + ','
                insert = insert[:-1] + ");\n"

        return insert

    def convert(self, colname, from_format, to_format):

        select = ', '.join(self.pkey.columns)

        sql = f"""
        select {select}, {colname}
        from {self.name}
        """

        with self.db.engine.connect() as cnxn:
            rows = cnxn.execute(text(sql)).mappings()
        for row in rows:
            if row[colname] is None:
                continue
            wheres = []
            params = {}
            for key in self.pkey.columns:
                wheres.append(key + '= :' + key)
                params[key] = row[key]

            where = ', '.join(wheres)

            try:
                value = pypandoc.convert_text(row[colname], to_format,
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

            with self.db.engine.connect() as conn:
                conn.execute(text(sql), params)
                conn.commit()

        return 'success'
