"""Module for handling tables"""
import pypandoc
import json
import os
from addict import Dict
import util
from settings import Settings, yaml
from models.record import Record
from models.column import Column
from models.field import Field
from models.grid import Grid
from models.expression import Expression

cfg = Settings()


class Table:
    """Contains methods for getting metadata for table"""

    def __init__(self, db, tbl_name, type=None, alias=None):
        self.state = db.state.tables[tbl_name]
        self.db = db
        self.name = tbl_name
        self.label = db.get_label(tbl_name)
        self.view = tbl_name
        self.main_type = type
        if tbl_name + '_view' in db.tablenames:
            cols = self.db.refl.columns(self.db.schema, tbl_name + '_view')
            colnames = [col['name'] for col in cols]
            if set(colnames) >= set(self.pkey.columns):
                self.view = tbl_name + '_view'
        self.grid_view = self.view
        if tbl_name + '_grid' in db.tablenames:
            cols = self.db.refl.columns(self.db.schema, tbl_name + '_grid')
            colnames = [col['name'] for col in cols]
            if set(colnames) >= set(self.pkey.columns):
                self.grid_view = tbl_name + '_grid'
        self.alias = alias or self.view
        self.fts = False

    def get(self):
        grid = Grid(self)

        view_names = self.db.viewnames

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

        return Dict({
            'name': self.name,
            'type': self.type,
            'view': view,
            'icon': None,
            'label': self.db.get_label(self.name),
            'rowcount': (None if not self.db.config.update_cache
                         else self.rowcount),
            'pkey': self.pkey,
            'comment': self.comment,
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
    def comment(self):
        if not self.state.comment:
            metadata = self.db.refl.tables(self.db.schema, table=self.name)
            self.state.comment = metadata.comment
        return self.state.comment

    @property
    def type(self):
        """Return type of table. One of 'table', 'list', 'xref', 'ext', 'view' """
        if not self.state.type:

            if self.main_type is None:
                tbl_names = self.db.tablenames
                self.main_type = 'table' if self.name in tbl_names else 'view'

            if self.main_type == 'view':
                self.state.type = 'view'
                return 'view'

            list_idx = self.indexes.get(self.name.rstrip('_') + "_list_idx", None)
            if list_idx:
                self.state.type = 'list'
                return 'list'

            else:
                self.state.type = 'data'

            all_fkey_columns = set()
            for fkey in self.fkeys.values():
                # An fkey with same columns as primary key designates an
                # extension table. But the fkey can also have invisible columns
                # used to control if the table should be displayed as relation
                cols = [col for col in fkey.constrained_columns
                        if not (col.startswith('_') or col.startswith('const_'))]
                if cols == self.pkey.columns:
                    self.state.type = "ext"
                    break
                elif set(cols) < set(self.pkey.columns):
                    all_fkey_columns.update(set(cols))

            if self.pkey and set(self.pkey.columns) <= all_fkey_columns:
                self.state.type = "xref"

        return self.state.type

    @property
    def rowcount(self):
        if not hasattr(self, '_rowcount'):
            sql = f'select count(*) from {self.db.schema}.{self.name}'
            with self.db.cnxn.cursor() as crsr:
                crsr.execute(sql)
                self._rowcount = crsr.fetchone()[0]

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
        if not self.state.indexes:
            self.db.indexes
            self.state.indexes = self.db.indexes[self.name]
        return self.state.indexes

    @property
    def fkeys(self):
        if not self.state.fkeys:
            self.db.fkeys
            self.state.fkeys = self.db.fkeys[self.name]
        return self.state.fkeys

    def get_fkey(self, name):
        """Return single foreign key based on key name or last column"""
        if not self.state.fkeys:
            self.state.fkeys = self.db.fkeys[self.name]

        if name in self.state.fkeys:
            return self.state.fkeys[name]
        else:
            col_fkey = None
            for fkey in self.state.fkeys.values():
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
        if not self.state.fields:
            fields = Dict()
            indexed_cols = []

            if self.db.state.columns:
                cols = self.db.state.columns[self.name]
            else:
                cols = self.db.refl.columns(self.db.schema, self.name)

            for col in cols:
                col = Dict(col)

                column = Column(self, col)
                field = Field(self, col.name)
                field.set_attrs_from_col(column)
                if hasattr(field, 'fkey'):
                    field.options = field.get_options('', {})

                if (
                    field.name in indexed_cols and
                    not getattr(field, 'options', False) and
                    field.datatype == 'str' and
                    field.attrs.get('data-format', None) != 'ISO 8601'
                ):
                    # Make the field show up as autocomplete
                    field.attrs['type'] = 'search'
                    field.element = 'input'

                # Get info about column use if user has chosen this option
                if (
                    self.db.config and self.db.config.column_use and
                    col.name not in self.pkey.columns and
                    not self.name.startswith('meta_') and
                    self.type != 'view'
                ):
                    if col.name not in indexed_cols:
                        column.create_index(col.type_name)

                    # Find if column is (largely) empty
                    field.use = column.check_use()

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

            self.state.fields = fields

        return self.state.fields

    @property
    def pkey(self):
        """Return primary key of table"""
        if self.state.pkey:
            return self.state.pkey
        if (self.db.cache and not self.db.config.update_cache):
            self._pkey = self.db.cache.tables[self.name].pkey
            return self._pkey

        if self.db.pkeys:
            self.state.pkey = self.db.pkeys[self.name]
        else:
            pkey = self.db.refl.pkeys(self.db.schema, self.name)
            self.state.pkey = Dict({
                'table_name': self.name,
                'name': pkey['name'] or 'PRIMARY',
                'unique': True,
                'columns': pkey['constrained_columns']
            })

        if (
            not self.state.pkey.columns and
            self.db.engine.name == 'sqlite' and
            self.type != 'view'
        ):
            self.state.pkey.columns = ['rowid']
            self.state.pkey.name = self.name + '_rowid'

        if (not self.state.pkey.columns):
            attrs = self.db.state.html_attrs
            selector = f'table[data-name="{self.name}"]'
            if attrs[selector]['data-pkey']:
                self._pkey.name = self.name + '_pkey'
                self._pkey.columns = attrs[selector]['data-pkey']

        return self.state.pkey

    @property
    def columns(self):
        if self.state.columns:
            return self.state.columns
        if self.db.state.columns:
            self.state.columns = self.db.state.columns[self.name]
        else:
            self.state.columns = self.db.refl.columns(self.db.schema, self.name)

        return self.state.columns

    def get_parent_fk(self):
        """Return foreign key defining hierarchy"""
        fkey = None
        # Find relation to child records
        for rel in self.relations.values():
            if rel.table_name == self.name:
                fkey = self.get_fkey(rel.name)

        return fkey

    @property
    def joins(self):
        """Return all joins to table as single string"""
        q = Expression(self.db.engine).quote
        if hasattr(self, '_joins'):
            return self._joins
        joins = {}

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
            ons = [f'{q(fkey.ref_table_alias)}.{q(fkey.referred_columns[idx])} = '
                   f'{q(self.alias)}.{q(col)}'
                   for idx, col in enumerate(fkey.constrained_columns)]
            on_list = ' AND '.join(ons)

            joins[fkey.name] = (f'left join {self.db.schema}.{q(fkey.referred_table)} '
                                f'{q(fkey.ref_table_alias)} on {on_list}')

            # Join with 1:1 relation carrying access code
            fkey_table = Table(self.db, fkey.referred_table)
            access_idx = fkey_table.get_access_code_idx()
            if access_idx and access_idx.table_name != fkey_table.name:
                for key, fkey in fkey_table.relations.items():
                    if fkey.table_name == access_idx.table_name:
                        if fkey.table_name == self.name:
                            continue
                        ons = [f"{fkey.table_name}.{fkey.constrained_columns[idx]} = "
                               f"{fkey.ref_table_alias}.{col}"
                               for idx, col in enumerate(fkey.referred_columns)]
                        on = ' AND '.join(ons)
                        joins[fkey.name] = (
                            f"left join {self.db.schema}.{fkey.table_name} on {on}"
                        )

        for key, fkey in self.relations.items():
            if fkey.relationship == '1:1':
                prefix = fkey.referred_table.rstrip('_') + '_'
                alias = fkey.table_name.replace(prefix, '')
                ons = [f"{alias}.{q(fkey.constrained_columns[idx])} = "
                       f"{q(self.view)}.{q(col)}"
                       for idx, col in enumerate(fkey.referred_columns)]
                on = ' AND '.join(ons)
                joins[fkey.name] = (
                    f"left join {self.db.schema}.{q(fkey.table_name)} {alias} on {on}"
                )

                rel_tbl = Table(self.db, fkey.table_name, alias=alias)
                for fkey_name in rel_tbl.joins:
                    # Don't add the join defining the 1:1 relation
                    fkey = rel_tbl.fkeys[fkey_name]
                    if (
                        fkey and fkey.referred_table != self.name
                        and fkey.name not in joins
                    ):
                        joins[fkey.name] = rel_tbl.joins[fkey.name]

        if self.grid_view != self.name and self.grid_view in self.db.tablenames:
            join_view = "join " + self.grid_view + " on "
            ons = [f'{q(self.grid_view)}.{q(col)} = {q(self.view)}.{q(col)}'
                   for col in self.pkey.columns]
            join_view += ' AND '.join(ons) + "\n"

            joins[self.grid_view] = join_view

        if hasattr(self, 'fts_table'):
            join = f"join {self.fts_table} fts on fts.rowid = {self.name}.rowid\n"
            joins[self.fts_table] = join
        if hasattr(self, 'vec_table'):
            ons = [ f"vec.{colname} = {self.name}.{colname}"
                    for colname in self.pkey.columns ]
            on = ' and '.join(ons)
            pkey_cols_str = ', '.join(self.pkey.columns)
            joins[self.vec_table] = f"""
            join (SELECT {pkey_cols_str},
                   v.best_distance as distance
            FROM (
                SELECT {pkey_cols_str},
                       MIN(vec.distance) AS best_distance
                FROM {self.vec_table} vt
                JOIN vector_quantize_scan(
                    '{self.vec_table}',
                    'vector',
                    :query_vec,
                    :limit
                ) vec ON vec.rowid = vt.rowid
                where vec.distance < 0.05
                GROUP BY {pkey_cols_str}
            ) v) as vec on {on}
            """

        self._joins = joins

        return self._joins

    def get_relation(self, alias):
        """Return single relation"""
        if not self.state.relations:
            self.db.relations
            self.state.relations = self.db.relations[self.name]
        return self.state.relations[alias]

    @property
    def relations(self):
        """Return all 'has many' relations of table"""
        if not self.state.relations:
            self.db.relations
            self.state.relations = self.db.relations[self.name]
        return self.state.relations

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

    def save(self, records: list):
        """Save new and updated records in table"""
        from models.database import Database
        result = Dict()
        for rec in records:
            rec = Dict(rec)
            record = Record(self.db, self, rec.prim_key)
            if rec.method == 'delete' and rec.prim_key:
                msg = record.delete()
                if msg != 'success':
                    result.msg = msg
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

    def export_ddl(self, dialect, no_fkeys, no_empty, count_recs):
        """Return ddl for table"""
        ddl = "\n"
        if self.type == 'view':
            ddl += "-- view exported as table\n"
        ddl += f"create table {self.name}"
        if self.comment:
            ddl += '  -- ' + self.comment
        ddl += "\n(\n"
        coldefs = []
        comments = []
        cols = self.columns
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

            if dialect == 'duckdb' and datatype == 'str' and column.size:
                data = json.dumps({ "maxlength": column.size })
                comments.append(f"COMMENT ON COLUMN {self.name}.{col.name} IS '{data}'")

            if datatype == 'bytes':
                self.indexes[f'{self.name}_{column.name}_filepath_idx'] = Dict({
                    'name': f'{self.name}_{column.name}_filepath_idx',
                    'columns': [column.name],
                    'unique': False
                })
        ddl += "\n".join(coldefs)
        ddl = ddl[:-1]
        if (self.pkey.columns and self.pkey.columns != ['rowid']):
            ddl += f",\n    primary key ({', '.join(self.pkey.columns)})"

        if not no_fkeys:
            for fkey in self.fkeys.values():
                if no_empty and count_recs[fkey.referred_table] == 0:
                    continue
                ddl += ",\n    foreign key ("
                ddl += ", ".join(fkey.constrained_columns) + ") "
                ddl += f"references {fkey.referred_table}("
                ddl += ", ".join(fkey.referred_columns) + ")"
        ddl += "\n);\n\n"

        if comments:
            ddl += "\n".join(comments) + ';\n\n'

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

    def convert(self, colname, from_format, to_format):

        select = ', '.join(self.pkey.columns)

        sql = f"""
        select {select}, {colname}
        from {self.name}
        """

        with self.db.cnxn.cursor() as crsr:
            crsr.execute(sql)
            rows = crsr.fetchall()
            for row in rows:
                rec = util.to_rec(row, crsr)
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

            with self.db.cnxn.cursor() as crsr:
                sql, params = self.db.expr.prepare(sql, params)
                crsr.execute(sql, params)
                self.db.cnxn.commit()

        return 'success'


    def make_embeddings(self):
        BATCH_SIZE = 64
        model_dir = os.path.expanduser(cfg.gguf_model_dir)
        model_path = os.path.join(model_dir, cfg.gguf_model)

        if not os.path.exists(model_path):
            return "model missing"

        if not self.db.cnxn.model_loaded:
            self.db.cnxn.load_model(model_path)

        # Find columns for vector search or fulltext search
        cols = {col.name: col for col in self.columns}
        vector_cols = Dict()
        fts_cols = []
        for colname, col in cols.items():
            col.attrs = Dict()
            coltype = col.type.lower()
            if col.comment:
                try:
                    comment = yaml.load(col.comment)
                    col.attrs = Dict(comment)
                except (ValueError, TypeError):
                    col.attrs['title'] = col.comment
            if (coltype == 'text' or coltype.startswith('varchar') and
                'fulltext' in self.comment):
                fts_cols.append(col)
            if coltype == 'text' and 'vector' in self.comment:
                vector_cols[colname] = col
            if 'vector' in col.attrs.get('data-search-mode', ''):
                vector_cols[colname] = col
            if 'fulltext' in col.attrs.get('data-search-mode', ''):
                fts_cols.append(col)

        # Create fulltext index
        fts_name = f"_{self.name.strip()}_fts"
        if len(fts_cols):
            fts_cols = self.pkey.columns + [ col.name for col in fts_cols ]
            col_string = ", ".join(fts_cols)
            sql_drop = f"DROP TABLE IF EXISTS {fts_name};"
            sql_create = f"CREATE VIRTUAL TABLE {fts_name} USING fts5({col_string}"
            sql_create += f", content='{self.name}', content_rowid='{", ".join(self.pkey.columns)}');"
            sql_insert = f"INSERT INTO {fts_name} (rowid, {col_string})\n"
            sql_insert += f"SELECT rowid, {col_string} FROM {self.name};"
            with self.db.cnxn.cursor() as crsr:
                crsr.execute(sql_drop)
                crsr.execute(sql_create)
                crsr.execute(sql_insert)
            self.db.cnxn.commit()

        # Create table for vector search
        vec_tbl_name = f"_{self.name.strip()}_vector"
        if len(vector_cols) and vec_tbl_name in self.db.tablenames:
            pkeydefs = [ colname + ' ' + cols[colname].type
                         for colname in self.pkey.columns ]
            sql_drop = f"DROP TABLE IF EXISTS {vec_tbl_name};"
            sql_create = f"CREATE TABLE {vec_tbl_name} -- data-model: {cfg.gguf_model}"
            sql_create += "\n(\n"
            sql_create += ",\n".join(pkeydefs) + ',\nchunk integer,'
            sql_create += "\n column_name varchar(100),\nposition_start integer,"
            sql_create += "\nposition_end integer,\nvector blob,"
            sql_create += f"foreign key ({', '.join(self.pkey.columns)}) references "
            sql_create += f" {self.name} ({', '.join(self.pkey.columns)})"
            sql_create += "\n);"
            with self.db.cnxn.cursor() as crsr:
                crsr.execute(sql_drop)
                crsr.execute(sql_create)
                crsr.execute("SELECT llm_embed_generate('auto_detect_dimensions');")
                test_blob = crsr.fetchone()[0]
                dimension = len(test_blob) // 4
            self.db.cnxn.commit()

        with self.db.cnxn.cursor() as crsr:
            # Initialize embedding column
            init_query = f"""
                SELECT vector_init(
                    '{vec_tbl_name}',
                    'vector',
                    'type=FLOAT32,dimension={dimension},distance=COSINE'
                );
            """
            crsr.execute(init_query)
            self.db.cnxn.commit()

        # Create embeddings in vector columns
        for col in vector_cols.values():
            with self.db.cnxn.cursor() as crsr:

                ons = [ f"v.{colname} = s.{colname}" for colname in self.pkey.columns ]

                sql = f"""SELECT s.{', s.'.join(self.pkey.columns)}, s.{col.name}
                     FROM {self.name} s
                     LEFT JOIN {vec_tbl_name} v ON {' AND '.join(ons)}
                               and column_name = '{col.name}'
                     WHERE v.vector IS NULL and s.{col.name} is not null"""
                crsr.execute(sql)
                rows = crsr.fetchall()
                description = crsr.description

                for i in range(0, len(rows), BATCH_SIZE):
                    batch = rows[i:i + BATCH_SIZE]

                    print(f"Processing row {i} to {i + len(batch)} via sqlite-ai...")

                    for row in batch:
                        cols = [col[0] for col in description]
                        rec = Dict(zip(cols, row))

                        text = rec[col.name]

                        chunks = util.chunk_text_with_positions(text)
                        pkey_vals = [str(rec[colname]) for colname in self.pkey.columns]

                        for idx, chunk in enumerate(chunks):
                            # Generate embedding and insert into vector table
                            sql = f"""
                                INSERT INTO {vec_tbl_name}
                                VALUES (
                                    {', '.join(pkey_vals)},
                                    {idx},
                                    '{col.name}',
                                    {chunk.start_pos},
                                    {chunk.end_pos},
                                    llm_embed_generate(?)
                                )
                            """
                            crsr.execute(sql, (chunk.text, ))

                    # Commit per batch
                    self.db.cnxn.commit()

                crsr.execute(f"SELECT vector_quantize('{vec_tbl_name}', 'vector');")

        return "Finished! All embeddings are generated and saved."
