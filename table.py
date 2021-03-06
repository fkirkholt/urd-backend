"""Module for handling tables"""
import re
import math
import time
from addict import Dict
from record import Record
from column import Column
from expression import Expression

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
        self.label = db.get_label(tbl_name)
        self.cache = Dict()

        cursor = db.cnxn.cursor()
        tbl = cursor.tables(catalog=db.cat, schema=db.schema, table=tbl_name).fetchone()
        self.type_ = tbl.table_type.lower()


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
            rows = self.db.query(sql, [self.db.cnxn.user, self.name]).fetchall()
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
        sql = f"select count(*) from {self.name}"
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
        if not self.cache.get('foreign_keys', None):
            self.init_foreign_keys()

        return self.cache.foreign_keys

    def get_fkey(self, key):
        """Return single foreign key"""
        if not self.cache.get('foreign_keys', None):
            self.init_foreign_keys()

        return self.cache.foreign_keys[key]

    def get_fields(self):
        """Return all fields of table"""
        if not self.cache.get('fields', None):
            self.init_fields()

        return self.cache.fields


    @measure_time
    def get_primary_key(self):
        """Return primary key of table"""
        if self.db.cnxn.system == 'sqlite3':
            sql = self.db.expr.pkey(self.name)
            pkeys_cursor = self.db.query(sql)
        else:
            cursor = self.db.cnxn.cursor()
            pkeys_cursor = cursor.primaryKeys(table=self.name, catalog=self.db.cat,
                                            schema=self.db.schema)
        pkeys = [row.column_name for row in pkeys_cursor]

        if (len(pkeys) == 0 and self.db.system == 'sqlite3'):
            return ['rowid']

        return pkeys

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
        foreign_keys = self.get_fkeys()

        for key, fkey in foreign_keys.items():
            if fkey.table not in self.db.user_tables:
                continue

            alias = fkey.foreign[-1]

            # Get the ON statement in the join
            ons = [alias+'.'+fkey.primary[idx] + " = " + self.name + "." + col
                   for idx, col in enumerate(fkey.foreign)]
            on_list = ' AND '.join(ons)

            namespace = self.db.schema or self.db.cat
            joins.append(f"left join {namespace}.{fkey.table} {alias} on {on_list}")

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
                fkey = rel_table.get_fkey(key)
                for rel_rec in rel.records:
                    if 'values' not in rel_rec:
                        continue
                    for idx, col in enumerate(fkey.foreign):
                        pkcol = fkey.primary[idx]
                        rel_rec['values'][col] = record.get_value(pkcol)

                        # Primary keys of relation may be updated by
                        # cascade if primary keys of record is updated
                        if col in rel_rec.prim_key:
                            rel_rec.prim_key[col] = record.get_value(pkcol)

                rel_table.save(rel.records)

        return result

    @measure_time
    def init_foreign_keys(self):
        """Store foreign keys in table object"""
        if (self.db.metadata.get("cache", None) and not self.db.config):
            self.cache.foreign_keys = self.db.metadata.cache.tables[self.name].foreign_keys
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

        self.cache.foreign_keys = keys

    @measure_time
    def get_columns(self):
        """ Return all columns in table by reflection """
        cursor = self.db.cnxn.cursor()
        if self.db.cnxn.system == 'oracle':
            # cursor.columns doesn't work for all types of oracle columns
            sql = self.db.expr.columns()
            cols = cursor.execute(sql, self.db.schema, self.name).fetchall()
        else:
            cols = cursor.columns(table=self.name, catalog=self.db.cat,
                                  schema=self.db.schema).fetchall()

        return cols

    @measure_time
    def init_fields(self):
        """Store Dict of fields in table object"""
        if (self.db.metadata.get("cache", None) and not self.db.config):
            self.cache.fields = self.db.metadata.cache.tables[self.name].fields
            return
        fields = Dict()
        indexes = self.get_indexes()
        indexed_cols = []
        for key, index in indexes.items():
            indexed_cols.append(index.columns[0])
        pkey = self.get_primary_key()
        cols = self.get_columns()
        cursor = self.db.cnxn.cursor()
        contents = None if not self.db.metadata.cache else self.db.metadata.cache.contents

        for col in cols:
            colnames = [column[0] for column in col.cursor_description]
            col = Dict(zip(colnames, col))
            # Strip column size from type_name for sqlite3
            col.type_name = col.type_name.split('(')[0]
            if ('column_size' in col or 'display_size' in col):
                col.column_size = col.get('column_size', col.display_size)
            cname = col.column_name

            column = Column(self, cname)
            field = column.get_field(col)

            if (
                self.db.config and self.db.config.column_use and cname not in pkey and
                not self.name.startswith('meta_') and
                # table not in group named '...'
                (not contents['...'] or ('tables.' + self.name) not in contents['...'].subitems.values())
            ):
                if cname not in indexed_cols and col.type_name not in ['blob', 'clob', 'text']:
                    sql = f"""
                    create index {self.name}_{cname}_idx
                    on {self.name}({cname})
                    """

                    self.db.query(sql).commit()
                elif cname not in indexed_cols:
                    sql = f"""
                    create index {self.name}_{cname}_is_null_idx
                    on {self.name}({cname})
                    where {cname} is null
                    """

                    self.db.query(sql).commit()

                # Find if column is (largely) empty
                field.use = column.check_use()

                if col.type_name not in ['blob', 'clob', 'text']:
                    field.frequency = column.check_frequency()

            fields[cname] = field

        updated_idx = indexes.get(self.name + "_updated_idx", None)
        if updated_idx:
            for col in updated_idx.columns:
                fields[col].extra = "auto_update"
                fields[col].editable = False
        created_idx = indexes.get(self.name + "_created_idx", None)
        if created_idx:
            for col in created_idx.columns:
                fields[col].extra = "auto"
                fields[col].editable = False

        self.cache.fields = fields

    @measure_time
    def init_indexes(self):
        """Store Dict of indexes as attribute of table object"""
        if self.db.metadata.get("cache", None) and not self.db.config:
            self.cache.indexes= self.db.metadata.cache.tables[self.name].indexes
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
        if self.db.metadata.get("cache", None) and not self.db.config:
            self.cache.relations = self.db.metadata.cache.tables[self.name].relations
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
                    'base': row.fktable_cat or None,
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
        pkey = self.get_primary_key()
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
                default = col.default if not col.default_expr else col.default_expr
                if col.datatype in ['string', 'date'] and default != 'CURRENT_DATE':
                    coldef += " DEFAULT '" + default + "'"
                else:
                    coldef += " DEFAULT " + default
            coldefs.append(coldef)
        ddl += ",\n".join(coldefs)
        if pkey != ['rowid']:
            ddl += ",\n" + "    primary key (" + ", ".join(pkey) + ")"

        for fkey in self.get_fkeys().values():
            ddl += ",\n    foreign key (" + ", ".join(fkey.foreign) + ") references "
            ddl += fkey.table + "(" + ", ".join(fkey.primary) + ")"
        ddl += ");\n\n"

        for idx in self.get_indexes().values():
            if idx.columns == pkey:
                continue
            ddl += "create "
            if idx.unique:
                ddl += "unique "
            ddl += f"index {idx.name} on {self.name} (" + ",".join(idx.columns) + ");\n"

        return ddl

    def export_records(self):
        insert = ''
        sql = f"select * from {self.name}"
        cursor = self.db.cnxn.cursor()
        cursor.execute(sql)
        colnames = [column[0] for column in cursor.description]
        for row in cursor:
            insert += f'insert into {self.name} values ('
            for val in row:
                if type(val) is str:
                    val = "'" + str(val) + "'"
                elif val is None:
                    val = 'null'
                insert += str(val) + ','
            insert = insert[:-1] + ");\n"

        return insert
        

class Grid:
    """Contains methods for returning metadata and data for grid"""

    def __init__(self, table):
        self.tbl = table
        self.db = table.db
        self.user_filtered = False
        self.sort_columns = []
        self.cond = Dict({
            'prep_stmnts': [],
            'params': [],
            'stmnts': []
        })

    @measure_time
    def get(self, pkey_vals = None):
        """Return all metadata and data to display grid"""
        selects = {} # dict of select expressions
        pkey = self.tbl.get_primary_key()
        user_tables = self.db.get_user_tables()
        fields = self.tbl.get_fields()

        for col in pkey:
            selects[col] = self.tbl.name + '.' + col

        grid_columns = self.get_grid_columns()
        for colname in grid_columns:

            col = fields[colname]

            col.ref = self.tbl.name + '.' + colname

            if 'column_view' in col:
                selects[colname] = col.column_view
            elif col.element == 'textarea':
                selects[colname] = "substr(" + col.ref + ', 1, 255)'
            else:
                selects[colname] = col.ref

        expansion_column = self.get_expansion_column()
        if expansion_column:
            fkey = self.tbl.get_parent_fk()
            rel_column = fields[fkey.foreign[-1]]
            selects['count_children'] = self.select_children_count(fkey)

            # Filters on highest level if not filtered by user
            if (not self.user_filtered and len(self.cond.prep_stmnts) == 0):
                self.add_cond(self.tbl.name + '.' + rel_column.name, "IS NULL")

        actions = self.get_actions()

        if actions:
            for key, action in actions.items():
                if not action.disabled:
                    continue
                if isinstance(action.disabled, bool):
                    continue
                selects[key] = action.disabled

        values = self.get_values(selects)

        if (self.tbl.name + '_grid') in user_tables:
            view_name = self.tbl.name + '_grid'
            view = Table(self.db, view_name)
            cols = view.get_columns()
            display_columns = [view_name + '.' + column.column_name for column in cols]
            grid_columns = [col.column_name for col in cols]
            display_values = self.get_display_values_from_view(display_columns)
            view_fields = view.get_fields()
            print('view_fields: ', view_fields)
            for field_name, field in view_fields.items():
                if field_name not in fields:
                    field.virtual = True
                    field.table_name = view_name
                    fields[field_name] = field

        else:
            display_values = self.get_display_values(selects)

        recs = []
        for row in display_values:
            cols = {k: {'text': text} for k, text in row.items()}
            if 'count_children' in row:
                recs.append({
                    'count_children': row['count_children'],
                    'columns': cols
                })
                del row['count_children']
            else:
                recs.append({'columns': cols})


        for index, row in enumerate(values):
            for col, val in row.items():
                recs[index]['columns'][col]['value'] = val
            recs[index]['primary_key'] = {key: row[key] for key in pkey}

        row_formats = self.get_format()
        for idx, row in enumerate(row_formats.rows):
            classes = []
            for key, value in row.items():
                id_ = int(key[1:])
                if int(value):
                    classes.append(row_formats.formats[id_]['class'])
            class_ = " ".join(classes)
            recs[idx]['class'] = class_

        data = Dict({
            'name': self.tbl.name,
            'type' : self.tbl.type_,
            'records': recs,
            'count_records': self.get_rowcount(),
            'fields': fields,
            'grid': {
                'columns': grid_columns,
                'sums': self.get_sums(),
                'sort_columns': self.sort_columns,
                'actions': ["show_file"] if "show_file" in actions else []
            },
            'form': self.get_form(),
            'privilege': self.tbl.user_privileges(),
            'hidden': self.tbl.is_hidden(),
            'primary_key': pkey,
            'foreign_keys': self.tbl.get_fkeys(),
            'label': self.db.get_label(self.tbl.name),
            'actions': actions,
            'limit': self.tbl.limit,
            'offset': self.tbl.offset,
            'selection': self.get_selected_idx(pkey_vals, selects),
            'conditions': self.cond.stmnts,
            'expansion_column': expansion_column,
            'relations': self.tbl.get_relations(),
            'saved_filters': [] # Needed in frontend
        })

        return data

    def get_selected_idx(self, pkey_vals, selects):
        """Return rowindex for record selected in frontend"""
        if not pkey_vals:
            return None

        prep_stmnts = []
        params = []
        for colname, value in pkey_vals.items():
            prep_stmnts.append(f"{colname} = ?")
            params.append(value)

        # rec_conds = [f"{colname} = '{value}'" for colname, value in pkey_vals.items()]
        rec_cond = " WHERE " + " AND ".join(prep_stmnts)
        join = self.tbl.get_join()

        cond = ''
        if len(self.cond.prep_stmnts):
            cond = "WHERE " + " AND ".join(self.cond.prep_stmnts)

        order_by = self.make_order_by()

        sql = f"""
        select rownum - 1
        from   (select row_number() over ({order_by}) as rownum,
                       {self.tbl.name}.*
                from   {self.tbl.name}
                {join}
                {cond}) tab
        {rec_cond};
        """

        params = self.cond.params + params
        idx = self.db.query(sql, params).fetchval()
        if idx is not None:
            page_nr = math.floor(idx / self.tbl.limit)
            self.tbl.offset = page_nr * self.tbl.limit
            row_idx = idx - self.tbl.offset
        else:
            row_idx = 0

        return row_idx

    def select_children_count(self, fkey):
        """ number of relations to same table for expanding row"""
        wheres = []

        for idx, colname in enumerate(fkey.foreign):
            primary = fkey.primary[idx]
            wheres.append(colname + ' = ' + self.tbl.name + '.' + primary)

        where = ' and '.join(wheres)

        return f"""(
            select count(*)
            from {self.db.schema or self.db.cat}.{self.tbl.name} child_table
            where {where}
            )"""

    def get_expansion_column(self):
        """Return column that should expand a hierarchic table"""
        self_relation = False
        for rel in self.tbl.get_relations().values():
            if rel.table == self.tbl.name:
                self_relation = True
                break

        if not self_relation:
            return None

        ident_cols = None
        for idx in self.tbl.get_indexes().values():
            if idx.columns != self.tbl.get_primary_key() and idx.unique:
                ident_cols = idx.columns
                if idx.name.endswith("_sort_idx"):
                    break

        if not ident_cols:
            return None

        ident_col = None
        fields = self.tbl.get_fields()
        for colname in ident_cols:
            col = fields[colname]
            if col.datatype == 'string':
                ident_col = colname

        return ident_col

    def get_actions(self):
        # Make action for displaying files
        indexes = self.tbl.get_indexes()
        filepath_idx = indexes.get(self.tbl.name.lower() + "_filepath_idx", None)
        actions = Dict()
        if filepath_idx:
            last_col = filepath_idx.columns[-1]

            action = Dict({
                'label': self.db.get_label('show_file'),
                'url': "/file",
                'icon': "external-link",
                'communication': "download",
                'disabled': f"({last_col} is null)",
            })

            actions.show_file = action

        return actions

    def get_grid_columns(self):
        """Return columns belonging to grid"""
        indexes = self.tbl.get_indexes()
        grid_idx = indexes.get(self.tbl.name.lower() + "_grid_idx", None)
        if grid_idx:
            columns = grid_idx.columns
        else:
            pkey = self.tbl.get_primary_key()
            fkeys = self.tbl.get_fkeys()
            hidden = self.tbl.is_hidden()
            columns = []
            for key, field in self.tbl.get_fields().items():
                # Don't show hdden columns
                if field.name[0:1] == '_' or field.name[0:6].lower() == 'const_':
                    continue
                if field.size and field.size >= 255:
                    continue
                if field.datatype == 'json':
                    continue
                if ([field.name] == pkey and field.datatype == "integer"
                    and field.name not in fkeys
                    and hidden is False):
                    continue
                columns.append(key)
                if len(columns) == 5:
                    break

        return columns

    def make_order_by(self):
        """Return 'order by'-clause"""
        pkey = self.tbl.get_primary_key()

        order_by = "order by "
        sort_fields = Dict()
        if len(self.sort_columns) == 0:
            self.sort_columns = self.get_sort_columns()
        for sort in self.sort_columns:
            # Split into field and sort order
            parts = sort.split(' ')
            key = parts[0]
            direction = 'asc' if len(parts) == 1 else parts[1]
            if key in self.tbl.get_fields():
                tbl_name = self.tbl.name
            else:
                tbl_name = self.tbl.name + '_grid'
            sort_fields[key].field = tbl_name + "." + key
            sort_fields[key].order = direction

        if (len(pkey) == 0 and len(sort_fields) == 0):
            return ""

        for sort in sort_fields.values():
            if self.db.cnxn.system == 'mysql':
                order_by += f"isnull({sort.field}), {sort.field} {sort.order}, "
            elif self.db.cnxn.system in ['oracle', 'postgres']:
                order_by += f"{sort.field} {sort.order}, "
            elif self.db.cnxn.system == 'sqlite3':
                order_by += f"{sort.field} is null, {sort.field} {sort.order}, "

        for field in pkey:
            order_by += f"{self.tbl.name}.{field}, "

        order_by = order_by[0:-2]

        if self.db.cnxn.system in ['oracle', 'postgres']:
            order_by += " nulls last"

        return order_by

    @measure_time
    def get_values(self, selects):
        """Return values for columns in grid"""
        cols = []
        fields = self.tbl.get_fields()
        for key in selects.keys():
            if (key in fields or key == 'rowid') and 'source' not in fields[key]:
                cols.append(self.tbl.name + '.' + key)

        select = ', '.join(cols)
        join = self.tbl.get_join()
        cond = self.get_cond_expr()
        order = self.make_order_by()

        user_tables = self.db.get_user_tables()
        if (self.tbl.name + '_grid') in user_tables:
            pkey = self.tbl.get_primary_key()
            join_view = "join " + self.tbl.name + "_grid using (" + ','.join(pkey) + ")\n"
        else:
            join_view = ""

        sql = "select " + select + "\n"
        sql+= "from " + (self.db.schema or self.db.cat) + "." + self.tbl.name + "\n"
        sql+= join + "\n"
        sql+= join_view
        sql+= "" if not cond else "where " + cond +"\n"
        sql+= order

        cursor = self.db.cnxn.cursor()
        cursor.execute(sql, self.cond.params)
        cursor.skip(self.tbl.offset)
        if self.tbl.limit:
            rows = cursor.fetchmany(self.tbl.limit)
        else:
            rows = cursor.fetchall()

        result = []
        colnames = [column[0] for column in cursor.description]
        for row in rows:
            result.append(dict(zip(colnames, row)))

        return result

    def get_rowcount(self):
        """Return rowcount for grid"""
        conds = self.get_cond_expr()
        join = self.tbl.get_join()
        namespace = self.db.schema or self.db.cat

        user_tables = self.db.get_user_tables()
        if (self.tbl.name + '_grid') in user_tables:
            pkey = self.tbl.get_primary_key()
            join_view = "join " + self.tbl.name + "_grid using (" + ','.join(pkey) + ")\n"
        else:
            join_view = ""

        sql  = "select count(*)\n"
        sql += f"from {namespace}.{self.tbl.name}\n"
        sql += join + "\n"
        sql += join_view
        sql += "" if not conds else f"where {conds}\n"

        cursor = self.db.cnxn.cursor()
        count = cursor.execute(sql, self.cond.params).fetchval()

        return count

    def get_display_values_from_view(self, selects):
        view_name = self.tbl.name + '_grid'
        pkeys = self.tbl.get_primary_key()
        order = self.make_order_by()
        conds = self.get_cond_expr()

        sql  = "select " + ', '.join(selects) + "\n"
        sql += "from " + view_name + "\n"
        sql += "join " + self.tbl.name + " using (" + ', '.join(pkeys) + ")\n"
        sql+= "" if not conds else "where " + conds + "\n"
        sql += order

        cursor = self.db.cnxn.cursor()
        cursor.execute(sql, self.cond.params)
        cursor.skip(self.tbl.offset)
        rows = cursor.fetchmany(self.tbl.limit)

        result = []
        colnames = [column[0] for column in cursor.description]
        for row in rows:
            result.append(dict(zip(colnames, row)))

        return result

    def get_display_values(self, selects):
        """Return display values for columns in grid"""

        order = self.make_order_by()
        join = self.tbl.get_join()
        conds = self.get_cond_expr()

        alias_selects = {}
        for key, value in selects.items():
            alias_selects[key] = value + ' as ' + key
        select = ', '.join(alias_selects.values())

        sql = "select " + select + "\n"
        sql+= "from " + (self.db.schema or self.db.cat) + "." + self.tbl.name + "\n"
        sql+= join + "\n"
        sql+= "" if not conds else "where " + conds + "\n"
        sql+= order

        cursor = self.db.cnxn.cursor()
        cursor.execute(sql, self.cond.params)
        cursor.skip(self.tbl.offset)
        rows = cursor.fetchmany(self.tbl.limit)

        result = []
        colnames = [column[0] for column in cursor.description]
        for row in rows:
            result.append(dict(zip(colnames, row)))

        return result

    def get_sums(self):
        """Return list of sums for summation columns"""
        sums = []

        cols = self.get_summation_columns()
        join = self.tbl.get_join()
        cond = self.get_cond_expr()
        params = self.cond.params

        if len(cols) > 0:
            selects = []
            for col in cols:
                selects.append(f"sum({self.tbl.name}.{col}) as {col}")
            select = ', '.join(selects)

            sql = "select " + select + "\n"
            sql+= f"from {self.tbl.name}\n"
            sql+= join + "\n"
            sql+= "" if not cond else "where " + cond

            cursor = self.db.cnxn.cursor()
            row = cursor.execute(sql, params).fetchone()
            cols = [col[0] for col in cursor.description]
            sums = dict(zip(cols, row))

        return sums

    def get_sort_columns(self):
        """Return columns for default sorting of grid"""
        indexes = self.tbl.get_indexes()
        sort_idx = indexes.get(self.tbl.name.lower() + "_sort_idx", None)
        grid_idx = indexes.get(self.tbl.name + "_grid_idx", None)
        if sort_idx:
            columns = sort_idx.columns
        elif grid_idx:
            columns = grid_idx.columns[0:3]
        else:
            columns = []

        return columns


    def get_summation_columns(self):
        """Return columns that should be summed"""
        indexes = self.tbl.get_indexes()
        sum_idx = indexes.get(self.tbl.name + "_summation_idx", None)

        return [] if not sum_idx else sum_idx.columns

    def set_search_cond(self, query):
        """Set search conditions for grid queries"""
        filters = query.split(" AND ")
        for fltr in filters:
            parts = re.split(r"\s*([=<>]|!=| IN| LIKE|NOT LIKE|IS NULL|IS NOT NULL)\s*", fltr, 2)
            if len(parts) == 1:
                # Simple search in any text field
                value = parts[0]
                case_sensitive = value.lower() != value
                value = '%' + value + "%"

                fields = self.tbl.get_fields()
                conds = []
                params = []
                for field in fields.values():
                    if field.foreign_key:
                        view = field.name if not field.view else field.view
                        if case_sensitive:
                            conds.append(f"{view} LIKE ?")
                        else:
                            conds.append(f"lower({view}) LIKE ?")
                        params.append(value)
                    elif field.datatype == "string":
                        if case_sensitive:
                            conds.append(f"{self.tbl.name}.{field.name} LIKE ?")
                        else:
                            conds.append(f"lower({self.tbl.name}.{field.name}) LIKE ?")
                        params.append(value)
                expr = "(" + " OR ".join(conds) + ")"
                self.add_cond(expr=expr, value=params)
            else:
                field = parts[0]
                if "." not in field:
                    if field in self.tbl.get_fields():
                        tbl_name = self.tbl.name
                    else:
                        tbl_name = self.tbl.name + '_grid'
                    field = tbl_name + "." + field
                operator = parts[1].strip()
                value = parts[2].replace("*", "%")
                case_sensitive = value.lower() != value
                if (not case_sensitive and value.lower() != value.upper()):
                    field = f"lower({field})"
                if operator == "IN":
                    value = value.strip().split(",")
                if value == "":
                    value = None
                self.add_cond(field, operator, value)

    def add_cond(self, expr, operator=None, value=None):
        """Add condition used in grid queries"""
        if value is None:
            if operator in ["IS NULL", "IS NOT NULL"]:
                self.cond.prep_stmnts.append(f"{expr} {operator}")
            elif operator == "=":
                self.cond.prep_stmnts.append(f"{expr} IS NULL")
            else:
                self.cond.prep_stmnts.append(expr)
        elif operator == "IN":
            marks = ",".join(['?' for val in value])
            self.cond.prep_stmnts.append(f"{expr} {operator} ({marks})")
            self.cond.params.extend(value)
            value = "('" + "','".join(str(value)) + "')"
            self.cond.stmnts.append(f"{expr} {operator} {value}")
        elif operator is None:
            self.cond.prep_stmnts.append(expr)
            if isinstance(value, list):
                self.cond.params.extend(value)
            else:
                self.cond.params.append(value)

        else:
            self.cond.prep_stmnts.append(f"{expr} {operator} ?")
            self.cond.params.append(value)
            self.cond.stmnts.append(f"{expr} {operator} {value}")

    def get_cond_expr(self):
        """Return expression with all query conditions"""
        return " and ".join(self.cond.prep_stmnts)

    def get_client_conditions(self):
        """Return all conditions visible for client"""
        return self.cond.stmnts

    def get_field_groups(self, fields):
        """Group fields according to first part of field name"""
        col_groups = Dict()
        for field in fields.values():
            # Don't add column to form if it's part of primary key but not shown in grid
            if (field.name in self.tbl.get_primary_key() and
                field.name not in self.get_grid_columns()
            ): field.hidden = True

            # Group by prefix
            parts = field.name.split("_")
            group = parts[0]

            # Don't add fields that start with _
            # They are treated as hidden fields
            if group == "":
                field.hidden = True
                continue

            if group not in col_groups:
                col_groups[group] = []

            col_groups[group].append(field.name)

        return col_groups

    def get_form(self):
        """Return form as Dict for displaying record"""

        form = Dict({'items': {}})
        fields = self.tbl.get_fields()
        field_groups = self.get_field_groups(fields)

        for group_name, col_names in field_groups.items():
            if len(col_names) == 1:
                label = self.db.get_label(col_names[0])
                form['items'][label] = col_names[0]
            else:
                inline = False
                subitems = Dict()
                sum_size = 0
                for colname in col_names:
                    # removes group name prefix from column name and use the rest as label
                    rest = colname.replace(group_name+"_", "")
                    label = self.db.get_label(rest)
                    subitems[label] = colname

                    field = fields[colname]
                    if 'size' in field:
                        sum_size += field.size
                    elif field.datatype in ["date", "integer"]:
                        sum_size += 10

                if sum_size <= 50:
                    inline = True

                group_label = self.db.get_label(group_name)

                form['items'][group_label] = Dict({
                    'inline': inline,
                    'items': subitems
                })

        form = self.relations_form(form)

        return form

    def get_format(self):

        if 'meta_format' not in self.db.user_tables:
            return Dict({
                'formats': [],
                'rows': []
            })

        sql = """
        select id, class, filter
        from   meta_format
        where  table_ = ?
        """

        cursor = self.db.cnxn.cursor()
        rows = cursor.execute(sql, self.tbl.name).fetchall()
        colnames = [column[0] for column in cursor.description]
        selects = []
        formats = {}
        for row in rows:
            selects.append("(" + row.filter + ") AS f" + str(row.id))
            formats[row.id] = dict(zip(colnames, row))

        if len(selects) == 0:
            return Dict({
                'formats': [],
                'rows': []
            })

        select = ", ".join(selects)
        join = self.tbl.get_join()
        conds = self.get_cond_expr()
        cond = "" if not conds else f"where {conds}\n"
        params = self.cond.params
        ordr = self.make_order_by()

        sql = f"""
        select {select}
        from {self.tbl.name}
        {join}
        {cond}
        {ordr}
        """

        cursor = self.db.cnxn.cursor()
        cursor.execute(sql, params)
        cursor.skip(self.tbl.offset)
        rows = cursor.fetchmany(self.tbl.limit)

        result = []
        colnames = [column[0] for column in cursor.description]
        for row in rows:
            result.append(dict(zip(colnames, row)))

        return Dict({
            'formats': formats,
            'rows': result
        })

    def relations_form(self, form):
        """Add relations to form"""
        relations = self.tbl.get_relations()
        rel_tbl_names = self.tbl.get_rel_tbl_names()

        for alias, rel in relations.items():
            rel.order = 10
            rel_table = Table(self.db, rel.table)
            name_parts = rel.table.split("_")

            if rel.table not in self.db.user_tables:
                rel.hidden = True

            # Find indexes that can be used to get relation
            index_exist = False
            slice_obj = slice(0, len(rel.foreign))
            rel_indexes = rel_table.get_indexes()
            for index in rel_indexes.values():
                if index.columns[slice_obj] == rel.foreign:
                    index_exist = True

            if index_exist and not rel.get('hidden', False):
                rel_pkey = rel_table.get_primary_key()

                if set(rel_pkey) > set(rel.foreign):
                    # Set order priority
                    rel.order = len(rel_pkey) - rel_pkey.index(rel.foreign[-1])

                rel.label = self.db.get_label(rel_table.name
                             .replace(self.tbl.name + '_', '')
                             .replace('_' + self.tbl.name, ''))

                # Add name of foreign key column if other than name of reference table
                if rel.foreign[-1] not in self.tbl.name:
                    rel.label += " (" + self.db.get_label(rel.foreign[-1]).lower() + ")"
            else:
                rel.hidden = True

            relations[alias] = rel

        sorted_rels = dict(sorted(relations.items(), key=lambda tup: tup[1].order))

        for alias, rel in sorted_rels.items():
            name_parts = rel.table.split("_")
            if (len(name_parts) > 1 and name_parts[0] in rel_tbl_names):
                continue
            if not rel.hidden:
                form['items'][rel.label] = "relations." + alias

        return form
