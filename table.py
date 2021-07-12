import simplejson as json
from addict import Dict
from record import Record
from column import Column
from expression import Expression
import re

class Table:
    def __init__(self, db, tbl_name):
        self.db = db
        self.name = tbl_name
        self.cat = self.db.cat
        self.schema = self.db.schema
        self.label = db.get_label(tbl_name)
        self.offset = 0
        self.limit = 30
        self.conditions = []
        self.params = []
        self.client_conditions = []
        self.user_filtered = False
        self.join = None

    def get_view(self):
        if not hasattr(self, 'view'):
            self.init_view()

        return self.view

    def get_type(self):
        CASCADE = 0
        RESTRICT = 1
        SET_NULL = 2
        NO_ACTION = 3
        SET_DEFAULT = 4

        type_ = 'data'

        # Only databases with metadata table are expected to follow these rules
        if len(self.db.metadata):
            relations = self.get_relations()
            for rel in relations.values():
                if rel.delete_rule in [RESTRICT, NO_ACTION]:
                    type_ = 'reference'
            if self.name.startswith("meta_"):
                type_ = 'reference'

        return type_

    def get_db_table(self, base, table):
        from database import Database
        db = Database(self.db.cnxn, base)
        tbl = Table(db, table)

        return tbl

    def get_indexes(self):
        if not hasattr(self, 'indexes'):
            self.init_indexes()

        return self.indexes

    def get_fkeys(self):
        if not hasattr(self, 'foreign_keys'):
            self.init_foreign_keys()

        return self.foreign_keys

    def get_fkey(self, key):
        if not hasattr(self, 'foreign_keys'):
            self.init_foreign_keys()

        return self.foreign_keys[key]

    def get_fields(self):
        if not hasattr(self, 'fields'):
            self.init_fields()

        return self.fields

    def get_values(self, selects, order):
        #TODO: hent join selv, og kanskje flere
        cols = []
        fields = self.get_fields()
        for key in selects.keys():
            if key in fields and 'source' not in fields[key]:
                cols.append(self.name + '.' + key)

        select = ', '.join(cols)
        join = self.get_join()
        cond = self.get_conds()
        params = self.params

        sql = "select " + select
        sql+= "  from " + (self.schema or self.cat) + "." + self.name
        sql+= " " + join + "\n"
        sql+= "" if not cond else "where " + cond +"\n"
        sql+= order

        cursor = self.db.cnxn.cursor()
        cursor.execute(sql, params)
        cursor.skip(self.offset)
        rows = cursor.fetchmany(self.limit)

        result = []
        colnames = [column[0] for column in cursor.description]
        for row in rows:
            result.append(dict(zip(colnames, row)))

        return result

    def get_rowcount(self):
        conds = self.get_conds()
        join = self.get_join()

        sql  = "select count(*)\n"
        sql += f"from {self.schema or self.cat}.{self.name}\n"
        sql += join + "\n"
        sql += "" if not conds else f"where {conds}\n"

        cursor = self.db.cnxn.cursor()
        count = cursor.execute(sql, self.params).fetchval()

        return count

    def get_display_values(self, selects, order):
        for key, value in selects.items():
            selects[key] = value + ' as ' + key
        
        select = ', '.join(selects.values())
        join = self.get_join()
        conds = self.get_conds()
        params = self.params


        sql = "select " + select
        sql+= "  from " + (self.schema or self.cat) + "." + self.name
        sql+= " " + join + "\n" 
        sql+= "" if not conds else "where " + conds + "\n"
        sql+= order

        cursor = self.db.cnxn.cursor()
        cursor.execute(sql, params)
        cursor.skip(self.offset)
        rows = cursor.fetchmany(self.limit)

        #TODO: Vurder å legge det under til en funksjon
        result = []
        colnames = [column[0] for column in cursor.description]
        for row in rows:
            result.append(dict(zip(colnames, row)))

        return result

    def get_primary_key(self):
        cursor = self.db.cnxn.cursor()
        pkeys = cursor.primaryKeys(table=self.name, catalog=self.cat,
                                   schema=self.schema)
        return [row.column_name.lower() for row in pkeys]

    def get_grid_columns(self):
        indexes = self.get_indexes()
        grid_idx = indexes.get(self.name.lower() + "_grid_idx", None)
        if grid_idx:
            columns = [col.lower() for col in grid_idx.columns]
        else:
            columns = []
            for key, field in self.get_fields().items():
                # Don't show hdden columns
                if field.name[0:1] == '_': continue
                #TODO: Don't show autoinc columns
                columns.append(key)
                if len(columns) == 5: break
        
        return columns

    def get_sort_columns(self):
        indexes = self.get_indexes()
        sort_idx = indexes.get(self.name.lower() + "_sort_idx", None)
        grid_idx = indexes.get(self.name + "_grid_idx", None)
        if sort_idx:
            columns = sort_idx.columns
        elif grid_idx:
            columns = grid_idx.columns[0:3]
        else:
            columns = []

        return columns

    def get_summation_columns(self):
        indexes = self.get_indexes()
        sum_idx = indexes.get(self.name + "_summation_idx", None)
        
        return [] if not sum_idx else sum_idx.columns


    def get_grid(self):
        selects = {} # dict of select expressions
            #TODO: Behøver selects å være dict? Kan det ikke være list? Det forenkler vel koden litt.
        
        pkey = self.get_primary_key()
        foreign_keys = self.get_fkeys()
        fields = self.get_fields()

        grid = Dict({
            'columns': self.get_grid_columns(),
            'sort_columns': self.get_sort_columns(),
            'summation_columns': self.get_summation_columns()
        })

        for col in pkey:
            selects[col] = self.name + '.' + col

        for colname in grid.columns:

            col = fields[colname]

            col.ref = self.name + '.' + colname

            if 'column_view' in col:
                selects[colname] = col.column_view
            elif col.element == 'textarea':
                selects[colname] = "substr(" + col.ref + ', 1, 255)'
            else:
                selects[colname] = col.ref

        if hasattr(self, 'expansion_column'):
            # Get number of relations to same table for expanding row
            fk = self.get_parent_fk()
            rel_column = fields[fk.alias]
            wheres = []

            for idx, colname in enumerate(fk.foreign):
                foreign = fk.foreign[idx]
                wheres.append(colname + ' = ' + self.name + '.' + foreign)

            where = ' and '.join(wheres)
            selects['count_children'] = f"""(
                select count(*)
                from {self.db.name}.{self.name} child_table
                where {where}
                )"""

            # Filters on highest level if not filtered by user
            if self.user_filtered == False:
                expr = self.name + '.' + rel_column.name
                val = rel_column.get('default', None)
                self.add_cond(expr, "=", val)


        #TODO: Make select to get disabled status for actions
        #TODO: Find selected index

        order_by = self.make_order_by(selects)


        display_values = self.get_display_values(selects, order_by)
        values = self.get_values(selects, order_by)

        recs = []
        for row in display_values:
            recs.append({'columns': row})
        
        for index, row in enumerate(values):
            recs[index]['values'] = row
            recs[index]['primary_key'] = {key: row[key] for key in pkey}
        #TODO: row formats

        sums = self.get_sums()

        #TODO: Don't let fields be reference to fields
        #TODO: Burde ikke være nødvendig
        fields = json.loads(json.dumps(fields))

        #TODO: replace field.name with field.alias

        form = self.get_form()

        data = Dict({
            'name': self.name,
            'records': recs,
            'count_records': self.get_rowcount(),
            'fields': fields,
            'grid': {
                'columns': grid.columns,
                'sums': sums,
                'sort_columns': grid.sort_columns
            },
            'form': { #TODO:  kun ett attributt
                'items': None if 'items' not in form else form['items']
            },
            'permission': { #TODO: hent fra funksjon
                'view': 1,
                'add': 1,
                'edit': 1,
                'delete': 1
            },
            'type': self.get_type(),
            'primary_key': pkey,
            'foreign_keys': foreign_keys,
            'label': self.db.get_label(self.name),
            'actions': getattr(self, 'actions', []),
            'limit': self.limit,
            'offset': self.offset,
            'selection': 0, #TODO: row_idx
            'conditions': [], #TODO: self.client_conditions,
            'date_as_string': {'separator': '-'}, #TODO wtf
            'expansion_column': None, #TODO
            'relations': self.get_ref_relations(),
            'saved_filters': [] #TODO: self.get_saved_filters()
        })

        return data

    def get_parent_fk(self):
        # Find relation to child records
        relations = self.get_relations()
        rel = [rel for rel in list(relations) if rel.name == self.name][0]

        foreign_keys = self.get_fkeys()
        fk = foreign_keys[rel.foreign_key]
        fk.alias = rel.foreign_key

        return fk

    def get_join(self):
        if self.join is not None:
            return self.join
        joins = []
        foreign_keys = self.get_fkeys()
        fields = self.get_fields()
        for key, fk in foreign_keys.items():
            if key not in fields:
                continue

            table = self.get_db_table(fk.base or fk.schema, fk.table)

            # Get the ON statement in the join
            ons = [key+'.'+fk.primary[idx] + " = " + self.name + "." + col for idx, col in enumerate(fk.foreign)]
            on_list = ' AND '.join(ons)

            joins.append(f"left join {self.schema or self.cat}.{table.name} {key} on {on_list}")

        self.join = "\n".join(joins)

        return self.join


    def get_sort_fields(self, selects):
        sort_fields = Dict()
        for sort in self.get_sort_columns():
            # Split into field and sort order
            parts = sort.split(' ')
            key = parts[0]
            direction = 'asc' if len(parts) == 1 else parts[1]
            sort_fields[key].field = self.name + "." + key
            if key in selects:
                sort_fields[key].field = selects[key]
            sort_fields[key].order = direction
        
        return sort_fields


    def make_order_by(self, selects):
        pkey = self.get_primary_key()

        order_by = "order by "
        sort_fields = self.get_sort_fields(selects)

        if (len(pkey) == 0 and len(sort_fields) == 0):
            return ""

        for sort in sort_fields.values():
            if self.db.cnxn.system == 'mysql':
                order_by += f"isnull({sort.field}), {sort.field} {sort.order}, "
            elif self.db.cnxn.system in ['oracle', 'postgres']:
                order_by += f"{sort.field} {sort.order}, "
            elif self.db.cnxn.system == 'sqlite':
                order_by += f"{sort.field} is null, {sort.field} {sort.order}, "
        
        for field in pkey:
            order_by += f"{self.name}.{field}, "

        order_by = order_by[0:-2]

        if self.db.cnxn.system in ['oracle', 'postgres']:
            order_by += " nulls last"

        return order_by

    def get_sums(self):
        sums = []

        cols = self.get_summation_columns()
        join = self.get_join()
        cond = self.get_conds()
        params = self.params

        if len(cols):
            selects = []
            for col in cols:
                selects.append(f"sum({col}) as {col}")
            select = ', '.join(selects)

            sql = "select " + select + "\n"
            sql+= f"from {self.name}\n"
            sql+= join + "\n"
            sql+= "" if not cond else "where " + cond

            cursor = self.db.cnxn.cursor()
            row = cursor.execute(sql, params).fetchone()
            cols = [col[0] for col in cursor.description]
            sums = dict(zip(cols, row))

        return sums

    def get_field_groups(self, fields):
        """Group fields according to first part of field name"""
        col_groups = Dict()
        for field in fields.values():
            # Don't add column to form if it's part of primary key but not shown in grid
            if (field.name in self.get_primary_key() and
                field.name not in self.get_grid_columns()
            ): continue

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

        form = Dict({'items': {}}) #TODO: vurder 'subitems'
        fields = self.get_fields()
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

                if sum_size < 50:
                    inline = True

                group_label = self.db.get_label(group_name)
                
                form['items'][group_label] = Dict({
                    'inline': inline,
                    'items': subitems  #TODO vurder 'subitems' også for nøkkel
                })

        # Add relations to form
        relations = self.get_relations()

        for alias, rel in relations.items():
            #TODO: Finn faktisk database det lenkes fra
            rel_table = Table(self.db, rel.table)

            # Find indexes that can be used to get relation
            #TODO: Har jeg ikke gjort liknende lenger opp?
            # Se "Find if there exists an index to find foreign key"
            index_exist = False
            s = slice(0, len(rel.foreign))
            rel_indexes = rel_table.get_indexes()
            for index in rel_indexes.values():
                if index.columns[s] == rel.foreign:
                    index_exist = True

            if index_exist and not rel.get('hidden', False):
                rel_pkey = rel_table.get_primary_key()
                label = rel_table.name.replace(self.name + '_', '')
                if set(rel_pkey) > set(rel.foreign):
                    # If foreign key is part of primary key, and the other
                    # pk field is also a foreign key, we have a xref table
                    rel_fkeys = rel_table.get_fkeys()
                    rest = set(rel_pkey) - set(rel.foreign)
                    pk_field = list(rest)[-1]
                    if pk_field in rel_fkeys:
                        label = pk_field
                label = label.replace('_' + self.name, '')
                label = self.db.get_label(label).strip()
                form['items'][label] = "relations." + alias

        return form

    def get_relation(self, alias):
        if not hasattr(self, 'relations'):
            self.init_relations()

        return self.relations[alias]

    def get_relations(self):
        #TODO: Skal filtreres på permission
        if not hasattr(self, 'relations'):
            self.init_relations()

        return self.relations

    def get_ref_relations(self):
        """ Interim function needed to support old schema """
        relations = {}
        for fk in self.get_relations().values():
            # fk_table = Table(self.db, fk.table) #TODO: egentlig db

            # alias = fk.foreign[-1]
            # if alias in foreign_keys:
            #     alias = alias + "_2"
            # foreign_keys[alias] = fk

            relations[fk.name] = Dict({
                'table': fk.table,
                'foreign_key': fk.foreign[-1],
                # 'label': self.db.get_label(fk.table) #TODO: Finn bedre løsning
            })

        return relations

    def get_user_permission(self, tbl_name):
        #TODO: Når behøver jeg å angi tbl_name?
        user = 'admin' #TODO: Autentisering

        sql = """
        --sql
        select rp.*
        from role_permission rp 
        inner join (
            select max(schema_) schema_, max(role_) role_,
                   max(table_) table_
            from role_permission
            where schema_ in (?, '*')
              and table_ in (?, '*')
              and role_ in (select role_ from user_role where user_ = ?)
            group by role_
        ) rp2 on rp.role_ = rp2.role_ and rp.schema_ = rp2.schema_
        and rp.table_ = rp2.table_;
        """

        cursor = self.db.urd.cursor()
        rows = cursor.execute(sql, self.db.schema, tbl_name, user).fetchall()

        permission = Dict({
            'view': False,
            'add': False,
            'edit': False,
            'delete': False,
            'admin': False
        })

        for row in rows:
            if row.view_  : permission.view   = True
            if row.add_   : permission.add    = True
            if row.edit   : permission.edit   = True
            if row.delete_: permission.delete = True
            if row.admin  : permission.admin  = True

        #TODO: Kode hvis ingen permission er gitt. Merkelig

        if self.db.schema == 'urd':
            admin_schemas = self.db.get_user_admin_schemas() #TODO

            if len(admin_schemas):
                #TODO: definer listen med navn som beskriver hva dette er
                if self.name in ['filter', 'format', 'role', 'role_permission', 'user_role']:
                    self.add_cond(self.name + ".schema_ in ('" + "','".join(admin_schemas) + "')")

                #TODO: Merkelig å gjenta nesten samme lista
                #       Må iallfall kunne forenkle dette
                if self.name in ['filter', 'format', 'role', 'role_permission', 'user_', 'user_role']:
                    permission.view = 1
                    permission.add = 1
                    permission.edit = 1
                    permission.delete = 1

        if (self.get_type() == 'reference' and permission.admin == 0):
            permission.view = 0

        return permission

    def set_search_cond(self, query):
        filters = query.split(" AND ")
        for filter in filters:
            parts = re.split(r"\s*([=<>]|!=| IN| LIKE|NOT LIKE|IS NULL|IS NOT NULL)\s*", filter, 2)
            field = parts[0]
            if "." not in field:
                field = self.name + "." + field
            operator = parts[1].strip()
            value = parts[2].replace("*", "%")
            if operator == "IN":
                value = "('" + value.strip().split(",").join("','") + "')"
            value = value.strip()
            if value == "":
                value = None
            self.add_cond(field, operator, value)

    def add_cond(self, expr, operator="=", value=None):
        if value is None:
            if operator in ["IS NULL", "IS NOT NULL"]:
                self.conditions.append(f"{expr} {operator}")
            else:
                self.conditions.append(expr)
        else:
            self.conditions.append(f"{expr} {operator} ?")
            self.params.append(value)
            self.client_conditions.append(f"{expr} {operator} {value}")
        #TODO: Handle "in" operator

    def get_conds(self):
        return " and ".join(self.conditions)

    def get_conditions(self):
        return self.conditions

    def get_client_conditions(self):
        return self.client_conditions
    
    def get_record_count(self, join=''):
        cond = self.get_conds()
        params = self.params
        print('params', params)
        sql = "select count(*) \n"
        sql+= f"  from {self.schema or self.cat}.{self.name} \n"
        sql+= join + "\n"
        sql+= "" if not cond else "where " + cond

        cursor = self.db.cnxn.cursor()
        count = cursor.execute(sql, params).fetchval()
        
        return count

    def get_filter(self): #TODO
        return ""

    def save(self, records: list):
        from database import Database
        result = Dict()
        for rec in records:
            rec = Dict(rec)
            record = Record(self.db, self, rec.prim_key)
            if rec.method == 'delete':
                record.delete()
            elif rec.method == 'post':
                pk = record.insert(rec['values'])

                # Must get autoinc-value for selected record to get
                # correct offset when reloading table after saving
                if 'selected' in rec:
                    result.selected = pk
                
                # Insert value for primary key also in the relations
                for rel in self.get_relations().values():
                    for rel_rec in rel.records.values():
                        for idx, colname in enumerate(rel.foreign):
                            if colname not in rel_rec.values:
                                pk_col = rel.primary[idx]
                                rel_rec.values[colname] = pk[pk_col]
                    
            elif rec.method == "put":
                if rec['values']:
                    res = record.update(rec['values'])

            #TODO: Log to log table

            record_vals = record.get_values()

            # Iterates over all the relations to the record
            for rel in rec.relations.values():

                rel_db = Database(rel.base_name)
                rel_table = Table(rel_db, rel.table_name)
                
                # Set value of fkey columns to matched colums of record
                fkey = rel_table.get_fkey(rel.fkey)
                for rel_rec in rel.records:
                    for idx, col in enumerate(fkey.foreign):
                        pkcol = fkey.primary[idx]
                        rel_rec['values'][col] = record_vals[pkcol]

                        # Primary keys of relation may be updated by
                        # cascade if primary keys of record is updated
                        if col in rel_rec.prim_key:
                            rel_rec.prim_key[col] = record_vals[pkcol]

                rel_table.save(rel.records)
        
        return result

    def init_foreign_keys(self):
        if self.db.metadata.get("cache", None):
            self.foreign_keys = self.db.metadata.cache[self.name].foreign_keys
            return
        cursor = self.db.cnxn.cursor()
        foreign_keys = Dict()
        keys = {}

        for row in cursor.foreignKeys(foreignTable=self.name,
                                      foreignCatalog=self.cat,
                                      foreignSchema=self.schema):
            name = row.fk_name
            if name not in keys:
                keys[name] = Dict({
                    'name': name,
                    'table': row.pktable_name,
                    'base': row.pktable_cat,
                    'schema': row.pktable_schem,
                    'delete_rule': row.delete_rule,
                    'update_rule': row.update_rule,
                    'foreign': [],
                    'primary': []
                })
            keys[name].foreign.append(row.fkcolumn_name.lower())
            keys[name].primary.append(row.pkcolumn_name.lower())

        for fk in keys.values():
            alias = fk.foreign[-1]
            if alias in foreign_keys:
                alias = alias + "_2"
            foreign_keys[alias] = fk

        self.foreign_keys = foreign_keys

    def init_fields(self):
        if self.db.metadata.get("cache", None):
            self.fields = self.db.metadata.cache[self.name].fields
            return
        fields = Dict()
        indexes = self.get_indexes()
        cursor = self.db.cnxn.cursor()
        if self.db.cnxn.system == 'oracle':
            # cursor.columns doesn't work for all types of oracle columns
            sql = self.db.expr.columns()
            cols = cursor.execute(sql, self.db.schema, self.name).fetchall()
        else:
            cols = cursor.columns(table=self.name, catalog=self.cat,
                                  schema=self.schema).fetchall()
        for col in cols:
            colnames = [column[0] for column in col.cursor_description]
            col = Dict(zip(colnames, col))
            if ('column_size' in col or 'display_size' in col):
                col.column_size = col.get('column_size', col.display_size)
            cname = col.column_name

            column = Column(self, cname)
            fields[cname] = column.get_field(col)

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

        self.fields = fields

    def init_view(self):
        #TODO: Hvordan kan filter defineres i databasen?
        #       Og når er det behov for å gjøre det?
        tbl_filter = self.get_filter()
        if tbl_filter:
            condition = 'where ' + self.db.expr.replace_vars(tbl_filter)
        else:
            condition = ''

        cols = []
        n = 0  # modified columns
        fields = self.get_fields()
        for key, col in fields.items():
            if col.get('table', self.name) != self.name:
                continue

            if 'name' not in col:
                col.name = key

            if 'source' in col:
                cols.append(f"({col.source}) as {key}")
                n += 1
            elif col.name != key:
                cols.append(f"{col.name} as {key}")
                n += 1
            else:
                cols.append(col.name)

        if n:
            select = ', '.join(cols)
            view = "(select " + select + "\n"
            view += " from " + self.name + "\n"
            view += condition + ")\n"
        elif condition:
            view = "(select " + self.name + ".*\n"
            view += " from " + self.name + "\n"
            view += condition + ")"
        else:
            view = self.name

        self.view = view
            
    def init_indexes(self):
        if self.db.metadata.get("cache", None):
            self.indexes= self.db.metadata.cache[self.name].indexes
            return
        cursor = self.db.cnxn.cursor()
        indexes = Dict()

        for row in cursor.statistics(table=self.name, catalog=self.cat,
                                     schema=self.schema):
            name = row.index_name
            # Sometimes rows not part of index is returned
            if name == None: continue

            if name not in indexes:
                indexes[name] = Dict({
                    'name': name,
                    'unique': not row.non_unique,
                    'columns': []
                })

            indexes[name].columns.append(row.column_name)

        self.indexes = indexes

    def init_relations(self):
        if hasattr(self.db, 'relations'):
            print('self.name', self.name)
            self.relations = self.db.relations[self.name]
            return
        if self.db.metadata.get("cache", None):
            self.relations = self.db.metadata.cache[self.name].relations
            return
        cursor = self.db.cnxn.cursor()
        relations = Dict()

        for row in cursor.foreignKeys(table=self.name, catalog=self.cat,
                                      schema=self.schema):
            name = row.fk_name
            if name not in relations:
                relations[name] = Dict({
                    'name': name,
                    'table': row.fktable_name,
                    'base': row.fktable_cat,
                    'schema': row.fktable_schem,
                    'delete_rule': row.delete_rule,
                    'foreign': [],
                    'primary': []
                })

            relations[name].foreign.append(row.pkcolumn_name)
            relations[name].primary.append(row.fkcolumn_name)

        self.relations = relations

    def export_ddl(self, system):
        ddl = f"create table {self.name} (\n"
        coldefs = []
        for col in self.get_fields().values():
            expr = Expression(system)
            datatype = expr.to_native_type(col.datatype, col.size)
            coldef = f"    {col.name} {datatype}"
            if not col.nullable:
                coldef += " NOT NULL"
            coldefs.append(coldef)
        ddl += ",\n".join(coldefs)
        ddl += ")"

        return ddl
