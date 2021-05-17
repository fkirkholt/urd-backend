import json
from addict import Dict
from record import Record
import re

class Table:
    def __init__(self, db, tbl_name):
        self.db = db
        self.name = tbl_name
        self.label = tbl_name # todo
        self.offset = 0
        self.limit = 30
        self.conditions = []
        self.params = []
        self.client_conditions = []
        self.user_filtered = False

    def get_view(self):
        if not hasattr(self, 'view'):
            self.init_view()

        return self.view

    def get_type(self):
        if (
            self.name[0:4] == "ref_" or 
            self.name[-2:] == "_ref" or
            self.name[0:5] == "meta_"
        ):
            self.type = "reference"
        else:
            # Check if unique indexes cover all columns
            idx_cols = []
            for idx in self.get_indexes().values():
                if idx.unique:
                    idx_cols = idx_cols + idx.columns

            idx_cols = list(set(idx_cols))

            if len(self.get_fields()) == len(idx_cols):
                self.type = "reference"
            else:
                self.type = "data"

        return self.type

        # todo: Flere sjekker, særlig hvis ikke urd-struktur

    
    def get_db_table(self, base, table):
        from database import Database
        db = Database(base)
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

    
    def get_options(self, field, fields=None):

        fk = self.get_fkey(field.name)
        cand_tbl = self.get_db_table(fk.base, fk.table)

        # List of fields
        kodefelter = [field.name + '.' + name for name in fk.foreign]

        # Field that holds the value of the options
        value_field = kodefelter[-1]

        # Sorting
        cand_sort_columns = cand_tbl.get_sort_columns()
        sort_fields = [field.name + '.' + col for col in cand_sort_columns]

        order = "order by " + ', '.join(sort_fields) if len(sort_fields) else ''

        # Conditions
        conditions = []
        if 'filter' in fk:
            conditions.append("("+self.db.expr.replace_vars(fk.filter)+")")

        if fk.schema == 'urd' and 'schema_' in cand_tbl.fields:
            admin_schemas = "'" + "', '".join(self.db.get_user_admin_schemas()) + "'"
            conditions.append(f"schema_ in ({admin_schemas})")
        
        # Adds condition if this select depends on other selects
        if 'value' in field and len(fk.local) > 1:
            for idx, key in enumerate(fk.local):
                if key != field.name and fields[key].value:
                    conditions.append(fk.foreign[idx] + " = '" + str(fields[key].value) + "'")

        condition = "where " + " AND ".join(conditions) if len(conditions) else ''

        if not 'column_view' in field:
            field.column_view = field.view

        sql = "select " + value_field + " as value, "
        sql+= "(" + field.view + ") as label, "
        sql+= "(" + field.column_view + ") as coltext "
        sql+= "from " + cand_tbl.name + " " + field.name + "\n"
        sql+= condition + "\n" + order

        cursor = self.db.cnxn.cursor()
        count = cursor.execute(sql).rowcount

        if (count > 200):
            return False
        
        rows = cursor.fetchall()

        result = []
        colnames = [column[0] for column in cursor.description]
        for row in rows:
            result.append(dict(zip(colnames, row)))

        return result


    def get_values(self, selects, join, order):
        # todo: hent join selv, og kanskje flere
        cols = []
        fields = self.get_fields()
        for key in selects.keys():
            if key in fields and 'source' not in fields[key]:
                cols.append(self.name + '.' + key)

        select = ', '.join(cols)
        cond = self.get_conds()
        params = self.params

        sql = "select " + select
        sql+= "  from " + self.name
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

    def get_display_values(self, selects, join, order):
        for key, value in selects.items():
            selects[key] = value + ' as ' + key
        
        select = ', '.join(selects.values())

        conds = self.get_conds()
        params = self.params

        sql = "select " + select
        sql+= "  from " + self.name
        sql+= " " + join + "\n" 
        sql+= "" if not conds else "where " + conds + "\n"
        sql+= order

        cursor = self.db.cnxn.cursor()
        self.count = cursor.execute(sql, params).rowcount
        cursor.skip(self.offset)
        rows = cursor.fetchmany(self.limit)

        # todo: Vurder å legge det under til en funksjon
        result = []
        colnames = [column[0] for column in cursor.description]
        for row in rows:
            result.append(dict(zip(colnames, row)))

        return result

    def get_primary_key(self):
        cursor = self.db.cnxn.cursor()
        return [row.column_name for row in cursor.primaryKeys(self.name)]

    def get_grid_columns(self):
        indexes = self.get_indexes()
        grid_idx = indexes.get(self.name + "_grid_idx", None)
        if grid_idx:
            columns = grid_idx.columns
        else:
            columns = []
            for key, field in self.get_fields().items():
                # Don't show hdden columns
                if field.name[0:1] == '_': continue
                # todo: Don't show autoinc columns
                columns.append(key)
                if len(columns) == 5: break
        
        return columns

    def get_sort_columns(self):
        indexes = self.get_indexes()
        sort_idx = indexes.get(self.name + "_sort_idx", None)
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
            # todo: Behøver selects å være dict? Kan det ikke være list? Det forenkler vel koden litt.
        
        primary_key = self.get_primary_key()
        foreign_keys = self.get_fkeys()
        fields = self.get_fields()

        grid = Dict({
            'columns': self.get_grid_columns(),
            'sort_columns': self.get_sort_columns(),
            'summation_columns': self.get_summation_columns()
        })

        for col in primary_key:
            selects[col] = self.name + '.' + col

        for alias in grid.columns:

            col = fields[alias]
            col.alias = alias

            col.ref = self.name + '.' + alias

            if alias in foreign_keys:
                fk = foreign_keys[alias]
                if 'view' in col:
                    col.options = self.get_options(col)
            else:
                fk = None

            if 'view' in col and 'column_view' not in col:
                col.column_view = col.view
            
            if 'column_view' in col:
                selects[alias] = col.column_view
            elif col.element == 'textarea':
                selects[alias] = "substr(" + col.ref + ', 1, 255)'
            else:
                selects[alias] = col.ref

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


        # todo: Make select to get disabled status for actions

        join = self.get_join()

        # todo: Find selected index

        order_by = self.make_order_by(selects)


        display_values = self.get_display_values(selects, join, order_by)
        values = self.get_values(selects, join, order_by)

        recs = []
        for row in display_values:
            recs.append({'columns': row})
        
        for index, row in enumerate(values):
            recs[index]['values'] = row
            recs[index]['primary_key'] = {key: row[key] for key in primary_key}
        # todo: row formats

        sums = self.get_sums(join)

        # todo: Don't let fields be reference to fields
        # todo: Burde ikke være nødvendig
        fields = json.loads(json.dumps(fields))

        # todo: replace field.name with field.alias

        form = self.get_form()

        data = Dict({
            'name': self.name,
            'records': recs,
            'count_records': self.count,
            'fields': fields,
            'grid': {
                'columns': grid.columns,
                'sums': sums,
                'sort_columns': grid.sort_columns
            },
            'form': { # todo:  kun ett attributt
                'items': None if 'items' not in form else form['items']
            },
            'permission': { # todo: hent fra funksjon
                'view': 1,
                'add': 1,
                'edit': 1,
                'delete': 1
            },
            'type': self.get_type(),
            'primary_key': primary_key,
            'foreign_keys': foreign_keys,
            'label': self.db.get_label(self.name),
            'actions': getattr(self, 'actions', []),
            'limit': self.limit,
            'offset': self.offset,
            'selection': 0, # todo row_idx
            'conditions': [], # todo: self.client_conditions,
            'date_as_string': {'separator': '-'}, # todo wtf
            'expansion_column': None, # todo
            'relations': self.get_ref_relations(),
            'saved_filters': [] # todo: self.get_saved_filters()
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
        joins = []
        foreign_keys = self.get_fkeys()
        fields = self.get_fields()
        for key, fk in foreign_keys.items():
            if key not in fields:
                continue

            table = self.get_db_table(fk.base, fk.table)

            # Get the ON statement in the join
            ons = [key+'.'+fk.foreign[idx] + " = " + self.name + "." + col for idx, col in enumerate(fk.local)]
            on_list = ' AND '.join(ons)

            joins.append(f"left join {table.name} {key} on {on_list}")

        return "\n".join(joins)

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
        primary_key = self.get_primary_key()

        order_by = "order by "
        sort_fields = self.get_sort_fields(selects)

        for sort in sort_fields.values():
            if self.db.system == 'mysql':
                order_by += f"isnull({sort.field}), {sort.field} {sort.order}, "
            elif self.db.system in ['oracle', 'postgres']:
                order_by += f"{sort.field} {sort.order}, "
            elif self.db.system == 'sqlite':
                order_by += f"{sort.field} is null, {sort.field} {sort.order}, "
        
        for field in primary_key:
            order_by += f"{self.name}.{field}, "

        order_by = order_by[0:-2]

        if self.db.system in ['oracle', 'postgres']:
            order_by += " nulls last"

        return order_by

    def get_sums(self, join):
        sums = []

        cols = self.get_summation_columns()
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

    def get_form(self):

        col_groups = Dict()
        form = Dict({'items': {}}) # todo: vurder 'subitems'
        fields = self.get_fields()

        # Group fields according to first part of field name
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

        for group_name, col_names in col_groups.items():
            for group_name, col_names in col_groups.items():
                if len(col_names) == 1:
                    label = self.db.get_label(col_names[0])
                    form['items'][label] = col_names[0]
                else:
                    inline = False
                    colnames = Dict()  # todo: tullete med colnames og col_names
                    for colname in col_names:
                        # removes group name prefix from column name and use the rest as label
                        rest = colname.replace(group_name+"_", "")
                        label = self.db.get_label(rest)

                        colnames[label] = colname

                        if 'separator' in fields[colname]:
                            inline = True

                    group_label = self.db.get_label(group_name)
                    
                    form['items'][group_label] = Dict({
                        'inline': inline,
                        'items': colnames  # todo vurder 'subitems'
                    })

        # Add relations to form
        relations = self.get_relations()

        for alias, rel in relations.items():
            # todo: Finn faktisk database det lenkes fra
            rel_table = Table(self.db, rel.table)

            # Find indexes that can be used to get relation
            # todo: Har jeg ikke gjort liknende lenger opp?
            # Se "Find if there exists an index to find local key"
            index_exist = False
            s = slice(0, len(rel.foreign))
            rel_indexes = rel_table.get_indexes()
            for index in rel_indexes.values():
                if index.columns[s] == rel.foreign:
                    index_exist = True

            if index_exist and not rel.get('hidden', False):
                if rel.foreign == rel_table.get_primary_key():
                    rest = rel_table.name.replace(self.name+"_", "")
                    label = self.db.get_label(rest)
                else:
                    label = self.db.get_label(rel_table.name)
                form['items'][label] = "relations." + alias

        return form

    def get_relation(self, alias):
        if not hasattr(self, 'relations'):
            self.init_relations()

        return self.relations[alias]

    def get_relations(self): 
        # todo: Skal filtreres på permission
        if not hasattr(self, 'relations'):
            self.init_relations()

        return self.relations

    def get_ref_relations(self):
        """ Interim function needed to support old schema """
        relations = {}
        for fk in self.get_relations().values():
            # fk_table = Table(self.db, fk.table) # todo: egentlig db

            # alias = fk.foreign[-1]
            # if alias in foreign_keys:
            #     alias = alias + "_2"
            # foreign_keys[alias] = fk

            relations[fk.name] = Dict({
                'table': fk.table,
                'foreign_key': fk.foreign[-1],
                # 'label': self.db.get_label(fk.table) # todo: Finn bedre løsning
            })

        return relations

    def get_user_permission(self, tbl_name):
        # todo: Når behøver jeg å angi tbl_name?
        user = 'admin' # todo: Autentisering

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

        # todo: Kode hvis ingen permission er gitt. Merkelig

        if self.db.schema == 'urd':
            admin_schemas = self.db.get_user_admin_schemas() # todo

            if len(admin_schemas):
                # todo: definer listen med navn som beskriver hva dette er
                if self.name in ['filter', 'format', 'role', 'role_permission', 'user_role']:
                    self.add_cond(self.name + ".schema_ in ('" + "','".join(admin_schemas) + "')")

                # todo: Merkelig å gjenta nesten samme lista
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
            print('filter', filter)
            parts = re.split(r"\s*([=<>]|!=| IN| LIKE|NOT LIKE|IS NULL|IS NOT NULL)\s*", filter, 2)
            print('parts', parts)
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
        # todo: Handle "in" operator

    def get_conds(self):
        return " and ".join(self.conditions)

    def get_conditions(self):
        return self.conditions

    def get_client_conditions(self):
        return self.client_conditions
    
    def get_record_count(self, join=''):
        cond = self.get_conds()
        params = self.params
        sql = "select count(*) \n"
        sql+= f"  from {self.name} \n"
        sql+= join + "\n"
        sql+= "" if not cond else "where " + cond

        cursor = self.db.cnxn.cursor()
        count = cursor.execute(sql, params).fetchval()
        
        return count

    def get_filter(self): # todo
        return ""

    def get_select(self, req):
        # todo: Kan jeg ikke hente noe fra backend istenfor å få alt servert fra frontend? Altfor mange parametre!
        search = None if not 'q' in req else req.q.replace("*", "%")

        if 'key' in req:
            key = json.loads(req.key)
            col = key[-1]
        else:
            col = self.get_primary_key()[-1]

        view = req.get('view') or col
        col_view = req.get('column_view') or col

        conds = req.condition.split(" and ") if req.condition else []
        # ignore case
        if search:
            search = search.lower()
            conds.append(f"lower(cast({view} as varchar)) like '%{search}%'")
        
        cond = " and ".join(conds) if len(conds) else col + "IS NOT NULL"

        val_col = req.alias + "." + col

        sql = f"""
        select distinct {val_col} as value, {view} as label,
        {col_view} as coltext\n
        from {self.name} {req.alias}\n
        where {cond}\n
        order by {view}
        """

        rows = self.db.query(sql).fetchmany(int(req.limit))

        result = []
        for row in rows:
            result.append({'value': row.value, 'label': row.label})

        return result

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
                        for idx, colname in enumerate(rel.local):
                            if colname not in rel_rec.values:
                                pk_col = rel.foreign[idx]
                                rel_rec.values[colname] = pk[pk_col]
                    
            elif rec.method == "put":
                if rec['values']:
                    res = record.update(rec['values'])

            # todo: Log to log table

            record_vals = record.get_values()

            # Iterates over all the relations to the record
            for rel in rec.relations.values():

                rel_db = Database(rel.base_name)
                rel_table = Table(rel_db, rel.table_name)
                
                # Set value of fkey columns to matched colums of record
                fkey = rel_table.get_fkey(rel.fkey)
                for rel_rec in rel.records:
                    for idx, col in enumerate(fkey.local):
                        fcol = fkey.foreign[idx]
                        rel_rec['values'][col] = record_vals[fcol]

                        # Primary keys of relation may be updated by
                        # cascade if primary keys of record is updated
                        if col in rel_rec.prim_key:
                            rel_rec.prim_key[col] = record_vals[fcol]

                rel_table.save(rel.records)
        
        return result

    def init_foreign_keys(self):
        cursor = self.db.cnxn.cursor()
        foreign_keys = Dict()
        keys = {}

        for row in cursor.foreignKeys(foreignTable=self.name):
            name = row.fk_name
            if name not in keys:
                keys[name] = Dict({
                    'name': name,
                    'table': row.pktable_name,
                    'base': row.pktable_cat,
                    'schema': row.pktable_schem,
                    'local': [],
                    'foreign': []
                })
            keys[name].local.append(row.fkcolumn_name.lower())
            keys[name].foreign.append(row.pkcolumn_name.lower())

        for fk in keys.values():
            alias = fk.local[-1]
            if alias in foreign_keys:
                alias = alias + "_2"
            foreign_keys[alias] = fk

        self.foreign_keys = foreign_keys

    def init_fields(self):
        fields = Dict()
        foreign_keys = self.get_fkeys()
        pkey = self.get_primary_key()
        cursor = self.db.cnxn.cursor()
        for col in cursor.columns(table=self.name):
            cname = col.column_name
            type_ = self.db.expr.to_urd_type(col.type_name)
            
            # Decides what sort of input should be used
            if type_ == 'date':
                element = 'input[type=date]'
            elif type_ == 'boolean':
                if col.nullable:
                    element = 'select'
                    options = [
                        {
                            'value': 0,
                            'label': 'Nei'
                        },
                        {
                            'value': 1,
                            'label': 'Ja'
                        }
                    ]
                else:
                    element = 'input[type=checkbox]'
            elif cname in foreign_keys:
                element = 'select'
                options = []
            elif type_ == 'binary' or (type_ == 'string' and (col.display_size > 255)):
                element = "textarea"
            else:
                element = "input[type=text]"
            
            urd_col = Dict({
                'name': cname,
                'datatype': type_,
                'element': element,
                'nullable': col.nullable == True,
                'label': self.db.get_label(cname),
                'description': None # todo
            })

            if type_ not in ["boolean", "date"]:
                urd_col.size = col.display_size
            if col.auto_increment:
                urd_col.extra = "auto_increment"
            if element == "select" and len(options):
                urd_col.options = options
            elif cname in foreign_keys:
                fk = foreign_keys[cname]
                urd_col.foreign_key = fk
                ref_tbl = Table(self.db, fk.table)
                ref_tbl.pk = ref_tbl.get_primary_key()

                for index in ref_tbl.get_indexes().values():
                    if index.columns != ref_tbl.pk and index.unique:
                        cols = [cname+"."+col for col in index.columns]
                        urd_col.view = " || ".join(cols)
                        break

                if 'view' in urd_col:
                    urd_col.options = self.get_options(urd_col)
            if (type_ == 'integer' and cname == pkey[-1] and cname not in foreign_keys):
                urd_col.extra = "auto_increment"
            
            if col.column_def and not col.auto_increment:
                def_vals = col.column_def.split('::')
                default = def_vals[0]
                default = default.replace("'", "")

                # todo: Sjekk om jeg trenger å endre current_timestamp()

                urd_col.default = default

            fields[cname] = urd_col

        self.fields = fields

    def init_view(self):
        # todo: Hvordan kan filter defineres i databasen?
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
        cursor = self.db.cnxn.cursor()
        indexes = Dict()

        for row in cursor.statistics(self.name):
            name = row.index_name

            if name not in indexes:
                indexes[name] = Dict({
                    'name': name,
                    'unique': not row.non_unique,
                    'columns': []
                })

            indexes[name].columns.append(row.column_name)

        self.indexes = indexes

    def init_relations(self):
        cursor = self.db.cnxn.cursor()
        relations = Dict()

        for row in cursor.foreignKeys(table=self.name):
            name = row.fk_name
            if name not in relations:
                relations[name] = Dict({
                    'name': name,
                    'table': row.fktable_name,
                    'base': row.fktable_cat,
                    'schema': row.fktable_schem,
                    'local': [],
                    'foreign': []
                })
            relations[name].local.append(row.pkcolumn_name)
            relations[name].foreign.append(row.fkcolumn_name)

        self.relations = relations



