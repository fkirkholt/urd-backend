import json
from dotmap import DotMap
from schema import Schema
from sqlparams import SQLParams

class Table:
    def __init__(self, db, tbl_name):
        table = db.tables[tbl_name]
        self.db = db
        self.name = tbl_name
        for key, val in table.items():
            setattr(self, key, val)
        self.offset = 0 # todo
        self.limit = 30 # todo
        self.extension_tables = [] # todo
        if not hasattr(self, 'form'):
            self.form = self.get_form()
        self.conditions = []
        self.client_conditions = []
        if 'sort_columns' not in self.grid:
            self.grid['sort_columns'] = []
        

    def get_view(self): # todo
        return self.name
    
    def get_options(self, field): # todo
        fk = self.foreign_keys[field['alias']]

        if 'schema' not in fk or fk['schema'] == self.db.schema:
            fk['schema'] = self.db.schema
            ref_schema = self.db.schema
            ref_base = self.db
        else:
            ref_schema = Schema(fk['schema'])
            ref_base_name = ref_schema.get_db_name()
            ref_base = Database(ref_base_name)
        
        cand_tbl = Table(ref_base, fk['table'])

        # List of fields
        kodefelter = [field['alias'] + '.' + name for name in fk['foreign']]

        # Field that holds the value of the options
        value_field = kodefelter[-1]

        # Sorting
        sort_fields = [field['alias'] + '.' + col for col in cand_tbl.grid['sort_columns']]

        order = "order by " + ', '.join(sort_fields) if len(sort_fields) else ''

        # Conditions
        conditions = []
        # todo: hvis fk.filter

        if ref_schema == 'urd' and 'schema_' in cand_tbl.fields:
            admin_schemas = "'" + "', '".join(self.db.get_user_admin_schemas()) + "'"
            conditions.append("schema_ in (%s)" % admin_schemas)
        
        # todo: Adds condition if this select depends on other selects
        # Antar dette kun gjelder ved kall fra record.
        # Det er vel da "fields" er fylt ut, og field.value finnes
        # if 'value' in field and len(fk.local) > 1:
        #     for idx, foreign_field in fk.local.items():
        #         if foreign_field != field.name and 

        condition = "where " + " AND ".join(conditions) if len(conditions) else ''

        view = cand_tbl.get_view()

        # todo: Satt inn for å få dette til å fungere
        # jf. kommentaren under get_grid
        if not 'column_view' in field:
            field['column_view'] = field['view']

        sql = "select " + value_field + " as value, "
        sql+= "(" + field['view'] + ") as label, "
        sql+= "(" + field['column_view'] + ") as coltext "
        sql+= "from " + view + " " + field['alias'] + "\n"
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


    def get_values(self, selects, join, condition, order):
        # todo: hent join selv, og kanskje flere
        cols = []
        for key in selects.keys():
            if key in self.fields and 'source' not in self.fields[key]:
                cols.append(self.name + '.' + key)

        select = ', '.join(cols)

        sql = "select " + select
        sql+= "  from " + self.name
        sql+= " " + join + ' ' + condition + ' ' + order

        cursor = self.db.cnxn.cursor()
        cursor.execute(sql)
        cursor.skip(self.offset)
        rows = cursor.fetchmany(self.limit)

        result = []
        colnames = [column[0] for column in cursor.description]
        for row in rows:
            result.append(dict(zip(colnames, row)))

        return result

    def get_display_values(self, selects, join, condition, order):
        for key, value in selects.items():
            selects[key] = value + ' as ' + key
        
        select = ', '.join(selects.values())
        view = self.get_view() # todo

        sql = "select " + select
        sql+= "  from " + view + ' ' + self.name
        sql+= " " + join + ' ' + condition + ' ' + order

        cursor = self.db.cnxn.cursor()
        count = cursor.execute(sql).rowcount
        print('count display values')
        print(count) # todo: Legg inn i self.count eller noe slikt
        cursor.skip(self.offset)
        rows = cursor.fetchmany(self.limit)

        # todo: Vurder å legge det under til en funksjon
        result = []
        colnames = [column[0] for column in cursor.description]
        for row in rows:
            result.append(dict(zip(colnames, row)))

        return result


    def get_grid(self):
        selects = {} # dict of select expressions
            # todo: Behøver selects å være dict? Kan det ikke være list? Det forenkler vel koden litt.

        for col in self.primary_key:
            selects[col] = self.name + '.' + col

        for alias in self.grid['columns']:

            col = self.fields[alias]
            col['alias'] = alias

            col['ref'] = self.name + '.' + alias

            if alias in self.foreign_keys:
                fk = self.foreign_keys[alias]
                self.fields[alias]['foreign_key'] = fk # todo: burde være unødvendig
                if 'view' in col:
                    col['options'] = self.get_options(col)
            else:
                fk = None

            if 'view' in col and 'column_view' not in col:
                col['column_view'] = col['view']
            
            if 'column_view' in col:
                selects[alias] = col['column_view']
            elif col['element'] == 'textarea':
                selects[alias] = "substr(" + col['ref'] + ', 1, 255)'
            else:
                selects[alias] = col['ref']

        if hasattr(self, 'expansion_column'):
            # Get number of relations to same table for expanding row
            fk = self.get_parent_fk() # todo: make this function
            rel_column = self.fields[fk['alias']]
            wheres = []

            for idx, colname in enumerate(fk['foreign']):
                foreign = fk['foreign'][idx]
                wheres.append(col_name + ' = ' + self.name + '.' + foreign)

            # todo: Fullfør seinere

        # todo: Make select to get disabled status for actions

        joins = self.get_joins() # todo: Make function
        join = '\n'.join(joins)

        # todo: Find selected index

        order_by = self.make_order_by(selects)
        print("order by: " + order_by)

        condition = '' # todo

        display_values = self.get_display_values(selects, join, condition, order_by)
        values = self.get_values(selects, join, condition, order_by)

        recs = []
        for row in display_values:
            recs.append({'columns': row})
        
        for index, row in enumerate(values):
            recs[index]['values'] = row
            recs[index]['primary_key'] = {key: row[key] for key in self.primary_key}
        # todo: row formats

        # todo: hent records count i en av de andre funksjonene

        sums = [] # todo: self.get_sums(join, condition, order_by)

        # todo: Don't let fields be reference to self.fields
        fields = json.loads(json.dumps(self.fields))

        # todo: replace field.name with field.alias

        data = {
            'name': self.name,
            'records': recs,
            'count_records': 1, # count_records, # todo
            'fields': fields,
            'grid': {
                'columns': self.grid['columns'],
                'sums': sums,
                'sort_columns': self.grid['sort_columns']
            },
            'form': { # todo: kun ett attributt
                'items': None if 'items' not in self.form else self.form['items']
            },
            'permission': { # todo: hent fra funksjon
                'view': 1,
                'add': 1,
                'edit': 1,
                'delete': 1
            },
            'type': self.type,
            'primary_key': self.primary_key,
            'foreign_keys': self.foreign_keys,
            'label': self.name.title() if not getattr(self, 'label', None) else self.label,
            'actions': getattr(self, 'actions', []),
            'limit': self.limit,
            'offset': self.offset,
            'selection': 0, # todo row_idx
            'conditions': [], # todo: self.client_conditions,
            'date_as_string': {'separator': '-'}, # todo wtf
            'expansion_column': None, # todo
            'relations': self.get_relations(),
            'saved_filters': [] # todo: self.get_saved_filters()
        }

        return data

    def get_parent_fk(self):
        return {}

    def get_joins(self):
        # todo: Funksjonen er for lang
        joins = []
        for alias, field in self.fields.items():
            if alias not in self.foreign_keys or 'view' not in field:
                continue

            fk = self.foreign_keys[alias]

            # todo: Skal jeg kreve at fk['schema'] er satt i skjema?
            # todo: Omtrent samme kode har jeg i get_conditions
            #       så lag en funksjon
            if 'schema' not in fk or fk['schema'] == self.db.schema:
                fk['schema'] = self.db.schema
                ref_schema = self.db.schema
                ref_base = self.db
            else:
                ref_schema = Schema(fk['schema'])
                ref_base_name = ref_schema.get_db_name()
                ref_base = Database(ref_base_name)
            
            # Get view for reference table
            table = Table(ref_base, fk['table'])
            view = table.get_view()

            # Check if user has permission to view table
            # todo: Har ingenting i denne funksjonen å gjøre
            #       Finn ut hvor jeg skal flytte den
            #       Har foreløpig kommentert ut koden
            # permission = table.get_user_permission() # todo
            # if permission['view'] == False:
            #     field['expandable'] = False
            
            # Makes conditions for the ON statement in the join
            # todo: Prøv å bruke list comprehension isteden
            conditions = []
            for idx, col in enumerate(fk['local']):
                ref_field_name = fk['foreign'][idx]
                conditions.append(alias + '.' + ref_field_name + ' = ' + self.name + '.' + col)
            conditions_list = ' AND '.join(conditions)

            joins.append("left join %s %s on %s" % (view, alias, conditions_list))

        # Joins extension tables
        # todo: Må kunne gå gjennom skjemaet istedenfor
        #       å opprette Table for hver bidige tabell
        for tbl_name in self.extension_tables:
            table = Table(self.db, tbl_name)
            view = table.get_view()
            conditions = []
            # todo: Prøv å bruke list comprehension isteden
            for idx, field in table.primary_key.items():
                conditions.append(tbl_name + '.' + field + ' = ' + self.name + '.' + self.primary_key[idx])
            
            joins.append("left join %s %s on %s" % (view, tbl_name, ' and '.join(conditions)))
        
        return joins

    def get_sort_fields(self, selects):
        sort_fields = {}
        for sort in self.grid['sort_columns']:
            # Split into field and sort order
            parts = sort.split(' ')
            key = parts[0]
            direction = 'asc' if len(parts) == 1 else parts[1]
            sort_fields[key] = {'field': self.name + "." + key}
            if key in selects:
                sort_fields[key]['field'] = selects[key]
            sort_fields[key]['order'] = direction
        
        return sort_fields


    def make_order_by(self, selects):
        if 'sort_columns' in self.grid or len(self.primary_key):
            order_by = "order by "
            sort_fields = self.get_sort_fields(selects)

            for key, sort in sort_fields.items():
                if self.db.system == 'mysql':
                    order_by += "isnull(%s), %s %s, " % (sort['field'], sort['field'], sort['order'])
                elif self.db.system in ['oracle', 'postgres']:
                    order_by += "%s %s, " % (sort['field'], sort['order'])
                elif self.db.system == 'sqlite':
                    order_by += "%s is null, %s %s, " % (sort['field'], sort['field'], sort['order'])
            
            for field in self.primary_key:
                order_by += "%s.%s, " % (self.name, field)

            order_by = order_by[0:-2]

            if self.db.system in ['oracle', 'postgres']:
                order_by += " nulls last"

            return order_by
            
        return ''

    def get_sums(self):
        return []
    
    def get_form(self):
        form = {}
        for key, field in self.fields.items():
            if 'table' not in field:
                field['table'] = self.name
        
        form['items'] = [key for key, field in self.fields.items() if field['table'] == self.name]

        for tbl_name in self.extension_tables:
            item = {'label': tbl_name}
            item['items'] = [key for key, field in self.fields.items() if field['table'] == tbl_name]
            form['items'].append(item)

        for key in self.relations:
            form['items'].append("relations." + key)
        
        return form

    def get_relations(self): 
        # todo: Skal filtreres på permission
        return self.relations

    def get_user_permission(self, tbl_name):
        # todo: Når behøver jeg å angi tbl_name?
        roles = self.db.get_user_roles()

        if len(roles) == 0: roles =[0]

        sql = """
        --sql
        select rp.*
        from role_permission rp 
        inner join (
            select max(schema_) schema_, max(role_) role_,
                   max(table_) table_
            from role_permission
            where schema_ in (:schema, '*')
              and table_ in (:table, '*')
              and role_ in :roles
            group by role_
        ) rp2 on rp.role_ = rp2.role_ and rp.schema_ = rp2.schema_
        and rp.table_ = rp2.table_;
        """

        query = SQLParams('named', 'qmark')
        sql, params = query.format(sql, {
            'schema': self.db.schema,
            'table': tbl_name,
            'roles': tuple(roles)
        })

        cursor = self.db.urd.cursor()
        rows = cursor.execute(sql, params).fetchall()

        permission = {
            'view': False,
            'add': False,
            'edit': False,
            'delete': False,
            'admin': False
        }

        for row in rows:
            if row.view_  : permission['view']   = True
            if row.add_   : permission['add']    = True
            if row.edit   : permission['edit']   = True
            if row.delete_: permission['delete'] = True
            if row.admin  : permission['admin']  = True

        # todo: Kode hvis ingen permission er gitt. Merkelig

        if self.db.schema == 'urd':
            admin_schemas = self.db.get_user_admin_schemas() # todo

            if len(admin_schemas):
                # todo: definer listen med navn som beskriver hva dette er
                if self.name in ['filter', 'format', 'role', 'role_permission', 'user_role']:
                    self.add_condition(self.name + ".schema_ in ('" + "','".join(admin_schemas) + "')", False)

                # todo: Merkelig å gjenta nesten samme lista
                #       Må iallfall kunne forenkle dette
                if self.name in ['filter', 'format', 'role', 'role_permission', 'user_', 'user_role']:
                    permission['view'] = 1
                    permission['add'] = 1
                    permission['edit'] = 1
                    permission['delete'] = 1

        if self.type == 'reference' and permission['admin'] == 0:
            permission['view'] = 0

        return permission

    def add_condition(self, condition, client=True):
        self.conditions.append(condition)

        if (client):
            self.client_conditions.append(condition)

    def get_conditions(self):
        return self.client_conditions
    
    def get_record_count(self, condition, join=''):
        view = self.get_view()
        sql = "select count(*) \n"
        sql+= "  from %s %s \n" % (view, self.name)
        sql+= join + "\n"
        sql+= condition

        cursor = self.db.cnxn.cursor()
        count = cursor.execute(sql).fetchval()
        
        return count