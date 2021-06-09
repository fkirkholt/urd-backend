import pyodbc
import os
import json
from schema import Schema
from config import config
from expression import Expression
from addict import Dict

class Database:
    def __init__(self, db_name):
        cnxnstr = config['db']['connection_string']
        pyodbc.lowercase = True
        urd_cnxn = pyodbc.connect(cnxnstr)
        cursor = urd_cnxn.cursor()
        cursor.execute("select * from database_ where name = ? or alias = ?", db_name, db_name)
        base = cursor.fetchone()
        self.cnxn   = pyodbc.connect(base._connection_string)
        self.urd    = urd_cnxn
        self.name   = base.name 
        self.alias  = base.alias
        self.label  = base.label
        self.schema = base.schema_
        if base.system == 'mysql':
            self.cat = base.name
            self.schema = None
        else:
            self.cat = None
        self.system = base.system
        self.cache = None if not base.cache else Dict(json.loads(base.cache))
        self.use_cache = base.use_cache
        self.expr   = Expression(self.system)

    def get_info(self):

        branch = os.system('git rev-parse --abbrev-ref HEAD')
        branch = branch if branch else ''

        params = ['admin'] #TODO: Autentisering

        sql = """
        select count(*) from role_permission
        where role_ in (select role_ from user_role where user_ = ?)
        and admin = true
        """

        if self.schema == 'urd':
            sql += " and (schema_ = '*' or schema_ = ?)"
            params.append(self.schema)
        
        cursor = self.urd.cursor()
        is_admin = cursor.execute(sql, params).fetchval()

        self.user = {'admin': is_admin}

        info = {
            "branch": branch,
            "base": {
                "name": self.name, 
                "schema": self.schema,
                "label": self.label,
                "tables": self.get_tables(),
                "reports": {}, #TODO
                "contents": self.get_contents(),
                #'contents': self.contents
            },
            "user": {
                "name": 'Admin', #TODO: Autentisering
                "id": 'admin', #TODO: Autentisering
                "admin": is_admin
            }
        }

        return info

    def get_user_admin_schemas(self):
        user = 'admin' #TODO: Autentisering

        sql = """
        select schema_
        from role_permission
        where role_ in (select role_ from user_ where user_ = ?)
          and admin = '1'
        """

        cursor = self.urd.cursor()
        rows = cursor.execute(sql, user).fetchall()

        return [row.schema_ for row in rows]

    def view_rights(self, user):
        """ Find the tables the user has permission to view"""

        sql = """
        select table_, view_
        from role_permission r
        where role_ in (select role_ from user_role where user_ = ?)
          and (schema_ = '*' or schema_ = ?)
        """

        cursor = self.urd.cursor()
        rows = cursor.execute(sql, user, self.schema).fetchall()
        return {row.table_: row.view_ for row in rows}

    def filter_tables(self):
        user = 'admin' #TODO: autentisering

        rights = self.view_rights(user)

        sql = """
        select table_, expression exp
        from filter f
        where schema_ = ?
          and user_ in (?, 'urd')
          and standard = '1'
        """

        cursor = self.urd.cursor( )

        rows = cursor.execute(sql, (self.schema, user)).fetchall()
        filters = {row.table_: row.exp for row in rows}

        # Make array of tables the user has access to
        tables = {}
        for key, table in self.tables.items():
            if 'label' not in table:
                table['label'] = table['name'].replace("_", " ").capitalize()

            # Don't show tables the user doesn't have access to
            view = False
            if key in rights:
                view = rights[key]
            elif '*' in rights:
                view = rights['*']

            # Allow admins access to some tables in urd
            urd_tables = ['filter', 'format', 'role', 'role_permission', 'user_']
            #TODO: Prøv å sjekke på navn på urd-tabellen, slik registrert i config
            if self.schema == 'urd' and self.user['admin'] and key in urd_tables:
                view = True
            
            if not view: continue

            if key in filters:
                table['default_filter'] = filters[key]
                #TODO: Replace variables

            tables[key] = table
        
        return tables

    def get_tables(self):
        if (self.use_cache and self.cache):
            self.tables = self.cache
            return self.cache
        cursor = self.cnxn.cursor()
        tables = Dict()

        rows = cursor.tables(catalog=self.cat, schema=self.schema).fetchall()
        
        for tbl in rows:
            tbl_name = tbl.table_name

            table = Dict({
                'name': tbl_name,
                'icon': None,
                'label': self.get_label(tbl_name),
                'primary_key': self.get_pkey(tbl_name),
                'description': tbl.remarks,
                'hidden': False
            })

            table.indexes = self.get_indexes(tbl_name)

            table.fields = self.get_columns(tbl_name)
            table.foreign_keys = self.get_foreign_keys(tbl_name)
            table.relations = self.get_relations(tbl_name)
            table.type = self.get_table_type(table)

            tables[tbl_name] = table

        if (self.use_cache):
            cursor = self.urd.cursor()
            self.cache = tables
            sql = "update database_ set cache = ?\n"
            sql+= "where name = ?"
            result = cursor.execute(sql, json.dumps(tables), self.name).commit()

        self.tables = tables
        return tables

    def get_table_type(self, table):
        index_cols = []
        for index in table.indexes.values():
            if index.unique:
                index_cols = index_cols + index.columns

        if len(set(index_cols)) == len(table.fields):
            type_ = 'reference'
        elif table.name[0:4] == "ref_" or table.name[:-4] == "_ref" or table.name[0:5] == "meta_":
            type_ = "reference"
        else:
            type_ = "data"

        return type_

    def is_top_level(self, table):
        if table.type == "reference":
            return False
        
        for fkey in table.foreign_keys.values():
            if fkey.table not in self.tables: continue

            # Not top level if has foreign keys to other table
            # that is not a reference table
            if fkey.table != table.name:
                fk_table = self.tables[fkey.table]
                if fk_table.type != "reference":
                    return False
        
        return True

    def add_module(self, table, modules):
        rel_tables = self.get_relation_tables(table.name, [])
        rel_tables.append(table.name)

        module_id = None
        for idx, module in enumerate(modules):
            common = [val for val in rel_tables if val in module]
            if len(common):
                if module_id == None:
                    modules[idx] = list(set(module + rel_tables))
                    module_id = idx
                else:
                    modules[module_id] = list(set(module + modules[module_id]))
                    del modules[idx]
        
        if module_id == None:
            modules.append(rel_tables)

        return modules

    def get_relation_tables(self, table_name, relation_tables):
        table = self.tables[table_name]

        for relation in table.relations.values():
            if relation.get('hidden', False):
                continue

            if relation.table in relation_tables:
                relation_tables.append(relation.table)
                relation_tables = self.get_relation_tables(
                    relation.table, relation_tables)

        return relation_tables

    def get_tbl_groups(self):
        tbl_groups = Dict()
        terms = Dict() #TODO: lag self.terms
        for tbl_key, table in self.tables.items():
            group = tbl_key.split("_")[0]

            # Find if the table is subordinate to other tables
            # i.e. the primary key also has a foreign key
            subordinate = False
            if not table.primary_key:
                subordinate = True
            
            for colname in table.primary_key:
                if colname in table.foreign_keys:
                    subordinate = True

            if not subordinate:
                # Remove group prefix from label
                rest = tbl_key.replace(group+"_", "")
                if rest in terms:
                    label = terms[rest].label
                else:
                    label = rest.replace("_", " ").capitalize()
                
                tbl_groups[group][label] = tbl_key

        return tbl_groups

    def get_sub_tables(self):
        sub_tables = Dict()
        for tbl_key, table in self.tables.items():

            #TODO: Legg til get_table_type isteden
            if len(table.primary_key):
                last_pk_col = table.primary_key[-1]
                if last_pk_col in table.foreign_keys: #TODO extends
                    table.type = "xref"

            for colname in table.primary_key:
                if colname in table.foreign_keys:
                    key = table.foreign_keys[colname]

                    if table.type == "xref":
                        break

                    if key.table not in sub_tables:
                        sub_tables[key.table] = []

                    sub_tables[key.table].append(tbl_key)
                    break

        return sub_tables

    def get_label(self, term):
        terms = Dict() #TODO
        if term in terms:
            label = terms[term].label
        else:
            label = term.replace("_", " ")
        
        norwegian_chars = True #TODO
        if norwegian_chars:
            label = label.replace("ae", "æ")
            label = label.replace("oe", "ø")
            label = label.replace("aa", "å")
        
        label = label.capitalize()

        return label

    def get_content_items(self, tbl_alias, sub_tables, contents):
        label = self.get_label(tbl_alias)

        if tbl_alias not in sub_tables:
            contents[label] = "tables." + tbl_alias
        else:
            contents[label] = Dict()
            contents[label].item = "tables." + tbl_alias
            contents[label].subitems = Dict()

            for subtable in sub_tables[tbl_alias]:
                contents[label].subitems = self.get_content_items(
                    subtable, sub_tables, contents[label].subitems)

        return contents

    def get_contents(self):
        contents = Dict()
        modules = []
        for table in self.tables.values():
            top_level = self.is_top_level(table)
            if top_level:
                modules = self.add_module(table, modules)

        tbl_groups = self.get_tbl_groups()
        sub_tables = self.get_sub_tables()

        # Sort modules so that modules with most tables are listed first
        modules.sort(key=len, reverse=True)

        for group_name, table_names in tbl_groups.items():
            if len(table_names) == 1 and group_name != "meta":
                table_alias = list(table_names.values())[0]

                # Loop through modules to find which one the table belongs to
                placed = False

                contents = self.get_content_items(table_alias, sub_tables, contents)
            else:
                label = self.get_label(group_name)
                if label == "Ref":
                    label = "Oppslagstabeller"

                contents[label] = {
                    'class_label': "b",
                    'class_content': "ml3",
                    'subitems': table_names
                }
        
        return contents

    def get_indexes(self, tbl_name):
        if not hasattr(self, 'indexes'):
            self.init_indexes()

        return self.indexes[tbl_name]

    def get_columns(self, tbl_name):
        if not hasattr(self, 'columns'):
            self.init_columns()

        return self.columns[tbl_name]

    def get_pkey(self, tbl_name):
        if not hasattr(self, 'pkeys'):
            self. init_pkeys()

        return self.pkeys[tbl_name]

    def get_foreign_keys(self, tbl_name):
        if not hasattr(self, 'fkeys'):
            self.init_foreign_keys()

        return self.fkeys[tbl_name]

    def get_relations(self, tbl_name):
        if not hasattr(self, 'relations'):
            self.init_relations()

        return self.relations[tbl_name]

    def get_reports(self): #TODO
        return {}

    def query(self, sql, params=[]):
        cursor = self.cnxn.cursor()
        return cursor.execute(sql, params)

    def init_indexes(self):
        cursor = self.cnxn.cursor()
        indexes = Dict()
        if self.system in ["oracle"]:
            sql = self.expr.indexes()
            for row in cursor.execute(sql, self.schema):
                name = row.index_name

                indexes[row.table_name][name].name = name
                indexes[row.table_name][name].unique = not row.non_unique
                if not 'columns' in indexes[row.table_name][name]:
                    indexes[row.table_name][name].columns = []
                indexes[row.table_name][name].columns.append(row.column_name)
        else:
            for tbl in cursor.tables(catalog=self.cat, schema=self.schema):
                for row in cursor.statistics(tbl.table_name):
                    name = row.index_name
                    indexes[tbl.table_name][name].name = name
                    indexes[tbl.table_name][name].unique = not row.non_unique
                    if not 'columns' in indexes[tbl.table_name][name]:
                        indexes[tbl.table_name][name].columns = []
                    indexes[tbl.table_name][name].columns.append(row.column_name)

        self.indexes = indexes

    def init_columns(self):
        cursor = self.cnxn.cursor()
        columns = Dict()
        if self.system in ["oracle"]:
            sql = self.expr.columns()
            for col in cursor.execute(sql, self.schema, None):
                cname = col.column_name
                type_ = self.expr.to_urd_type(col.type_name)
                urd_col = Dict({
                    'name': cname,
                    'datatype': type_
                })

                columns[col.table_name][cname] = urd_col
        else:
            rows = cursor.tables(catalog=self.cat, schema=self.schema).fetchall()
            for row in rows:
                cols = cursor.columns(table=row.table_name, catalog=self.cat, schema=self.schema).fetchall()
                for col in cols:
                    cname = col.column_name
                    type_ = self.expr.to_urd_type(col.type_name)
                    urd_col = Dict({
                        'name': cname,
                        'datatype': type_
                    })

                    columns[row.table_name][cname] = urd_col

        self.columns = columns

    def init_pkeys(self):
        cursor = self.cnxn.cursor()
        pkeys = Dict()
        if self.system in ["oracle"]:
            sql = self.expr.pkeys()
            rows = cursor.execute(sql, self.schema, None)
            for row in rows:
                if row.table_name not in pkeys:
                    pkeys[row.table_name] = []
                pkeys[row.table_name].append(row.column_name)
        else:
           tbls = cursor.tables(catalog=self.cat, schema=self.schema).fetchall()
           for tbl in tbls:
               rows = cursor.primaryKeys(table=tbl.table_name, catalog=self.cat, schema=self.schema)
               pkey = [row.column_name for row in rows]
               pkeys[tbl.table_name] = pkey

        self.pkeys = pkeys

    def init_foreign_keys(self):
        cursor = self.cnxn.cursor()
        fkeys = Dict()
        foreign_keys = Dict()
        if self.system in ["oracle"]:
            sql = self.expr.fkeys()
            for row in cursor.execute(sql, self.schema):
                name = row.fk_name
                fkeys[row.fktable_name][name].name = row.fk_name
                fkeys[row.fktable_name][name].table = row.pktable_name
                fkeys[row.fktable_name][name].schema = row.pktable_cat #TODO: merkelig
                if not 'local' in fkeys[row.fktable_name][name]:
                    fkeys[row.fktable_name][name].local = []
                    fkeys[row.fktable_name][name].foreign = []
                fkeys[row.fktable_name][name].local.append(row.fkcolumn_name.lower())
                fkeys[row.fktable_name][name].foreign.append(row.pkcolumn_name.lower())

            for tbl_name, keys in fkeys.items():
                for fkey in keys.values():
                    alias = fkey.local[-1]
                    if alias in foreign_keys[tbl_name]:
                        alias = alias + "_2"
                    foreign_keys[tbl_name][alias] = fkey
        else:
            from table import Table
            rows = cursor.tables(catalog=self.cat, schema=self.schema).fetchall()
            for row in rows:
                tbl = Table(self, row.table_name)
                foreign_keys[row.table_name] = tbl.get_fkeys()

        self.fkeys = foreign_keys

    def init_relations(self):
        if not hasattr(self, 'fkeys'):
            self.init_foreign_keys()

        relations = Dict()

        for fktable_name, keys in self.fkeys.items():
            for alias, key in keys.items():
                if key.schema == self.schema:
                    relations[key.table][key.name] = Dict({
                        "table": fktable_name,
                        "foreign_key": alias,
                        "label": self.get_label(key.table) #TODO: Fix
                    })

        self.relations = relations
