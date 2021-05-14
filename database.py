import pyodbc
import os
from schema import Schema
from config import config
from expression import Expression
from addict import Dict

class Database:
    def __init__(self, db_name):
        cnxnstr = config['db']['connection_string']
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
        self.system = base.system
        self.expr   = Expression(self.system)

    def get_info(self):

        branch = os.system('git rev-parse --abbrev-ref HEAD')
        branch = branch if branch else ''

        params = ['admin'] # todo: Autentisering

        q = """ 
        select count(*) from role_permission
        where role_ in (select role_ from user_role where user_ = ?)
        and admin = true
        """

        if self.schema == 'urd':
            q += " and (schema_ = '*' or schema_ = ?)"
            params.append(self.schema)
        
        cursor = self.urd.cursor()
        is_admin = cursor.execute(q, params).fetchval()

        self.user = {'admin': is_admin}

        info = {
            "base": {
                "name": self.name, 
                "schema": self.schema,
                "label": self.label,
                # todo: branch should not be connected to base
                "branch": branch,
                "tables": self.get_tables(),
                "reports": {}, # todo
                "contents": self.get_contents(),
                #'contents': self.contents
            },
            "user": {
                "name": 'Admin', # todo: Autentisering
                "id": 'admin', # todo: Autentisering
                "admin": is_admin
            }
        }

        return info

    def get_user_admin_schemas(self):
        user = 'admin' # todo: Autentisering

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
        user = 'admin' # todo: autentisering

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
            elif self.schema == 'urd' and self.user['admin'] and key in ['filter', 'format', 'role', 'role_permission', 'user_']:
                view = True
            
            if not view: continue

            # todo: Don't exposer filter to client

            if key in filters:
                table['default_filter'] = filters[key]
                # todo: Replace variables

            tables[key] = table
        
        return tables

    def get_tables(self):
        cursor = self.cnxn.cursor()
        tables = Dict()

        rows = cursor.tables().fetchall()
        
        for tbl in rows:
            tbl_name = tbl.table_name
            pk = [row.column_name for row in cursor.primaryKeys(tbl_name)]

            table = Dict({
                'name': tbl_name,
                'icon': None,
                'label': self.get_label(tbl_name),
                'primary_key': pk,
                'description': tbl.remarks,
                'hidden': False
            })

            table.indexes = self.get_indexes(tbl_name)

            table.fields = self.get_fields(table)
            table.type = self.get_table_type(table)

            table.foreign_keys = self.get_foreign_keys(tbl_name)

            # table.relations = self.get_relations(tbl_name)

            tables[tbl_name] = table

        self.tables = tables
        return tables

    def get_fields(self, table):
        fields = Dict()
        cursor = self.cnxn.cursor()
        for col in cursor.columns(table=table.name):
            cname = col.column_name
            type_ = self.expr.to_urd_type(col.type_name)
            urd_col = Dict({
                'name': cname,
                'datatype': type_
            })

            fields[cname] = urd_col
        
        return fields

            
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
        
        for fk in table.foreign_keys.values():
            if fk.table not in self.tables: continue

            # Not top level if has foreign keys to other table
            # that is not a reference table
            if fk.table != table.name:
                fk_table = self.tables[fk.table]
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
        terms = Dict() # todo: lag self.terms
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

            # todo: Legg til get_table_type isteden
            last_pk_col = table.primary_key[-1]
            if last_pk_col in table.foreign_keys: # todo extends
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
        terms = Dict() # todo
        if term in terms:
            label = terms[term].label
        else:
            label = term.replace("_", " ")
        
        norwegian_chars = True # todo
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
        
        return contents

    def get_indexes(self, tbl_name):
        cursor = self.cnxn.cursor()
        indexes = Dict()
        for row in cursor.statistics(tbl_name):
            name = row.index_name

            if name not in indexes:
                indexes[name] = Dict({
                    'name': name,
                    'unique': not row.non_unique,
                    'columns': []
                })

            indexes[name].columns.append(row.column_name)

        return indexes

    def get_foreign_keys(self, tbl_name):
        cursor = self.cnxn.cursor()
        foreign_keys = {}
        keys = {}

        for row in cursor.foreignKeys(foreignTable=tbl_name):
            name = row.fk_name
            if name not in keys:
                keys[name] = Dict({
                    'name': name,
                    'table': row.pktable_name,
                    # todo: Dette er "databasen"
                    'schema': row.pktable_cat,
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

        return foreign_keys

    def get_relations(self, tbl_name):
        cursor = self.cnxn.cursor()
        relations = Dict()
        foreign_keys = Dict()
        keys = Dict()

        for row in cursor.foreignKeys(table=tbl_name):
            name = row.pk_name
            if name not in keys:
                keys[name] = Dict({
                    'name': name,
                    'table': row.fktable_name,
                    # todo: Dette er "databasen"
                    'schema': row.fktable_cat,
                    'local': [],
                    'foreign': []
                })
            keys[name].local.append(row.pkcolumn_name)
            keys[name].foreign.append(row.fkcolumn_name)

        for fk in keys.values():
            alias = fk.foreign[-1]
            if alias in foreign_keys:
                alias = alias + "_2"
            foreign_keys[alias] = fk

            relations[fk.name] = Dict({
                'table': fk.table,
                'foreign_key': alias,
                'label': "todo"
            })

        return relations

    def get_reports(self): # todo
        return {}

    def query(self, sql, params=[]):
        cursor = self.cnxn.cursor()
        return cursor.execute(sql, params)

