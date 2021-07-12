import pyodbc
import os
import json
from schema import Schema
from expression import Expression
from addict import Dict

class Connection:
    def __init__(self, system, server, user, pwd, db_name=None):
        self.system = system
        driver = self.get_driver()
        cnxnstr = f'Driver={driver};'
        if (db_name and system != 'oracle'):
            path = db_name.split('.')
            cnxnstr += 'Database=' + path[0] + ';'
        if system == 'oracle':
            cnxnstr += "DBQ=" + server + ';'
        else:
            srv_parts = server.split(':')
            cnxnstr += 'Server=' + srv_parts[0] + ';'
            if len(srv_parts) == 2:
                cnxnstr += 'Port=' + srv_parts[1] + ';'
        cnxnstr += 'Uid=' + user + ';Pwd=' + pwd + ';'
        pyodbc.lowercase = True
        print('cnxnstr', cnxnstr)
        cnxn = pyodbc.connect(cnxnstr)
        self.cursor = cnxn.cursor
        self.user = user
        self.expr = Expression(self.system)
        self.string = cnxnstr

    def get_driver(self):
        drivers = [d for d in pyodbc.drivers() if self.system in d.lower()]

        return drivers[0]

    def get_databases(self):
        sql = self.expr.databases()
        rows = self.cursor().execute(sql).fetchall()
        result = []
        for row in rows:
            base = Dict()
            base.columns.name = row[0]
            base.columns.label = row[0].capitalize()
            result.append(base)

        if self.system == 'oracle':
            base = Dict()
            base.columns.name = self.user
            base.columns.label = self.user.capitalize()
            result.append(base)

        return result

class Database:
    def __init__(self, cnxn, db_name):
        self.cnxn   = cnxn
        self.name   = db_name
        if cnxn.system == 'mysql':
            self.cat = db_name
            self.schema = None
        elif cnxn.system == 'postgres':
            path = db_name.split('.')
            self.cat = path[0]
            self.schema = 'public' if len(path) == 1 else path[1]
        elif cnxn.system == 'oracle':
            self.cat = None
            self.schema = db_name
        else:
            self.schema = 'public'
            self.cat = None
        self.expr   = Expression(cnxn.system)
        self.use_cache = False #TODO
        self.metadata = self.get_metadata()

    def get_metadata(self):
        if not hasattr(self, 'metadata'):
            self.init_metadata()
        return self.metadata

    def init_metadata(self):
        cursor = self.cnxn.cursor()
        metadata = Dict()
        md_table= cursor.tables(table='meta_data', catalog=self.cat, schema=self.schema).fetchone()
        tables = cursor.tables(catalog=self.cat).fetchall()
        if (md_table):
            sql = f"select * from {self.schema or self.cat}.meta_data"
            rows = cursor.execute(sql).fetchall()
            for row in rows:
                if row.key_ == "cache" and row.value_:
                    metadata[row.key_] = Dict(json.loads(row.value_))
                else:
                    metadata[row.key_] = row.value_

        self.metadata = metadata

    def get_info(self):

        branch = os.system('git rev-parse --abbrev-ref HEAD')
        branch = branch if branch else ''

        info = {
            "branch": branch,
            "base": {
                "name": self.name,
                "system": self.cnxn.system,
                "schema": self.schema,
                "schemata": self.get_schemata(),
                "label": self.metadata.get('label', self.name.capitalize()),
                "tables": self.get_tables(),
                "reports": {}, #TODO
                "contents": self.get_contents(),
                "description": self.metadata.get('description', None),
                #'contents': self.contents
            },
            "user": {
                "name": 'Admin', #TODO: Autentisering
                "id": 'admin', #TODO: Autentisering
                "admin": self.get_privileges().usage
            }
        }

        return info

    def get_privileges(self):
        privilege = Dict()
        sql = self.expr.privilege()
        cursor = self.cnxn.cursor()

        if not sql:
            privilege.create = 0
        else:
            priv = cursor.execute(sql, self.schema or self.cat).fetchone()
            privilege.create = priv.create
            privilege.usage = 0

        return privilege


    def get_schemata(self):
        cursor = self.cnxn.cursor()
        schemata = []

        if self.cnxn.system == 'postgres':
            sql = self.expr.schemata()
            rows = cursor.execute(sql).fetchall()
            for row in rows:
                schemata.append(row.schema_name)

        return schemata


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
        from table import Table
        if self.metadata.get('cache', None):
            self.tables = self.metadata.cache
            return self.tables
        cursor = self.cnxn.cursor()
        tables = Dict()

        rows = cursor.tables(catalog=self.cat, schema=self.schema).fetchall()

        for tbl in rows:
            tbl_name = tbl.table_name

            table = Table(self, tbl_name)

            tables[tbl_name] = Dict({
                'name': tbl_name,
                'icon': None,
                'label': self.get_label(tbl_name),
                'primary_key': self.get_pkey(tbl_name),
                'description': tbl.remarks,
                'hidden': False,
                'indexes': self.get_indexes(tbl_name),
                'foreign_keys': self.get_foreign_keys(tbl_name),
                'relations': self.get_relations(tbl_name),
                'type': table.get_type(),
                # fields are needed only when creating cache
                'fields': None if 'cache' not in self.metadata
                           else self.get_columns(tbl_name),
            })


        if 'cache' in self.metadata:
            cursor = self.cnxn.cursor()
            self.cache = tables
            sql = "update meta_data set value_ = ?\n"
            sql+= "where key_ = ?"
            result = cursor.execute(sql, json.dumps(tables), 'cache').commit()

        self.tables = tables
        return tables

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

            #TODO: Legg til Table.get_type isteden
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
            elif group_name in table_names.values():
                table_names = {key:val for key, val in table_names.items() if val != group_name}
                if group_name in sub_tables:
                    sub_tables[group_name].extend(table_names.values())
                else:
                    sub_tables[group_name] = table_names.values()
                contents = self.get_content_items(group_name, sub_tables, contents)
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
        if self.cnxn.system in ["oracle"]:
            sql = self.expr.indexes()
            for row in cursor.execute(sql, self.schema):
                name = row.index_name

                indexes[row.table_name][name].name = name
                indexes[row.table_name][name].unique = not row.non_unique
                if not 'columns' in indexes[row.table_name][name]:
                    indexes[row.table_name][name].columns = []
                indexes[row.table_name][name].columns.append(row.column_name)
        else:
            tbls = cursor.tables(catalog=self.cat, schema=self.schema).fetchall()
            for tbl in tbls:
                for row in cursor.statistics(tbl.table_name):
                    name = row.index_name
                    indexes[tbl.table_name][name].name = name
                    indexes[tbl.table_name][name].unique = not row.non_unique
                    if not 'columns' in indexes[tbl.table_name][name]:
                        indexes[tbl.table_name][name].columns = []
                    indexes[tbl.table_name][name].columns.append(row.column_name)

        self.indexes = indexes

    def init_columns(self):
        from table import Table
        cursor = self.cnxn.cursor()
        rows = cursor.tables(catalog=self.cat, schema=self.schema).fetchall()
        columns = Dict()
        for row in rows:
            tbl = Table(self, row.table_name)
            columns[tbl.name] = tbl.get_fields()

        self.columns = columns

    def init_pkeys(self):
        cursor = self.cnxn.cursor()
        pkeys = Dict()
        if self.cnxn.system in ["oracle"]:
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
        if self.cnxn.system in ["oracle"]:
            sql = self.expr.fkeys()
            for row in cursor.execute(sql, self.schema):
                name = row.fk_name
                fkeys[row.fktable_name][name].name = row.fk_name
                fkeys[row.fktable_name][name].table = row.pktable_name
                fkeys[row.fktable_name][name].schema = row.pktable_cat #TODO: merkelig
                fkeys[row.fktable_name][name].delete_rule = row.delete_rule
                if not 'foreign' in fkeys[row.fktable_name][name]:
                    fkeys[row.fktable_name][name].foreign = []
                    fkeys[row.fktable_name][name].primary = []
                fkeys[row.fktable_name][name].foreign.append(row.fkcolumn_name.lower())
                fkeys[row.fktable_name][name].primary.append(row.pkcolumn_name.lower())

            for tbl_name, keys in fkeys.items():
                for fkey in keys.values():
                    alias = fkey.foreign[-1]
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
                        "name": key.name,
                        "table": fktable_name,
                        "base": key.base,
                        "schema": key.schema,
                        "foreign_key": alias,
                        "delete_rule": key.delete_rule,
                        "foreign": key.primary,
                        "primary": key.foreign,
                        "label": self.get_label(key.table) #TODO: Fix
                    })

        self.relations = relations
