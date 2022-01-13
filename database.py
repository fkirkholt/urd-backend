import pyodbc
from fastapi import HTTPException
from starlette import status
import os
import simplejson as json
from schema import Schema
from expression import Expression
from addict import Dict
import time

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
        try:
            cnxn = pyodbc.connect(cnxnstr)
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication"
            )
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
        self.user_tables = self.get_user_tables()
        self.metadata = self.get_metadata()
        self.config = Dict()

    def get_metadata(self):
        if not hasattr(self, 'metadata'):
            self.init_metadata()
        return self.metadata

    def init_metadata(self):
        start = time.time()
        from table import Table
        cursor = self.cnxn.cursor()
        metadata = Dict()
        if '_meta_data' in self.user_tables:
            sql = f"select * from {self.schema or self.cat}._meta_data"
            row = cursor.execute(sql).fetchone()
            colnames = [col[0] for col in cursor.description]
            metadata = Dict(zip(colnames, row))

        if metadata.cache:
            metadata.cache = Dict(json.loads(metadata.cache))
        end = time.time()
        print('init_metadata:', end - start)
        self.metadata = metadata

    def get_terms(self):
        if not hasattr(self, 'terms'):
            self.init_terms()
        return self.terms

    def init_terms(self):
        from table import Table
        cursor = self.cnxn.cursor()
        terms = Dict()
        if '_meta_terms' in self.user_tables:
            sql = f"select * from {self.schema or self.cat}._meta_term"
            try:
                rows = cursor.execute(sql).fetchall()
                colnames = [column[0] for column in cursor.description]
                for row in rows:
                    terms[row.term] = Dict(zip(colnames, row))
            except:
                pass

        self.terms = terms

    def get_info(self):
        start = time.time()

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
                "admin": self.get_privileges().create
            },
            "config": None if not self.metadata.get('cache', None) else self.metadata.cache.config
        }
        end = time.time()
        print('get_info', end - start)

        return info

    def get_privileges(self):
        start = time.time()
        privilege = Dict()
        sql = self.expr.privilege()
        cursor = self.cnxn.cursor()

        if not sql:
            privilege.create = 0
        else:
            priv = cursor.execute(sql, self.schema or self.cat).fetchone()
            privilege.create = int(priv.create)
            privilege.usage = 0
        end = time.time()
        print('get_privileges', end - start)

        return privilege


    def get_schemata(self):
        start = time.time()
        cursor = self.cnxn.cursor()
        schemata = []

        if self.cnxn.system == 'postgres':
            sql = self.expr.schemata()
            rows = cursor.execute(sql).fetchall()
            for row in rows:
                schemata.append(row.schema_name)
        end = time.time()
        print('get_schemata', end - start)

        return schemata


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

    def get_user_tables(self):
        start = time.time()
        sql = self.expr.user_tables()
        cursor = self.cnxn.cursor()
        user_tables = []

        rows = cursor.execute(sql, self.schema or self.cat).fetchall()
        for row in rows:
            user_tables.append(row.table_name)
        end = time.time()
        print('get_user_tables', end - start)

        return user_tables

    def get_tables(self):
        start_function = time.time()
        from table import Table
        if (self.metadata.get('cache', None) and not self.config):
            self.tables = self.metadata.cache.tables
            return self.tables
        cursor = self.cnxn.cursor()
        tables = Dict()

        start = time.time()
        rows = cursor.tables(catalog=self.cat, schema=self.schema).fetchall()
        end = time.time()
        print('cursor.tables', end - start)

        for tbl in rows:
            tbl_name = tbl.table_name

            if tbl_name not in self.user_tables:
                continue

            if (
                tbl_name[0:1] == "_" or
                tbl_name[0:4] == "ref_" or
                tbl_name[:-4] == "_ref"
            ):
                hidden = True
            else:
                hidden = False

            # Hides table if user has marked the table to be hidden
            if 'hidden' in self.config.tables[tbl_name]:
                print('skjuler ' + tbl_name)
                if hidden != self.config.tables[tbl_name].hidden:
                    hidden = self.config.tables[tbl_name].hidden
                else:
                    del self.config.tables[tbl_name].hidden
                    if not self.config.tables[tbl_name]:
                        del self.config.tables[tbl_name]

            if self.config:
                table = Table(self, tbl_name)
                table.rowcount = self.query(f"select * from {tbl_name}").rowcount
                table.fields = table.get_fields()

            tables[tbl_name] = Dict({
                'name': tbl_name,
                'icon': None,
                'label': self.get_label(tbl_name),
                'rowcount': None if not self.config.count_rows else table.rowcount,
                'primary_key': self.get_pkey(tbl_name),
                'description': tbl.remarks,
                'indexes': self.get_indexes(tbl_name),
                'foreign_keys': self.get_foreign_keys(tbl_name),
                'relations': self.get_relations(tbl_name),
                'hidden': hidden,
                # fields are needed only when creating cache
                'fields': None if ('cache' not in self.metadata and not self.config)
                           else table.fields,
            })

        if ('cache' in self.metadata and self.config):
            cursor = self.cnxn.cursor()
            # self.cache = tables
            sql = "update _meta_data set cache = ?\n"
            sql+= "where _name = ?"
            cache = {
                "tables": tables,
                "config": self.config
            }
            result = cursor.execute(sql, json.dumps(cache), self.name).commit()

        self.tables = tables
        end_function = time.time()
        print('get_tables', end_function - start_function)
        return tables

    def is_top_level(self, table):
        if table.hidden is True:
            return False

        for fkey in table.foreign_keys.values():
            if fkey.table not in self.tables:
                continue

            # Not top level if has foreign keys to other table
            # that is not a hidden table
            if fkey.table != table.name:
                fk_table = self.tables[fkey.table]
                if fk_table.hidden is False:
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

            if relation.table not in relation_tables:
                relation_tables.append(relation.table)
                relation_tables = self.get_relation_tables(
                    relation.table, relation_tables)

        return relation_tables

    def get_tbl_groups(self):
        """Group tables by prefix"""
        tbl_groups = Dict()
        terms = Dict() #TODO: lag self.terms
        for tbl_key, table in self.tables.items():
            if tbl_key[0:1] == "_":
                name = tbl_key[1:]
            else:
                name = tbl_key
            group = name.split("_")[0]

            # Find if the table is subordinate to other tables
            # i.e. the primary key also has a foreign key
            subordinate = False
            if not table.primary_key:
                subordinate = True

            for colname in table.primary_key:
                if colname in table.foreign_keys:
                    subordinate = True
                    break

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
            name_parts = tbl_key.split("_")

            for colname in table.primary_key:
                if colname in table.foreign_keys:
                    key = table.foreign_keys[colname]

                    if (len(name_parts) > 1 and
                        name_parts[0] in self.tables and
                        name_parts[0] != key.table
                    ):
                        continue

                    if key.table not in sub_tables:
                        sub_tables[key.table] = []

                    sub_tables[key.table].append(tbl_key)
                    break

        return sub_tables

    def get_label(self, term):
        terms = self.get_terms()
        if term in terms:
            label = terms[term].label
        else:
            label = term.replace("_", " ")

        if self.config.norwegian_chars:
            label = label.replace("ae", "æ")
            label = label.replace("oe", "ø")
            label = label.replace("aa", "å")

        label = label.capitalize()

        return label

    def get_description(self, term):
        terms = self.get_terms()
        description = None
        if term in terms:
            description = terms[term].description

        return description

    def get_content_item(self, tbl_name):

        if tbl_name not in self.sub_tables:
            node = "tables." + tbl_name
        else:
            node = Dict()
            node.item = "tables." + tbl_name
            node.subitems = Dict()

            for subtable in self.sub_tables[tbl_name]:
                label = subtable.replace(tbl_name + '_', '')
                label = self.get_label(label)
                node.subitems[label] = self.get_content_item(subtable)

        return node

    def get_module_item(self, tbl_name, contents):
        label = self.get_label(tbl_name)
        placed = False
        for idx, module in enumerate(self.modules):
            if len(module) > 2 and tbl_name in module:
                mod = "Modul " + str(idx + 1)
                contents[mod].class_label = "b"
                contents[mod].class_content = "ml3"
                contents[mod].subitems[label] = self.get_content_item(tbl_name)
                if 'count' not in contents[mod]:
                    contents[mod].count = 0
                contents[mod].count += 1
                placed = True

        if not placed:
            if 'Andre' not in contents:
                contents['Andre'] = Dict({
                    'class_label': "b",
                    'class_content': "ml3",
                    'subitems': {},
                    'count': 0
                })
            contents['Andre'].subitems[label] = self.get_content_item(tbl_name)
            contents['Andre'].count += 1

        return contents

    def get_contents(self):
        if (self.metadata.get('cache', None) and not self.config):
            self.contents = self.metadata.cache.contents
            return self.contents
        start = time.time()
        contents = Dict()

        modules = []
        for table in self.tables.values():
            top_level = self.is_top_level(table)
            if top_level:
                modules = self.add_module(table, modules)

        # Sort modules so that modules with most tables are listed first
        modules.sort(key=len, reverse=True)
        self.modules = modules

        tbl_groups = self.get_tbl_groups()
        self.sub_tables = self.get_sub_tables()

        for group_name, table_names in tbl_groups.items():
            if len(table_names) == 1 and group_name != "meta":
                tbl_name = list(table_names.values())[0]
                label = self.get_label(tbl_name)

                if not self.config or self.config.urd_structure:
                    contents[label] = self.get_content_item(tbl_name)
                else:
                    # group contents in modules
                    self.get_module_item(tbl_name, contents)

            elif group_name in table_names.values():
                table_names = {key:val for key, val in table_names.items() if val != group_name}
                if group_name in self.sub_tables:
                    self.sub_tables[group_name].extend(table_names.values())
                else:
                    self.sub_tables[group_name] = table_names.values()

                if not self.config or self.config.urd_structure:
                    label = self.get_label(group_name)
                    contents[label] = self.get_content_item(group_name)
                else:
                    self.get_module_item(group_name, contents)

            else:
                label = self.get_label(group_name)

                contents[label] = {
                    'class_label': "b",
                    'class_content': "ml3",
                    'subitems': table_names
                }
        end = time.time()
        print('get_contents', end - start)

        if ('cache' in self.metadata and self.config):
            cursor = self.cnxn.cursor()
            # self.cache = tables
            sql = "update _meta_data set cache = ?\n"
            sql+= "where _name = ?"
            cache = {
                "tables": self.tables,
                "contents": contents,
                "config": self.config
            }
            result = cursor.execute(sql, json.dumps(cache), self.name).commit()

        return contents

    def get_indexes(self, tbl_name):
        if not hasattr(self, 'indexes'):
            self.init_indexes()

        return self.indexes[tbl_name]

    def get_pkey(self, tbl_name):
        if not hasattr(self, 'pkeys'):
            self.init_pkeys()

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
        start = time.time()
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
        end = time.time()
        print('init_indexes', end - start)

        self.indexes = indexes

    def init_pkeys(self):
        start = time.time()
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
        end = time.time()
        print('init_pkeys', end - start)

        self.pkeys = pkeys

    def init_foreign_keys(self):
        start = time.time()
        cursor = self.cnxn.cursor()
        fkeys = Dict()
        foreign_keys = Dict()
        if self.cnxn.system in ["oracle", "postgres"]:
            sql = self.expr.fkeys()
            for row in cursor.execute(sql, self.schema):
                name = row.fk_name
                fkeys[row.fktable_name][name].name = row.fk_name
                fkeys[row.fktable_name][name].table = row.pktable_name
                fkeys[row.fktable_name][name].base = self.cat
                fkeys[row.fktable_name][name].schema = row.pktable_schema
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
        end = time.time()
        print('init_fkeys', end - start)

        self.fkeys = foreign_keys

    def init_relations(self):
        start = time.time()
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
                        "foreign": key.foreign,
                        "primary": key.primary,
                        "label": self.get_label(key.table) #TODO: Fix
                    })
        end = time.time()
        print('init_relations', end - start)

        self.relations = relations
