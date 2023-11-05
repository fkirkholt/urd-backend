"""Module for handling databases and connections"""
import os
import time
from graphlib import TopologicalSorter
from sqlalchemy import text, inspect, bindparam, exc
import sqlglot
import simplejson as json
from addict import Dict
from settings import Settings
from table import Table
from grid import Grid
from user import User
from datatype import Datatype


class Database:
    """Contains methods for getting data and metadata from database"""

    def __init__(self, engine, db_name):
        self.engine = engine
        self.identifier = db_name
        path = db_name.split('.')
        if engine.name == 'postgresql':
            self.schema = 'public' if len(path) == 1 else path[1]
            self.cat = path[0]
        elif engine.name == 'mssql':
            self.schema = 'dbo' if len(path) == 1 else path[1]
            self.cat = path[0]
        elif engine.name == 'sqlite':
            self.schema = 'main'
            self.cat = None
        else:
            self.schema = db_name
            self.cat = None

        self.refl = inspect(engine)

        self.user = User(engine)
        self.html_attrs = self.init_html_attributes()
        attrs = Dict(self.html_attrs.pop('base', None))
        self.cache = attrs.pop('data-cache', None)
        if attrs.get('cache.config', None):
            self.config = self.cache.config
        else:
            self.config = Dict(Settings())

    def init_html_attributes(self):
        """Get data from table html_attributes"""
        attrs = Dict()
        if 'html_attributes' in self.tablenames:
            sql = f"""
            select selector, attributes as attrs
            from {self.schema}.html_attributes
            """
            try:
                with self.engine.connect() as cnxn:
                    rows = cnxn.execute(text(sql))
                for row in rows:
                    attrs[row.selector] = json.loads(row.attrs)
            except Exception as e:
                print(e)

        return attrs

    def filter_schema(self, schema):
        system_schemas = [
            'information_schema',
            'performance_schema',
            'mysql'
        ]
        if self.engine.name == 'postgresql' and schema.startswith('pg_'):
            return False
        elif schema in system_schemas:
            return False

        return True

    def get_info(self):
        """Get info about database"""

        branch = os.system('git rev-parse --abbrev-ref HEAD')
        branch = branch if branch else ''

        info = {
            "branch": branch,
            "base": {
                "name": self.identifier,
                "cat": self.cat,
                "system": self.engine.name,
                "server": self.engine.url.host,
                "schema": self.schema,
                "schemata": self.schemas,
                "label": self.get_label(self.identifier),
                "tables": self.get_tables(),
                "contents": self.get_contents(),
                "description": self.get_comment(),
                "html_attrs": self.html_attrs,
                "privilege": self.user.schema_privilege(self.schema)
            },
            "user": {
                "name": self.user.name,
                "admin": self.user.is_admin(self.schema)
            },
            "config": self.config
        }

        return info

    def get_comment(self):
        """Get database comment"""
        if self.engine.name in ['mysql', 'mariadb', 'postgresql']:
            user = User(self.engine)
            comment = user.databases(self.schema, self.cat)[0].db_comment
        else:
            comment = None

        return comment

    def create_html_attributes(self):
        """Create table holding html_attributes"""

        datatype = Datatype('str')
        string_datatype = datatype.to_native_type(self.engine.name)

        sql = f"""
        create table {self.schema}.html_attributes(
            selector varchar(100) not null,
            attributes {string_datatype} not null,
            primary key (selector)
        )
        """

        with self.engine.connect() as cnxn:
            cnxn.execute(text(sql))
            cnxn.commit()

        self.tablenames.append('html_attributes')
        attributes = {
            'data-type': 'json',
            'data-format': 'yaml'
        }

        val = json.dumps(attributes)

        sql = f"""
            insert into {self.schema}.html_attributes (selector, attributes)
            values ('[data-field="html_attributes.attributes"]', '{val}')
        """

        with self.engine.connect() as cnxn:
            cnxn.execute(text(sql))
            cnxn.commit()

        # Refresh attributes
        self.html_attrs = self.init_html_attributes()
        attrs = Dict(self.html_attrs.pop('base', None))
        self.cache = attrs.pop('data-cache', None)

    def get_tables(self):
        """Return metadata for every table"""

        # Return metadata from cache if set
        if (self.cache and not self.config.tables):
            self.tables = self.cache.tables
            return self.tables

        self.tables = Dict()

        if (
            self.config.update_cache and
            'html_attributes' not in self.tablenames
        ):
            self.create_html_attributes()

        tbl_names = self.refl.get_table_names(self.schema)
        view_names = self.refl.get_view_names(self.schema)

        for tbl_name in self.tablenames:
            if tbl_name[-5:] == '_view' and tbl_name[:-5] in self.tablenames:
                continue

            hidden = tbl_name[0:1] == "_"

            table = Table(self, tbl_name)
            grid = Grid(table)

            table.main_type = 'table' if tbl_name in (tbl_names) else 'view'

            # Hides table if user has marked the table to be hidden
            if 'hidden' in self.config.tables[tbl_name]:
                if hidden != self.config.tables[tbl_name].hidden:
                    hidden = self.config.tables[tbl_name].hidden
                else:
                    del self.config.tables[tbl_name].hidden
                    if not self.config.tables[tbl_name]:
                        del self.config.tables[tbl_name]

            # Change table type if set in config
            if 'type' in self.config.tables[tbl_name]:
                if table.type != self.config.tables[tbl_name].type:
                    table.type = self.config.tables[tbl_name].type
                else:
                    del self.config.tables[tbl_name].type
                    if not self.config.tables[tbl_name]:
                        del self.config.tables[tbl_name]

            if self.config.update_cache:
                table.rowcount = table.count_rows()
                space = ' ' * (30 - len(tbl_name))
                print('Table: ', f"{tbl_name}{space}({table.rowcount})")

            view = tbl_name
            if tbl_name + '_view' in view_names:
                view = tbl_name + '_view'

            if self.engine.name == 'sqlite' or tbl_name not in self.comments:
                comment = None
            else:
                comment = self.comments[tbl_name]

            self.tables[tbl_name] = Dict({
                'name': tbl_name,
                'type': table.type,
                'view': view,
                'icon': None,
                'label': self.get_label(tbl_name),
                'rowcount': (None if not self.config.update_cache
                             else table.rowcount),
                'pkey': table.pkey,
                'description': comment,
                'fkeys': table.fkeys,
                # Get more info about relations for cache, including use
                'relations': table.relations,
                'indexes': table.indexes,
                'hidden': hidden,
                # fields are needed only when creating cache
                'fields': (None if not self.config.update_cache
                           else table.fields),
                'grid': None if not self.config.update_cache else {
                    'columns': grid.columns
                }
            })

        return self.tables

    @property
    def schemas(self):
        if self.engine.name == 'postgresql' and not self.user.is_admin(self.schema):
            sql = """
            select table_schema
            from information_schema.role_table_grants
            where table_catalog = :cat
                and grantee = :user
            """

            params = {
                'cat': self.cat,
                'user': self.user.name
            }
            with self.engine.connect() as cnxn:
                rows = cnxn.execute(text(sql), params).fetchall()
            self._schemas = [row[0] for row in rows]
        else:
            self._schemas = list(filter(self.filter_schema,
                                 self.refl.get_schema_names()))

        return self._schemas

    @property
    def tablenames(self):
        if hasattr(self, '_tablenames'):
            return self._tablenames
        if self.engine.name == 'postgresql' and not self.user.is_admin(self.schema):
            sql = """
            select table_name
            from information_schema.role_table_grants
            where table_catalog = :cat
              and table_schema = :schema
              and grantee in (
                WITH RECURSIVE cte AS (
                   SELECT oid FROM pg_roles WHERE rolname = current_user

                   UNION ALL
                   SELECT m.roleid
                   FROM   cte
                   JOIN   pg_auth_members m ON m.member = cte.oid
                   )
                SELECT oid::regrole::text AS rolename FROM cte
            )
            """
            with self.engine.connect() as cnxn:
                params = {'cat': self.cat, 'schema': self.schema}
                rows = cnxn.execute(text(sql), params).fetchall()
            self._tablenames = [row[0] for row in rows]
        else:
            table_names = self.refl.get_table_names(self.schema)
            view_names = self.refl.get_view_names(self.schema)
            self._tablenames = table_names + view_names

        return self._tablenames

    @property
    def columns(self):
        if not hasattr(self, '_columns'):
            self._columns = Dict()
            columns = self.refl.get_multi_columns(self.schema)
            for (schema, table), cols in columns.items():
                self._columns[table] = cols

        return self._columns

    def is_top_level(self, table):
        """Check if table is top level, i.e. not subordinate to other tables"""
        if (table.type == 'list'):
            return False

        for fkey in table.fkeys.values():
            if fkey.referred_table not in self.tablenames:
                continue

            # Not top level if has foreign keys to other table
            # that is not a hidden table
            if fkey.referred_table != table.name:
                fk_table = self.tables[fkey.referred_table]
                if fk_table.hidden is False and fk_table.type != 'list':
                    return False

        return True

    def attach_to_module(self, table, modules):
        """Attach tables to module"""
        rel_tables = self.get_relation_tables(table.name, [])
        rel_tables.append(table.name)

        module_id = None
        for idx, module in enumerate(modules):
            common = [val for val in rel_tables if val in module]
            if len(common):
                if module_id is None:
                    modules[idx] = list(set(module + rel_tables))
                    module_id = idx
                else:
                    modules[module_id] = list(set(module + modules[module_id]))
                    del modules[idx]

        if module_id is None:
            modules.append(rel_tables)

        return modules

    def get_relation_tables(self, table_name, relation_tables):
        """Get all relation tables in hierarchy recursively"""
        table = self.tables[table_name]

        for relation in table.relations.values():
            if relation.get('hidden', False):
                continue

            if relation.table not in relation_tables:
                relation_tables.append(relation.table)
                relation_tables = self.get_relation_tables(
                    relation.table, relation_tables)

        return relation_tables

    def get_tbl_groups_urdr(self):
        """Group tables by prefix or relations

        If not generating cache or generating cache for databases
        with Urdr structure. This is the default behaviour, which
        treats databases as following the Urdr rules for self
        documenting databases
        """
        tbl_groups = Dict()
        i = 0
        for tbl_name, table in self.tables.items():
            i += 1
            if (tbl_name[-5:] == '_grid' and table.type == 'view'):
                continue
            if tbl_name[0:1] == "_":
                name = tbl_name[1:]
            else:
                name = tbl_name
            name = tbl_name[1:] if tbl_name[0:1] == "_" else tbl_name
            parts = name.split("_")

            # Don't include tables that are subordinate to other tables
            # i.e. the primary key also has a foreign key
            # These are handled in get_content_node
            tbl = Table(self, tbl_name)
            if tbl.is_subordinate():
                continue

            placed = False
            for group in tbl_groups:
                if name.startswith(group + '_'):
                    tbl_groups[group].append(tbl_name)
                    placed = True

            if not placed:
                group = None
                for part in parts:
                    test_group = group + '_' + part if group else part
                    if (
                        len(self.tables) > i and
                        list(self.tables)[i].startswith(test_group+'_')
                    ):
                        group = test_group
                    elif group is None:
                        group = part

                if not tbl_groups[group]:
                    tbl_groups[group] = []

                tbl_groups[group].append(tbl_name)

        return tbl_groups

    def get_table_groups(self):
        tbl_groups = Dict()
        # Group for tables not belonging to other groups
        tbl_groups['...'] = []

        for table in self.tables.values():
            top_level = self.is_top_level(table)
            if top_level:
                self.tables[table.name].top_level = True
                # modules = self.attach_to_module(table, modules)

                # Recursively get all tables under this top level table
                grouptables = self.get_relation_tables(table.name, [])

                if table.name not in grouptables:
                    grouptables.append(table.name)
                if len(grouptables) > 2:
                    tbl_groups[table.name] = grouptables
                else:
                    tbl_groups['...'].extend(grouptables)
            elif table.type == 'list':
                tbl_groups['...'].append(table.name)

        self.relocate_tables(tbl_groups)

        return tbl_groups

    def relocate_tables(self, tbl_groups):
        """Relocate tables between groups"""
        delete_groups = []
        for group_name, tbl_names in tbl_groups.items():
            for group_name2, tbl_names2 in tbl_groups.items():
                if group_name2 == group_name:
                    continue

                diff = set(tbl_names) - set(tbl_names2)
                common = [tbl_name for tbl_name in tbl_names
                          if tbl_name in tbl_names2]
                len_combined = len(set(tbl_names + tbl_names2))

                if len(common):
                    if (
                        len(tbl_names) <= len(tbl_names2) and
                        (len(diff) == 1 or len_combined < 15)
                    ):
                        tbl_groups[group_name2].extend(tbl_names)
                        delete_groups.append(group_name)
                        break
                    elif (
                        len(tbl_names) <= len(tbl_names2) and
                        len(common) == 1 and len(diff) > 1
                    ):
                        # We want the common tables only in the smallest
                        # group
                        tbl_groups[group_name2].remove(common[0])
                    elif (len(tbl_names) <= len(tbl_names2)):
                        for tbl_name in common:
                            tbl_groups[group_name2].remove(tbl_name)

        for group_name in delete_groups:
            del tbl_groups[group_name]

    def get_sub_tables(self):
        """Return Dict of tables with subordinate tables"""
        sub_tables = Dict()
        for tbl_name, table in self.tables.items():
            name_parts = tbl_name.split("_")
            tbl = Table(self, tbl_name)

            for colname in table.pkey.columns:
                fkey = tbl.get_fkey(colname)
                if fkey:
                    if (
                        len(name_parts) > 1 and
                        name_parts[0] in self.tables and
                        name_parts[0] != fkey.referred_table
                    ):
                        continue

                    if fkey.referred_table not in sub_tables:
                        sub_tables[fkey.referred_table] = []

                    sub_tables[fkey.referred_table].append(tbl_name)
                    break

        return sub_tables

    def get_label(self, identifier, prefix=None, postfix=None):
        """Get label based on identifier"""
        id_parts = identifier.split('_')
        if id_parts[-1] in ("list", "liste", "xref", "link"):
            identifier = "_".join(id_parts[:-1])
        if prefix:
            identifier = identifier.replace(prefix, '')
        if postfix:
            identifier = identifier.replace(postfix, '')
        label = identifier.replace('_', ' ')

        if self.config.norwegian_chars:
            label = label.replace("ae", "æ")
            label = label.replace("oe", "ø")
            label = label.replace("aa", "å")

        label = label.strip().capitalize()

        return label

    def get_content_node(self, tbl_name):
        """Return a node in the content list, based on a table"""
        if tbl_name not in self.sub_tables:
            node = "tables." + tbl_name
        else:
            node = Dict()
            node.item = "tables." + tbl_name
            node.subitems = Dict()

            for subtable in self.sub_tables[tbl_name]:
                label = self.get_label(subtable, prefix=tbl_name)
                node.subitems[label] = self.get_content_node(subtable)

        return node

    def get_contents(self):
        """Get list of contents"""
        if (self.cache and not self.config):
            self.contents = self.cache.contents
            return self.contents

        contents = Dict()

        if (not self.config.update_cache or self.config.urd_structure):
            tbl_groups = self.get_tbl_groups_urdr()
        else:
            tbl_groups = self.get_tbl_groups()

        self.sub_tables = self.get_sub_tables()

        for group_name, table_names in tbl_groups.items():
            if len(table_names) == 1:  # and group_name != "meta":
                tbl_name = table_names[0]
                label = self.get_label(tbl_name)

                contents[label] = self.get_content_node(tbl_name)

            else:
                label = self.get_label(group_name)
                table_names = list(set(table_names))

                contents[label] = Dict({
                    'class_label': "b",
                    'class_content': "ml3",
                    'count': len(table_names)
                })

                table_names.sort()
                for tbl_name in table_names:
                    # Remove group prefix from label
                    tbl_label = self.get_label(tbl_name, prefix=group_name)
                    if tbl_label == '':
                        tbl_label = self.get_label(tbl_name)

                    contents[label].subitems[tbl_label] = \
                        self.get_content_node(tbl_name)

        if self.config.update_cache:
            sql = """
            select count(*) from html_attributes
            where selector = :selector
            """
            count = self.query(sql, {'selector': 'base'}).first()[0]

            cache = {
                "tables": self.tables,
                "contents": contents,
                "config": self.config
            }
            attrs = {
                'data-cache': cache
            }
            attrs_txt = json.dumps(attrs)

            if count:
                sql = """
                update html_attributes
                set attributes = :attrs
                where selector = :selector
                """
            else:
                sql = """
                insert into html_attributes(attributes, selector)
                values (:attrs, :selector)
                """

            with self.engine.connect() as cnxn:
                params = {
                    'attrs': attrs_txt,
                    'selector': 'base'
                }
                cnxn.execute(text(sql), params)
                cnxn.commit()

        return contents

    @property
    def comments(self):
        self._comments = {}
        # SQLAlchemy reflection doesn't work for comments in mysql/mariadb
        if self.engine.name in ['mysql', 'mariadb']:
            sql = """
            select table_name, table_comment
            from   information_schema.tables
            where table_schema = :schema
            """
            rows = self.query(sql, {'schema': self.schema})
            for row in rows:
                self._comments[row.table_name] = row.table_comment
        else:
            rows = self.refl.get_multi_table_comment(self.schema)
            for (schema, table), row in rows.items():
                self._comments[table] = row['text']

        return self._comments

    @property
    def pkeys(self):
        """Get primary key of table"""
        if not hasattr(self, '_pkeys'):
            self._pkeys = Dict()
            pkey_constraints = self.refl.get_multi_pk_constraint(self.schema)
            for (schema, table), pkey in pkey_constraints.items():
                self._pkeys[table] = Dict({
                    'table_name': table,
                    'name': pkey['name'] or 'PRIMARY',
                    'unique': True,
                    'columns': pkey['constrained_columns']
                })

        return self._pkeys

    @property
    def indexes(self):
        if not hasattr(self, '_indexes'):
            self._indexes = Dict()
            schema_indexes = self.refl.get_multi_indexes(self.schema)

            for (schema, table), indexes in schema_indexes.items():

                for idx in indexes:
                    idx = Dict(idx)
                    idx.columns = idx.pop('column_names')

                    self._indexes[table][idx.name] = idx
                    pkey = self.pkeys[table]
                    self._indexes[table][pkey.name] = pkey

        return self._indexes

    @property
    def fkeys(self):
        """Get all foreign keys of table"""
        if not hasattr(self, '_fkeys'):
            self._fkeys = Dict()
            schema_fkeys = self.refl.get_multi_foreign_keys(self.schema)

            for key, fkeys in schema_fkeys.items():
                for fkey in fkeys:
                    fkey = Dict(fkey)
                    fkey.table = key[-1]

                    # Can't extract constraint names in SQLite
                    if not fkey.name:
                        fkey.name = fkey.table + '_'
                        fkey.name += '_'.join(fkey.constrained_columns)+'_fkey'

                    self._fkeys[fkey.table][fkey.name] = Dict(fkey)

        return self._fkeys

    @property
    def relations(self):
        """Get all has-many relations of table"""
        if not hasattr(self, '_relations'):
            self._relations = Dict()
            schema_fkeys = self.refl.get_multi_foreign_keys(self.schema)

            for key, fkeys in schema_fkeys.items():
                for fkey in fkeys:
                    fkey = Dict(fkey)
                    fkey.table = key[1]
                    fkey.schema = key[0] or self.db.schema
                    # Can't extract constraint names in SQLite
                    if not fkey.name:
                        fkey.name = fkey.table + '_'
                        fkey.name += '_'.join(fkey.constrained_columns)+'_fkey'
                    self._relations[fkey.referred_table][fkey.name] = fkey

        return self._relations

    def query_result(self, sql, limit):
        """Get query result for user defined sql"""
        query = Dict()
        sql = sql.strip()
        query.string = sql
        if len(query.string) == 0:
            return None
        t1 = time.time()
        with self.engine.connect() as cnxn:
            try:
                result = cnxn.execute(text(query.string))
            except exc.StatementError as ex:
                query.time = round(time.time() - t1, 4)
                query.success = False
                query.result = 'ERROR: {}'.format(ex.orig)

                return query

            query.success = True
            query.time = round(time.time() - t1, 4)

            # if cursor.description:
            if result.cursor:
                if limit:
                    query.data = result.mappings().fetchmany(limit)
                else:
                    query.data = result.mappings().fetchall()

                # Find the table selected from
                query.table = str(sqlglot.parse_one(sql)
                                  .find(sqlglot.exp.Table))
            else:
                rowcount = result.rowcount

                cnxn.commit()

                query.rowcount = rowcount
                query.result = f"Query OK, {rowcount} rows affected"

        return query

    def query(self, sql, params={}):
        """Execute sql query"""
        t = time.time()
        with self.engine.connect() as cnxn:
            stmt = text(sql)
            for col, val in params.items():
                if isinstance(val, list):
                    stmt = stmt.bindparams(bindparam(col, expanding=True))
            cursor = cnxn.execute(stmt, params)
        if (time.time()-t) > 1:
            print("Query took " + str(time.time()-t) + " seconds")
            print('query:', sql)
        return cursor

    def export_as_sql(self, dialect: str, include_recs: bool,
                      select_recs: bool):
        """Create sql for exporting a database

        Parameters:
        dialect: The sql dialect used (mysql, postgresql, sqlite)
        include_recs: If records should be included
        select_recs: If included records should be selected from
                     existing database
        """
        ddl = ''
        graph = {}
        self_referring = {}

        tbl_names = self.refl.get_table_names(self.schema)

        # Make graph to use in topologic sort
        for tbl_name in tbl_names:
            ref_tables = []
            fkeys = self.refl.get_foreign_keys(tbl_name, self.schema)
            for fkey in fkeys:
                if fkey['referred_table'] == tbl_name:
                    self_referring[tbl_name] = fkey
                    continue
                ref_tables.append(fkey['referred_table'])
            graph[tbl_name] = ref_tables

        sorter = TopologicalSorter(graph)
        ordered_tables = tuple(sorter.static_order())

        for view_name in self.refl.get_view_names(self.schema):
            if dialect == 'oracle':
                ddl += f"drop view {view_name};\n"
            else:
                ddl += f"drop view if exists {view_name};\n"

        for tbl_name in reversed(ordered_tables):
            if dialect == 'oracle':
                ddl += f"drop table {tbl_name};\n"
            else:
                ddl += f"drop table if exists {tbl_name};\n"

        for tbl_name in ordered_tables:
            if tbl_name is None:
                continue
            if tbl_name == 'sqlite_sequence':
                continue
            table = Table(self, tbl_name)
            ddl += table.export_ddl(dialect)
            if include_recs:
                self_ref = None
                if tbl_name in self_referring:
                    self_ref = self_referring[tbl_name]
                ddl += table.export_records(select_recs, self_ref)

        for view_name in self.refl.get_view_names(self.schema):
            view_def = self.refl.get_view_definition(view_name, self.schema)
            ddl += view_def + ";\n"

        return ddl
