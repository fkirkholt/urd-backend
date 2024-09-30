"""Module for handling databases and connections"""
import os
import time
import re
from graphlib import TopologicalSorter
from sqlalchemy import text, inspect, exc
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

    def __init__(self, engine, db_name, uid):
        self.engine = engine
        self.identifier = db_name
        self.refl = inspect(engine)
        self.user = User(engine, name=uid)
        path = db_name.split('.')
        if engine.name == 'postgresql':
            self.schema = 'public' if len(path) == 1 else path[1]
            self.cat = path[0]
        elif engine.name == 'mssql':
            self.schema = 'dbo' if len(path) == 1 else path[1]
            self.cat = path[0]
        elif engine.name in ('duckdb', 'sqlite'):
            self.schema = 'main' if len(path) == 1 else path[1]
            self.cat = path[0]
        else:
            self.schema = db_name
            self.cat = None

        if 'urdr' in self.refl.get_schema_names() or db_name == 'urdr':
            schema = 'main' if db_name == 'urdr' else 'urdr'
            self.cte_access = f"""
            with recursive cte_access (code, parent) as (
                select a1.code, a1.parent
                from {schema}.access a1
                join {schema}.user_access ua on ua.access_code = a1.code
                where ua.user_id = :uid
                union all
                select a2.code, a2.parent
                from {schema}.access a2
                join cte_access cte on cte.code = a2.parent
            )
            """
        else:
            self.cte_access = None

        self.html_attrs = self.init_html_attributes()
        attrs = Dict(self.html_attrs.pop('base', None))
        self.cache = attrs.pop('data-cache', None)
        if attrs.get('cache.config', None):
            self.config = self.cache.config
        else:
            config = Settings()
            self.config = Dict({
                'norwegian_chars': config.norwegian_chars
            })

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
        elif self.engine.name == 'duckdb' and (
            schema.endswith('.information_schema') or
            schema.startswith('system.') or
            schema.startswith('temp.') or
            '.fts_' in schema
        ):
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
                "schemata": [s for s in self.schemas if s != 'urdr'],
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

        tbl_names = self.user.tables(self.schema)
        view_names = self.refl.get_view_names(self.schema)

        for tbl_name in self.tablenames:
            if tbl_name[-5:] == '_view' and tbl_name[:-5] in self.tablenames:
                continue
            if '_fts' in tbl_name:
                continue

            hidden = tbl_name[0:1] == "_" or tbl_name == 'html_attributes'

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
            table_names = self.user.tables(self.schema)
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
        if (table.type == 'list' or table.hidden):
            return False

        for fkey in table.fkeys.values():
            if fkey.referred_table not in self.tablenames:
                continue

            # Not top level if has foreign keys to other table
            # that is not a hidden table and not of type 'list'
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

            if relation.table_name not in relation_tables:
                relation_tables.append(relation.table_name)
                relation_tables = self.get_relation_tables(
                    relation.table_name, relation_tables)

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
            if (
                (tbl_name[-5:] == '_grid' or tbl_name[-7:] == '_footer')
                and table.type == 'view'
            ):
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

    def get_tbl_groups(self):
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

                if len(common) and len(tbl_names) <= len(tbl_names2):
                    if len(diff) == 1 or len_combined < 15:
                        tbl_groups[group_name2].extend(tbl_names)
                        delete_groups.append(group_name)
                        break
                    elif len(common) == 1 and len(diff) > 1:
                        # We want the common tables only in the smallest
                        # group
                        tbl_groups[group_name2].remove(common[0])
                    else:
                        for tbl_name in common:
                            if tbl_name in tbl_groups[group_name2]:
                                tbl_groups[group_name2].remove(tbl_name)

        for group_name in delete_groups:
            del tbl_groups[group_name]

    def get_sub_tables(self):
        """Return Dict of tables with subordinate tables"""
        sub_tables = Dict()
        for tbl_name, table in self.tables.items():
            name_parts = tbl_name.rstrip('_').split("_")
            tbl = Table(self, tbl_name)

            for colname in table.pkey.columns:
                fkey = tbl.get_fkey(colname)
                if fkey:
                    if (
                        len(name_parts) > 1 and (
                            name_parts[0] in self.tables or
                            name_parts[0]+'_' in self.tables
                        ) and name_parts[0] != fkey.referred_table.strip('_')
                    ):
                        continue

                    if fkey.referred_table not in sub_tables:
                        sub_tables[fkey.referred_table] = []

                    sub_tables[fkey.referred_table].append(tbl_name)

        return sub_tables

    def get_label(self, identifier, prefix=None, postfix=None):
        """Get label based on identifier"""
        id_parts = identifier.split('_')
        if id_parts[-1] in ("list", "liste", "xref", "link"):
            identifier = "_".join(id_parts[:-1])
        if prefix:
            prefix = prefix.rstrip('_') + '_'
            identifier = identifier.replace(prefix, '')
        if postfix:
            postfix = postfix.lstrip('_') + '_'
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
        if (self.cache and not self.config.update_cache):
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

            with self.engine.connect() as cnxn:
                count = cnxn.execute(text(sql), {'selector': 'base'}).first()[0]

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
            # Must have column aliases to avoid error in SQLAlchemy for this
            # query in MySQL. Don't know why 
            sql = """
            select table_name as table_name, table_comment as table_comment
            from   information_schema.tables
            where table_schema = :schema
            """

            with self.engine.connect() as cnxn:
                rows = cnxn.execute(text(sql), {'schema': self.schema})
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
            # reflection of constraints is not implemented for duckdb yet
            if self.engine.name == 'duckdb':
                sql = """
                select * from duckdb_constraints()
                where constraint_type = 'PRIMARY KEY'
                """
                with self.engine.connect() as cnxn:
                    rows = cnxn.execute(text(sql)).fetchall()
                    for row in rows:
                        pkey = Dict({
                            'table_name': row.table_name,
                            'name': 'PRIMARY',
                            'unique': True,
                            'columns': row.constraint_column_names
                        })
                        self._pkeys[row.table_name] = pkey
            else:
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
            if self.engine.name == 'duckdb':
                sql = "select * from duckdb_indexes()"
                with self.engine.connect() as cnxn:
                    result = cnxn.execute(text(sql))
                    rows = result.mappings().fetchall()
                    for row in rows:
                        idx = Dict({
                            'table_name': row.table_name,
                            'name': row.index_name,
                            'unique': row.is_unique
                        })
                        expr = row.sql
                        x = re.search(r"\bON \w+\s?\(([^)]*)\)", expr)
                        cols_delim = x.group(1).split(',')
                        idx.columns = [s.strip() for s in cols_delim]
                        self._indexes[row.table_name][idx.name] = idx
                for table, pkey in self.pkeys.items():
                    self._indexes[table][pkey.name] = pkey
            else:
                schema_indexes = self.refl.get_multi_indexes(self.schema)

                for (schema, table), indexes in schema_indexes.items():

                    for idx in indexes:
                        idx = Dict(idx)
                        idx.columns = idx.pop('column_names')
                        idx.pop('dialect_options', None)

                        self._indexes[table][idx.name] = idx

                for table in self.pkeys:
                    pkey = self.pkeys[table]
                    self._indexes[table][pkey.name] = pkey

        return self._indexes

    @property
    def fkeys(self):
        """Get all foreign keys of table"""
        if not hasattr(self, '_fkeys'):
            self._fkeys = Dict()
            self._relations = Dict()
            aliases = {}

            schema_fkeys = self.refl.get_multi_foreign_keys(self.schema)

            for key, fkeys in schema_fkeys.items():
                for fkey in fkeys:
                    fkey = Dict(fkey)
                    fkey.base = self.cat
                    fkey.table_name = key[-1]
                    fkey.schema = key[0] or self.db.schema
                    if set(self.pkeys[fkey.table_name].columns) <= set(fkey.constrained_columns):
                        fkey.relationship = '1:1'
                    else:
                        fkey.relationship = '1:M'

                    fkey.name = fkey.table_name + '_'
                    fkey.name += '_'.join(fkey.constrained_columns)+'_fkey'

                    fkey_col = fkey.constrained_columns[-1]
                    ref_col = fkey.referred_columns[-1].strip('_')
                    if fkey_col in [fkey.referred_table + '_' + ref_col,
                                    fkey.referred_columns[-1]]:
                        ref_table_alias = fkey.referred_table
                    else:
                        ref_table_alias = fkey_col.strip('_')
                    # In seldom cases there might be two foreign keys ending
                    # in same column
                    if fkey.table_name not in aliases:
                        aliases[fkey.table_name] = []
                    if ref_table_alias in aliases[fkey.table_name]:
                        ref_table_alias = ref_table_alias + '2'
                    fkey.ref_table_alias = ref_table_alias
                    aliases[fkey.table_name].append(ref_table_alias)

                    self._fkeys[fkey.table_name][fkey.name] = Dict(fkey)
                    self._relations[fkey.referred_table][fkey.name] = Dict(fkey)

        return self._fkeys

    @property
    def relations(self):
        """Get all has-many relations of table"""
        if not hasattr(self, '_relations'):
            self.fkeys

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
            if result.returns_rows:
                if limit:
                    query.data = result.mappings().fetchmany(limit)
                else:
                    query.data = result.mappings().fetchall()

                # Find the table selected from
                query.table = str(sqlglot.parse_one(sql)
                                  .find(sqlglot.exp.Table))
            else:
                rowcount = result.rowcount

                query.rowcount = rowcount
                query.result = f"Query OK, {rowcount} rows affected"

            cnxn.commit()

        return query

    def export_as_sql(self, dialect: str, table_defs: bool, list_recs: bool,
                      data_recs: bool, select_recs: bool):
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

        if table_defs:
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
            if table_defs:
                ddl += table.export_ddl(dialect)
            if list_recs or data_recs:
                self_ref = None
                if tbl_name in self_referring:
                    self_ref = self_referring[tbl_name]
                if (
                    (table.type == 'list' and list_recs) or
                    (table.type != 'list' and data_recs)
                ):
                    ddl += table.export_records(dialect, select_recs, self_ref)

        if table_defs:
            i = 0
            for view_name in self.refl.get_view_names(self.schema):
                if i == 0:
                    print('\n')
                i += 1
                view_def = self.refl.get_view_definition(view_name, self.schema)
                ddl += view_def + ";\n\n"

        return ddl

    def export_as_kdrs_xml(self, version, descr):
        xml = "<views>\n"
        xml += "  <version>" + version + "</version>\n"
        xml += "  <description>" + descr + "</description>\n"
        
        self.get_tables()
        contents = self.get_contents()

        for label, obj in contents.items():
            if type(obj) is str and obj[0:7] == 'tables.':
                tbl_name = obj[7:]
            elif 'item' in obj:
                tbl_name = obj.item[7:]
            else:
                continue

            tbl = self.tables[tbl_name]
            if not tbl.fields:
                table = Table(self, tbl_name)
                tbl.fields = table.fields

            xml += "  <view>\n"
            xml += "    <name>" + tbl.label + "</name>\n"
            xml += "    <table>\n"
            xml += "      <name>" + tbl.name + "</name>\n"
            xml += "      <heading>Finn " + tbl.label + "</heading>\n"
            xml += "      <fields>" + ', '.join(tbl.fields.keys()) + "</fields>\n"
            xml += "      <primarykey>" + ', '.join(tbl.pkey.columns) + "</primarykey>\n"
            xml += "      <preview>false</preview>\n"  # TODO: how to choose value?
            xml += "    </table>\n"

            if 'subitems' in obj:
                for subitem, subobj in obj.subitems.items():
                    if type(subobj) is str and subobj[0:7] == 'tables.':
                        subtbl_name = subobj[7:]
                    elif 'item' in subobj:
                        subtbl_name = subobj.item[7:]
                    else:
                        continue

                    subtbl = self.tables[subtbl_name]

                    if not subtbl.fields:
                        table = Table(self, subtbl_name)
                        subtbl.fields = table.fields

                    for key, fkey in subtbl.fkeys.items():
                        if fkey.referred_table == tbl.name:
                            fkey_str = ', '.join(fkey.constrained_columns)

                    xml += "    <table>\n"
                    xml += "      <name>" + subtbl.name + "</name>\n"
                    xml += "      <heading>" + subtbl.label + "</heading>\n"
                    xml += "      <parent>" + tbl.name + "</parent>\n"
                    xml += "      <fields>" + ', '.join(subtbl.fields.keys()) + "</fields>\n"
                    xml += "      <primarykey>" + ', '.join(subtbl.pkey.columns) + "</primarykey>\n"
                    xml += "      <foreignkey>" + fkey_str + "</foreignkey>\n"
                    xml += "      <search>true</search>\n"  # TODO: how to choose value?
                    if subtbl.grid and 'sort_columns' in subtbl.grid:
                        xml += "      <sort>" + ', '.join(subtbl.grid.sort_columns) + "</sort>\n"

                    for key, fkey in subtbl.fkeys.items():
                        last_col = fkey.constrained_columns[-1]
                        view = subtbl.fields[last_col].view
                        view = view.replace(last_col + '.', '')
                        xml += "      <lookup>\n"
                        xml += "        <foreignkey>" + ', '.join(fkey.constrained_columns) + "</foreignkey>\n"
                        xml += "        <table>" + fkey.referred_table + "</table>\n"
                        xml += "        <primarykey>" + ', '.join(fkey.referred_columns) + "</primarykey>\n"
                        xml += "        <fields>" + view + ' as ' + last_col + "</fields>\n"
                        xml += "      </lookup>\n"

                    xml += "    </table>\n"

            xml += "  </view>\n"
        xml += "</views>"

        return xml
