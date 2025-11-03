"""Module for handling databases and connections"""
import os
import time
import re
import csv
import sys
import shutil
import tempfile
from pathlib import Path
from graphlib import TopologicalSorter
from sqlalchemy import inspect, exc
import sqlglot
import simplejson as json
import pyodbc
from addict import Dict
from settings import Settings
from table import Table
from grid import Grid
from user import User
from datatype import Datatype
from odbc_engine import ODBC_Engine
from reflection import Reflection
from expression import Expression
from util import prepare, to_rec, time_func, time_stream_generator


class Database:
    """Contains methods for getting data and metadata from database"""

    def __init__(self, engine, db_name, uid):
        self.pkeys_loaded = False
        self.fkeys_loaded = False
        self.columns_loaded = False
        self.indexes_loaded = False
        self.engine = engine
        self.identifier = db_name
        self.user = User(engine, name=uid)
        path = db_name.split('.')
        if engine.name == 'postgresql':
            self.schema = 'public' if len(path) == 1 else path[1]
            self.cat = path[0]
        elif engine.name == 'mssql':
            self.schema = 'dbo' if len(path) == 1 else path[1]
            self.cat = path[0]
        elif engine.name in ('duckdb', 'sqlite'):
            self.schema = 'main'
            self.cat = None
        else:
            self.schema = db_name
            self.cat = None
        self.refl = Reflection(engine, self.cat) if type(engine) is ODBC_Engine else inspect(engine)

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
                'norwegian_chars': config.norwegian_chars,
                'exportdir': config.exportdir
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
                    sql, _ = prepare(sql)
                    rows = cnxn.execute(sql)
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

    @time_func
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

        datatype = Datatype('json')
        string_datatype = datatype.to_native_type(self.engine.name)

        sql = f"""
        create table {self.schema}.html_attributes(
            selector varchar(100) not null,
            attributes {string_datatype} not null,
            primary key (selector)
        )
        """

        with self.engine.connect() as cnxn:
            sql, _ = prepare(sql)
            cnxn.execute(sql)
            cnxn.commit()

        self.tablenames.append('html_attributes')
        attributes = {
            'data-format': 'yaml'
        }

        val = json.dumps(attributes)

        sql = f"""
            insert into {self.schema}.html_attributes (selector, attributes)
            values ('[data-field="html_attributes.attributes"]', '{val}')
        """

        with self.engine.connect() as cnxn:
            sql, _ = prepare(sql)
            cnxn.execute(sql)
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

        # Loads metadata so we don't have to load for each table
        self.fkeys
        self.indexes
        self.pkeys

        for tbl_name in self.tablenames:
            if tbl_name[-5:] == '_view' and tbl_name[:-5] in self.tablenames:
                continue
            if '_fts' in tbl_name:
                continue

            table = Table(self, tbl_name)

            self.tables[tbl_name] = table.get()
            self.tables[tbl_name].fkeys = self.fkeys[tbl_name]
            self.tables[tbl_name].relations = self.relations[tbl_name]

        return self.tables

    @property
    def schemas(self):
        if hasattr(self, '_schemas'):
            return self._schemas

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
                sql, params = prepare(sql, params)
                rows = cnxn.execute(sql, params).fetchall()
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
                sql, params = prepare(sql, {'cat': self.cat, 'schema': self.schema})
                rows = cnxn.execute(sql, params).fetchall()
            self._tablenames = [row[0] for row in rows]
        else:
            table_names = self.user.tables(self.cat, self.schema)
            view_names = self.refl.get_view_names(self.schema)
            self._tablenames = table_names + view_names

        return self._tablenames

    @property
    def columns(self):
        """ Return all columns in database grouped by table name """
        self.columns_loaded = True

        if not hasattr(self, '_columns'):
            self._columns = Dict()

            if self.engine.name == 'duckdb':
                sql = """
                select table_name, column_name as name, is_nullable as nullable,
                       column_default as "default", data_type as type
                from duckdb_columns
                where schema_name = 'main'
                """
                with self.engine.connect() as cnxn:
                    sql, _ = prepare(sql)
                    rows = cnxn.execute(sql).fetchall()
                    for row in rows:
                        if row.table_name not in self._columns:
                            self._columns[row.table_name] = []
                        col = to_rec(row)
                        self._columns[row.table_name].append(col)

            else:
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

    def is_subordinate(self, tbl_name):
        for tables in self.sub_tables.values():
            if tbl_name in tables:
                return True

        return False

    def get_tbl_groups_urdr(self):
        """Group tables by prefix

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
            elif table.type == 'xref':
                continue

            name = tbl_name[1:] if tbl_name[0:1] == "_" else tbl_name
            parts = name.split("_")

            # Don't include tables that are subordinate to other tables,
            # i.e. the primary key also has a foreign key.
            # These are handled in get_content_node
            if self.is_subordinate(tbl_name):
                continue

            placed = False
            for group in tbl_groups:
                if name.startswith(group + '_'):
                    tbl_groups[group].append(tbl_name)
                    placed = True

            if not placed:
                group = None
                # Find longest prefix shared by at least two tables
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

    @property
    def sub_tables(self):
        """Return Dict of tables with subordinate tables"""
        if hasattr(self, '_sub_tables'):
            return self._sub_tables

        sub_tables = Dict()
        for tbl_name, table in self.tables.items():
            name_parts = tbl_name.rstrip('_').split("_")
            tbl = Table(self, tbl_name)

            if tbl.type == 'xref':
                continue

            for colname in table.pkey.columns:
                fkey = tbl.get_fkey(colname)
                if fkey:
                    if tbl.type == 'ext' and fkey.relationship == '1:M':
                        continue

                    ref_tbl = Table(self, fkey.referred_table)
                    if ref_tbl.type == 'list' and tbl.type != 'list':
                        continue

                    if fkey.referred_table not in sub_tables:
                        sub_tables[fkey.referred_table] = []

                    sub_tables[fkey.referred_table].append(tbl_name)

        self._sub_tables = sub_tables

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
            postfix = '_' + postfix.lstrip('_')
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
                sql, params = prepare(sql, {'selector': 'base'})
                count = cnxn.execute(sql, params).fetchone()[0]

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
                sql, params = prepare(sql, {
                    'attrs': attrs_txt,
                    'selector': 'base'
                })
                cnxn.execute(sql, params)
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
                sql, params = prepare(sql, {'schema': self.schema})
                rows = cnxn.execute(sql, params)
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

        if hasattr(self, '_pkeys'):
            return self._pkeys

        self.pkeys_loaded = True
        self._pkeys = Dict()
        # reflection of constraints is not implemented for duckdb yet
        if self.engine.name == 'duckdb':
            sql = """
            select * from duckdb_constraints()
            where constraint_type = 'PRIMARY KEY'
            """
            with self.engine.connect() as cnxn:
                sql, _ = prepare(sql)
                rows = cnxn.execute(sql).fetchall()
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
        self.indexes_loaded = True
        if hasattr(self, '_indexes'):
            return self._indexes

        self._indexes = Dict()
        if self.engine.name == 'duckdb':
            sql, _ = prepare("select * from duckdb_indexes()")
            with self.engine.connect() as cnxn:
                rows = cnxn.execute(sql).fetchall()
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

                    if idx.name and idx.columns != [None]:
                        self._indexes[table][idx.name] = idx

            for table in self.pkeys:
                pkey = self.pkeys[table]
                self._indexes[table][pkey.name] = pkey

        return self._indexes

    @property
    def fkeys(self):
        """Get all foreign keys of table"""
        if hasattr(self, '_fkeys'):
            return self._fkeys
        self.fkeys_loaded = True

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
                if (
                    self.pkeys[fkey.table_name].columns and
                    set(self.pkeys[fkey.table_name].columns) <= set(fkey.constrained_columns)
                ):
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

        if len(self._fkeys) == 0:
            fkeys = Dict()
            relations = Dict()
            for tbl_name_1 in self.columns:
                tbl_fkey_columns = []
                for col_1 in self.columns[tbl_name_1]:
                    col_1 = Dict(col_1)
                    for tbl_name_2 in self.tablenames:
                        if (tbl_name_2.rstrip('_') + '_') in col_1.name:
                            constrained_cols = []
                            for col_2 in self.columns[tbl_name_2]:
                                col_2 = Dict(col_2)
                                if col_1.name == col_2.name and tbl_name_1 == tbl_name_2:
                                    continue
                                # reference to column in referred table
                                ref = (tbl_name_2 + '_' + col_2.name).replace('__', '_').rstrip('_')
                                if col_1.name.endswith(ref) or col_1.name == col_2.name:
                                    tbl_fkey_columns.append(col_1.name)
                                    # possible prefix that describes what relation this is
                                    # e.g. in `updated_by_user_name` the prefix is `upated_by`
                                    prefix = col_1.name.replace(ref, '').rstrip('_')
                                    prefix = '_' + prefix if prefix else ''
                                    name = tbl_name_1 + '_' + tbl_name_2 + prefix + '_fkey'
                                    fkeys[tbl_name_1][name].name = name
                                    fkeys[tbl_name_1][name].table_name = tbl_name_1
                                    fkeys[tbl_name_1][name].schema = self.schema
                                    referred_cols = constrained_cols.copy()
                                    referred_cols.append(col_2.name)
                                    constrained_cols.append(col_1.name) 
                                    fkeys[tbl_name_1][name].constrained_columns = constrained_cols
                                    fkeys[tbl_name_1][name].referred_columns = referred_cols
                                    fkeys[tbl_name_1][name].referred_schema = self.schema
                                    fkeys[tbl_name_1][name].referred_table = tbl_name_2
                                    fkeys[tbl_name_1][name].ref_table_alias = prefix or tbl_name_2
                                    relations[tbl_name_2][name] = fkeys[tbl_name_1][name]
                                elif col_2.name in tbl_fkey_columns:
                                    constrained_cols.append(col_2.name)
            for tbl_name in fkeys:
                for fkey in fkeys[tbl_name].values():
                    if (
                        self.pkeys[fkey.table_name].columns and
                        set(self.pkeys[fkey.table_name].columns) <= set(fkey.constrained_columns)
                    ):
                        fkey.relationship = '1:1'
                    else:
                        fkey.relationship = '1:M'

            self._fkeys = fkeys
            self._relations = relations

        return self._fkeys

    @property
    def relations(self):
        """Get all has-many relations of table"""
        if not hasattr(self, '_relations'):
            self.fkeys

        return self._relations

    @property
    def functions(self):
        """Get all functions in database"""
        functions = Dict()
        sql = self.user.expr.functionlines()
        if sql is None:
            return functions
        with self.engine.connect() as cnxn:
            sql, params = prepare(sql, {'owner': self.schema})
            rows = cnxn.execute(sql, params).fetchall()
        for row in rows:
            if row.name not in functions:
                functions[row.name] = row.text
            else:
                functions[row.name] += row.text
        return functions

    @property
    def procedures(self):
        """Get all procedures in database"""
        procedures = Dict()
        sql = self.user.expr.procedurelines()
        if sql is None:
            return procedures
        with self.engine.connect() as cnxn:
            sql, params = prepare(sql, {'owner': self.schema})
            rows = cnxn.execute(sql, params).fetchall()
        for row in rows:
            if row.name not in procedures:
                procedures[row.name] = row.text
            else:
                procedures[row.name] += row.text
        return procedures

    def query_result(self, sql, limit, cnxn):
        """Get query result for user defined sql"""
        query = Dict()
        sql = sql.strip()
        query.string = sql
        if len(query.string) == 0:
            return None
        t1 = time.time()
        sql, _ = prepare(sql)
        if type(self.engine) is ODBC_Engine:
            try:
                result = cnxn.execute(sql)
            except pyodbc.Error as ex:
                sqlstate = ex.args[1]
                sqlstate = sqlstate.replace('[HY000]', '')
                sqlstate = sqlstate.replace('[SQLite]', '')
                sqlstate = sqlstate.replace('(1)', '')
                sqlstate = sqlstate.replace('(SQLExecDirectW)', '')
                query.time = round(time.time() - t1, 4)
                query.success = False
                query.result = 'ERROR: ' + sqlstate.strip()

                return query
        else:
            cwd = os.getcwd()
            folder = Path(self.engine.url.database).parent
            os.chdir(folder)

            try:
                result = cnxn.execute(sql)
            except exc.StatementError as ex:
                os.chdir(cwd)
                query.time = round(time.time() - t1, 4)
                query.success = False
                query.result = 'ERROR: {}'.format(ex.orig)

                return query

            os.chdir(cwd)
        query.success = True
        query.time = round(time.time() - t1, 4)

        if type(self.engine) is ODBC_Engine:
            returns_rows = result.description
        else:
            returns_rows = result.returns_rows

        if returns_rows:
            if limit:
                rows = result.fetchmany(limit)
            else:
                rows = result.fetchall()

            query.data = [to_rec(row) for row in rows]
            # Find the table selected from
            query.table = str(sqlglot.parse_one(query.string)
                              .find(sqlglot.exp.Table))

            # Get table name in correct case
            tbl_names = self.refl.get_table_names(self.schema)

            for tbl_name in tbl_names:
                if tbl_name.lower() == query.table.lower():
                    query.table = tbl_name
                    break

        else:
            rowcount = result.rowcount

            query.rowcount = rowcount
            query.result = f"Query OK, {rowcount} rows affected"

        cnxn.commit()

        return query

    @time_stream_generator
    async def export_sql(self, dest, dialect, table_defs, no_fkeys, list_recs,
                         data_recs, select_recs, view_as_table, no_empty,
                         table, filter):
        # Loads metadata so we don't have to load for each table
        self.pkeys
        self.fkeys
        self.columns

        tbl_names = self.refl.get_table_names(self.schema)
        if table:
            views = []
        else:
            views = tuple(self.refl.get_view_names(self.schema))

        tbl_list = []
        params = []
        if filter:
            tbl = Table(self, table)
            grid = Grid(tbl)
            grid.set_search_cond(filter)
            join = '\n'.join(tbl.joins.values())
            cond = grid.get_cond_expr()
            params = grid.cond.params
        # Count rows
        count_recs = Dict()
        if data_recs or no_empty:
            data = json.dumps({
                'msg': 'Counting records',
                'progress': 0,
            })
            yield f"data: {data}\n\n"
            total_rows = 0
            with self.engine.connect() as cnxn:
                sql = self.user.expr.rowcount()
                if sql and not filter:
                    sql, params = prepare(sql, {'schema': self.schema})
                    rows = cnxn.execute(sql, params).fetchall()
                    for row in rows:
                        count_recs[row.table_name] = row.count_rows
                        total_rows += row.count_rows
                        if row.count_rows or not no_empty:
                            tbl_list.append(row.table_name)
                    if view_as_table:
                        for view_name in views:
                            sql = f'select count(*) from {view_name}'
                            sql, _ = prepare(sql)
                            n = cnxn.execute(sql).fetchone()[0]
                            count_recs[view_name] = n
                            if n or not no_empty:
                                tbl_list.append(view_name)

                else:
                    for tbl_name in tbl_names:
                        sql = f'select count(*) from {tbl_name}'
                        if filter:
                            sql += '\n' + join
                            sql += ' where ' + cond
                        sql, params = prepare(sql, params)
                        n = cnxn.execute(sql, params).fetchone()[0]
                        count_recs[tbl_name] = n
                        total_rows += n
                        if n or not no_empty:
                            tbl_list.append(tbl_name)
        else:
            tbl_list = tbl_names

        download = True if dest == 'download' else False
        if download:
            dest = tempfile.gettempdir()
        else:
            os.makedirs(dest, exist_ok=True)
        if table:
            filepath = os.path.join(dest, f"{table}.{dialect}.sql")
            ordered_tables = [table]
        else:
            filepath = os.path.join(dest, f"{self.identifier.lower()}.{dialect}.sql")
            data = json.dumps({
                'msg': 'Sorting tables',
                'progress': 0,
            })
            yield f"data: {data}\n\n"
            ordered_tables = self.sorted_tbl_names(tbl_list)

        ddl = ''

        file = open(filepath, 'w')
        if hasattr(self, 'circular'):
            for line in self.circular:
                file.write('-- ' + line + '\n')
        if dialect == 'oracle':
            file.write("SET DEFINE OFF;\n")
            file.write("SET FEEDBACK OFF;\n")
            file.write("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD';\n")
        if table_defs:
            if dialect == self.engine.name:
                for view_name in views:
                    if dialect == 'oracle':
                        ddl += f"drop view {view_name};\n"
                    else:
                        ddl += f"drop view if exists {view_name};\n"

            for tbl_name in reversed(ordered_tables):
                if no_empty and count_recs[tbl_name] == 0:
                    continue
                if dialect == 'oracle':
                    ddl += f"drop table {tbl_name};\n"
                else:
                    ddl += f"drop table if exists {tbl_name};\n"

            file.write(ddl)
            ddl = ''

        if dialect == 'oracle':
            file.write('WHENEVER SQLERROR EXIT 1;\n')

        count = 0
        last_progress = 0
        i = 0
        expr = Expression(dialect)
        for tbl_name in ordered_tables:
            i += 1

            if no_empty and count_recs[tbl_name] == 0:
                continue
            if tbl_name is None:
                continue
            if tbl_name == 'sqlite_sequence':
                continue
            if '_fts' in tbl_name:
                continue
            table = Table(self, tbl_name)
            if table_defs:
                file.write(table.export_ddl(dialect, no_fkeys, no_empty, count_recs))
                file.write(table.get_indexes_ddl())

            if (
                (table.type == 'list' and not list_recs) or
                (table.type != 'list' and not data_recs)
            ):
                progress = '{:.1f}'.format(round(i/len(ordered_tables) * 100, 1))
                if progress != last_progress:
                    data = json.dumps({
                        'msg': (table.name[:17] + "..." if len(table.name) > 17
                                else table.name),
                        'progress': progress
                    })
                    yield f"data: {data}\n\n"
                    last_progress = progress
                continue

            if dialect == 'oracle':
                file.write(f'prompt inserts into {table.name}\n')
            if select_recs:
                file.write(f'insert into {table.name}\n')
                file.write(f'select * from {self.schema}.{table.name};\n')
            else:
                params = []
                join = ''
                cond = None
                if filter:
                    grid = Grid(self)
                    grid.set_search_cond(filter)
                    cond = grid.get_cond_expr()
                    params = grid.cond.params
                sql = expr.rows(table, cond)
                with self.engine.connect() as cnxn:
                    sql, params = prepare(sql, params)
                    cursor = cnxn.execute(sql, params)
                    
                    # Insert time grows exponentially with number of inserts
                    # per `insert all` in Oracle after a certain value.
                    # This value is around 50 for version 19c
                    max = 50 if dialect == 'oracle' else 1000;

                    while True:
                        rows = cursor.fetchmany(max)
                        if not rows:
                            break

                        # rowcount = len(rows)
                        i = 0
                        if dialect == 'oracle':
                            insert = f'insert into {table.name}\n'
                        else:
                            insert = f'insert into {table.name} values '
                        file.write(insert)
                        for row in rows:
                            progress = '{:.1f}'.format(round(count/total_rows * 100, 1))
                            if progress != last_progress:
                                data = json.dumps({'msg': table.name, 'progress': progress})
                                yield f"data: {data}\n\n"
                                last_progress = progress
                            i += 1
                            count += 1
                            insert = ''
                            if dialect == 'oracle' and i > 1:
                                insert += ' union all\n'
                            rec = to_rec(row)
                            if i != 1:
                                insert += ','
                            insert += expr.insert_rec(table, rec) 
                            file.write(insert)

                        file.write(';\n\n')

        if table_defs and dialect == self.engine.name:
            i = 0
            for view_name in views:
                if i == 0:
                    ddl += '\n'
                i += 1
                try:
                    # Fails in mssql if user hasn't got permission VIEW DEFINITION
                    view_def = self.refl.get_view_definition(view_name, self.schema)
                except Exception as e:
                    view_def = f"-- ERROR: Couldn't get definition for view {view_name} "
                    print(e)
                if view_def:
                    ddl += f'{view_def}; \n\n'
                else:
                    ddl += f"-- View definition not supported for {self.engine.name} yet\n"
            for definition in self.functions.values():
                if dialect == 'oracle':
                    ddl += 'CREATE OR REPLACE '
                ddl += definition + '\n\n'
            for definition in self.procedures.values():
                if dialect == 'oracle':
                    ddl += 'CREATE OR REPLACE '
                ddl += definition + '\n\n'

            file.write(ddl)

        file.close()

        if download:
            new_path = os.path.join(tempfile.gettempdir(),
                                    os.path.basename(filepath))
            os.rename(filepath, new_path)
            data = json.dumps({'msg': 'done', 'path': new_path})
            yield f"data: {data}\n\n"
        else:
            data = json.dumps({'msg': 'done'})
            yield f"data: {data}\n\n"

    @time_stream_generator
    async def export_tsv(self, tables, dest, limit, clobs_as_files, cols, download, filter):
        # Loads metadata so we don't have to load for each table
        self.pkeys
        self.columns

        expr = Expression(self.engine.name)

        params = []
        if filter:
            tbl = Table(self, tables[0])
            grid = Grid(tbl)
            grid.set_search_cond(filter)
            join = '\n'.join(tbl.joins.values())
            cond = grid.get_cond_expr()
            params = grid.cond.params
        # Count rows
        total_rows = 0
        for table in tables:
            
            with self.engine.connect() as cnxn:
                sql = f'select count(*) from {expr.quote(table)}'
                if filter:
                    sql += '\n' + join
                    sql += ' where ' + cond
                sql, params = prepare(sql, params)
                n = cnxn.execute(sql, params).fetchone()[0]
                if limit and n > limit:
                    n = limit
                total_rows += n

        count = 0
        last_progress = 0
        for table in tables:
            table = Table(self, table, dest)
            table.offset = 0
            table.limit = limit
            filepath = os.path.join(dest, self.schema.lower() + '-data', table.name + '.tsv')
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            blobcolumns = []
            selects = {}
            for col in table.columns:
                col.datatype = expr.to_urd_type(col.type)
                if col.datatype == 'bytes' or (
                    clobs_as_files and col.datatype == 'str' and not col.size
                 ):
                    foldername = table.name + '.' + col.name
                    path = os.path.join(os.path.dirname(filepath), '../documents', foldername)
                    os.makedirs(path, exist_ok=True)
                    blobcolumns.append(col.name)
                if not cols or col.name in cols:
                    selects[col.name] = col.name
                    if col.datatype == 'geometry':
                        selects[col.name] = f"{col.name}.ToString() as {col.name}"

            select = ', '.join(selects.values())

            file = open(filepath, 'w')
            sql = f"select {select} from " + expr.quote(table.name)
            if filter:
                sql += '\n' + join
                sql += ' where ' + cond
            with self.engine.connect() as cnxn:
                sql, params = prepare(sql, params)
                try:
                    rows = cnxn.execute(sql, params)
                except Exception as e:
                    print(e)
                    print(sql)
                n = 0
                for row in rows:
                    progress = '{:.1f}'.format(round(count/total_rows * 100, 1))
                    if progress != last_progress:
                        data = json.dumps({'msg': table.name, 'progress': progress})
                        yield f"data: {data}\n\n"
                        last_progress = progress
                    if limit and n == limit:
                        break
                    n += 1
                    count += 1
                    rec = to_rec(row)
                    if n == 1:
                        file.write('\t'.join(rec.keys()) + '\n')
                    values = []
                    num_files = 0
                    for colname, val in rec.items():
                        if colname in blobcolumns:
                            num_files += 1
                            dir = os.path.dirname(filepath)
                            if table.pkey:
                                pkey_vals = []
                                for pkey_col in table.pkey.columns:
                                    pkey_vals.append(str(rec[pkey_col]))
                                filename = '-'.join(pkey_vals) + '.data'
                            else:
                                filename = str(num_files) + '.data'

                            foldername = table.name + '.' + colname
                            path = os.path.join(dir, '../documents', foldername, filename)
                            if val is not None:
                                for tbl_col in table.columns:
                                    if tbl_col.name == colname:
                                        col = tbl_col
                                        break

                                mode = 'wb' if col.datatype == 'bytes' else 'w' 
                                with open(path, mode) as blobfile:
                                    blobfile.write(val)
                                val = 'documents/' + foldername + '/' + filename
                        if type(val) is bool:
                            val = int(val)
                        if type(val) is str:
                            val = val.strip()
                            val = val.replace('\t', '\\t')
                            val = val.replace('\r\n', '\\n')
                            val = val.replace('\r', '\\n')
                            val = val.replace('\n', '\\n')
                        elif val is None:
                            val = ''
                        else:
                            val = str(val)
                        values.append(val)
                    file.write('\t'.join(values) + '\n')
                file.close()
                if n == 0:
                    os.remove(filepath)
        if download:
            path = shutil.make_archive(dest, 'zip', dest)
            new_path = os.path.dirname(path) + '/' + self.schema + '.zip'
            os.rename(path, new_path)
            data = json.dumps({'msg': 'done', 'progress': 100, 'path': new_path})

            yield f"data: {data}\n\n"
        else:
            data = json.dumps({'msg': 'done', 'progress': 100})
            yield f"data: {data}\n\n"

    @time_stream_generator
    async def import_tsv(self, dir: str):

        # Increase CSV field size limit to maximim possible
        # https://stackoverflow.com/a/15063941
        field_size_limit = sys.maxsize

        while True:
            try:
                csv.field_size_limit(field_size_limit)
                break
            except OverflowError:
                field_size_limit = int(field_size_limit / 10)

        filenames = os.listdir(dir)
        expr = Expression('sqlite')

        # count all rows in the tsv files
        total_rows = 0
        for filename in filenames:
            filepath = os.path.join(dir, filename)
            with open(filepath) as f:
                total_rows += sum(1 for _ in f) - 1

        i = 0
        count = 0
        last_progress = 0
        for filename in filenames:
            tbl_name = Path(filename).stem
            table = Table(self, tbl_name)
            i += 1
            filepath = os.path.join(dir, filename)

            cols = self.refl.get_columns(tbl_name, self.schema)
            mandatory = [col['name'] for col in cols if not col['nullable']]

            with open(filepath, 'r') as file:
                with self.engine.connect() as cnxn:
                    records = csv.DictReader(file, delimiter='\t')
                    sql = f'insert into {tbl_name} values '

                    n = 0
                    for rec in records:
                        count += 1
                        n += 1
                        progress = '{:.1f}'.format(round(count/total_rows * 100, 1))
                        if progress != last_progress:
                            data = json.dumps({'msg': tbl_name, 'progress': progress})
                            yield f"data: {data}\n\n"
                            last_progress = progress
                        if n == 10000:
                            sql, _ = prepare(sql)
                            cnxn.execute(sql)
                            cnxn.commit()
                            sql = f'insert into {tbl_name} values '
                            n = 1
                        if n != 1:
                            sql += ',' 
                        sql += expr.insert_rec(table, rec) 

                    sql, _ = prepare(sql)
                    cnxn.execute(sql)
                    cnxn.commit()

        data = json.dumps({'msg': 'done'})
        yield f"data: {data}\n\n"

    def sorted_tbl_names(self, tbl_names=None):
        graph = {}
        if not tbl_names:
            tbl_names = self.refl.get_table_names(self.schema)

        # Make graph to use in topologic sort
        for tbl_name in tbl_names:
            graph[tbl_name] = []
            fkeys = self.fkeys[tbl_name]
            for fkey in fkeys.values():
                if fkey['referred_table'] == tbl_name:
                    continue
                if fkey['referred_table'] in tbl_names:
                    # SQLite may have foreign keys referring to non existing tables
                    if fkey['referred_table'] not in graph[tbl_name]:
                        graph[tbl_name].append(fkey['referred_table'])
                        sorter = TopologicalSorter(graph)
                        try:
                            ordered_tables = tuple(sorter.static_order())
                        except Exception as e:
                            print(e)
                            if not hasattr(self, 'circular'):
                                self.circular = []
                            self.circular.append(str(e) + '. Removed fkey: ' + json.dumps(fkey))
                            graph[tbl_name].pop()

        sorter = TopologicalSorter(graph)
        ordered_tables = tuple(sorter.static_order())

        return ordered_tables

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
