import re
from addict import Dict
from sqlalchemy import inspect
from settings import Settings
from reflection import Reflection
from odbc_engine import ODBC_Engine
from util import prepare
from expression import Expression


class User:

    def __init__(self, engine, name=None):
        self.name = name or engine.url.username
        self.engine = engine
        self.expr = Expression(engine.name) 
        self.current = name is None
        self._is_admin = {}

    def databases(self, schema=None, cat=None):
        sql = self.expr.databases()

        with self.engine.connect() as cnxn:
            if self.engine.name == 'sqlite':
                params = {'uid': self.name}
            else:
                params = {'schema': schema, 'cat': cat}
            sql, params = prepare(sql, params)
            rows = cnxn.execute(sql, params).fetchall()

        return rows

    def tables(self, cat, schema):
        cfg = Settings()
        refl = Reflection(self.engine, cat) if type(self.engine) is ODBC_Engine else inspect(self.engine)
        tbl_names = refl.get_table_names(schema)
        if self.engine.name == 'sqlite' and cfg.database == 'urdr':
            db_path = self.engine.url.database
            db_name = self.engine.url.database.split(cfg.host)[1].lstrip('/')
            urdr = 'main' if db_path.endswith('/urdr.db') else 'urdr'
            sql = f"""
            with recursive cte_access (code, parent) as (
                select a1.code, a1.parent
                from {urdr}.access a1
                join user_access ua on ua.access_code = a1.code
                where ua.user_id = :uid
                union all
                select a2.code, a2.parent
                from {urdr}.access a2
                join cte_access cte on cte.code = a2.parent
            )
            select table_name from {urdr}.table_access
            where  database_name = :db and read_access is not NULL and
                   read_access not in (select code from cte_access)
            """

            params = {'uid': self.name, 'db': db_name}

            with self.engine.connect() as cnxn:
                sql, params = prepare(sql, params)
                rows = cnxn.execute(sql, params).fetchall()

                for row in rows:
                    tbl_names.remove(row.table_name)

        return tbl_names

    @property
    def roles(self):
        """ Get roles for current user """
        roles = []
        if self.engine.name in ['mysql', 'mariadb']:
            if self.current:
                sql = """
                select role_name
                from information_schema.applicable_roles
                """
                params = {}
            else:
                sql = """
                select Role as role_name
                from mysql.roles_mapping
                where User = :user
                """
                params = {'user': self.name}

            with self.engine.connect() as cnxn:
                sql, params = prepare(sql, params)
                rows = cnxn.execute(sql, params).fetchall()

            for row in rows:
                roles.append(row[0])

        return roles

    @property
    def access_codes(self):
        if hasattr(self, '_access_codes'):
            return self._access_codes
        if self.engine.name == 'sqlite':
            db_path = self.engine.url.database
            urdr = 'main' if db_path.endswith('/urdr.db') else 'urdr'

            sql = f"""
            with recursive cte_access (code, parent) as (
                select a1.code, a1.parent
                from {urdr}.access a1
                join user_access ua on ua.access_code = a1.code
                where ua.user_id = :uid
                union all
                select a2.code, a2.parent
                from {urdr}.access a2
                join cte_access cte on cte.code = a2.parent
            )
            select code from cte_access
            """

            self._access_codes = []
            with self.engine.connect() as cnxn:
                sql, params = prepare(sql, {'uid': self.name})
                rows = cnxn.execute(sql, params).fetchall()

            for row in rows:
                self._access_codes.append(row.code)

        return self._access_codes

    def schema_privilege(self, schema):
        """Get user privileges"""

        # Set privileges to 0 so that these could be overriden when
        # finding privileges for different database systemes
        privilege = Dict()
        privilege.select = 0
        privilege.insert = 0
        privilege['update'] = 0
        privilege.delete = 0

        cfg = Settings()

        if self.engine.name == 'sqlite' and cfg.database == 'urdr':
            db_path = self.engine.url.database
            db_name = self.engine.url.database.split(cfg.host)[1].lstrip('/')
            urdr = 'main' if db_path.endswith('/urdr.db') else 'urdr'

            sql = f"""
            select read_access, write_access from {urdr}.database_access
            where database_name = :db_name
            """

            with self.engine.connect() as cnxn:
                sql, params = prepare(sql, {'db_name': db_name})
                row = cnxn.execute(sql, params).fetchone()

            if row:
                read_access = row.read_access
                write_access = row.write_access
            else:
                read_access = None
                write_access = None

            if read_access is None or read_access in self.access_codes:
                privilege.select = 1

            if write_access is None or write_access in self.access_codes:
                privilege.select = 1
                privilege.insert = 1
                privilege['update'] = 1
                privilege.delete = 1

        elif self.engine.name in ['mysql', 'mariadb']:
            with self.engine.connect() as cnxn:
                sql, _ = prepare('show grants')
                rows = cnxn.execute('show grants').fetchall()
            for row in rows:
                stmt = row[0]
                m = re.search(r"^GRANT\s+(.+?)\s+ON\s+(.+?)\s+TO\s+", stmt)
                if not m:
                    continue
                privs = m.group(1).strip().lower() + ','
                privs = [priv.strip() for priv in
                         re.findall(r'([^,(]+(?:\([^)]+\))?)\s*,\s*', privs)]
                obj = m.group(2).replace('"', '').strip()
                if obj == schema + '.*' or obj == '*.*':
                    for priv in privilege:
                        if priv in privs or 'all privileges' in privs:
                            privilege[priv] = 1
        elif self.engine.name == 'postgresql':
            # postgres has no schema privileges, but default privileges
            # should be 0, so that table privileges may override these
            pass
        else:
            # Privilege not implemented for oracle or mssql yet
            privilege.select = 1
            privilege.insert = 1
            privilege['update'] = 1
            privilege.delete = 1

        self._privilege = privilege
        return privilege

    def table_privilege(self, schema, table):
        """Return privileges of database user"""

        cfg = Settings()
        schema_privilege = self.schema_privilege(schema)

        privilege = Dict({
            'select': schema_privilege.select or 0,
            'insert': schema_privilege.insert or 0,
            'update': schema_privilege['update'] or 0,
            'delete': schema_privilege.delete or 0
        })
        if self.engine.name == 'sqlite' and cfg.database == 'urdr':
            db_path = self.engine.url.database
            db_name = self.engine.url.database.split(cfg.host)[1].lstrip('/')
            urdr = 'main' if db_path.endswith('/urdr.db') else 'urdr'
            sql = f"""
            select count(*) from {urdr}.table_access ta
            where database_name = :db_name and table_name = :table
            """
            with self.engine.connect() as cnxn:
                sql, params = prepare(sql, {'db_name': db_name, 'table': table})
                count = cnxn.execute(sql, params).fetchone()[0]
            if count:
                privilege.select = 0
                privilege.insert = 0
                privilege['update'] = 0
                privilege.delete = 0

                sql = f"""
                with recursive cte_access (code, parent) as (
                    select a1.code, a1.parent
                    from {urdr}.access a1
                    join user_access ua on ua.access_code = a1.code
                    where ua.user_id = :uid
                    union all
                    select a2.code, a2.parent
                    from {urdr}.access a2
                    join cte_access cte on cte.code = a2.parent
                )
                select count(*) from {urdr}.table_access ta
                where database_name = :db and table_name = :table and
                      (read_access is NULL or
                      read_access in (select code from cte_access))
                """
                with self.engine.connect() as cnxn:
                    sql, params = prepare(sql, {'uid': self.name, 'db': db_name, 'table': table})
                    count_read = cnxn.execute(sql, params).fetchone()[0]

                sql = f"""
                with recursive cte_access (code, parent) as (
                    select a1.code, a1.parent
                    from {urdr}.access a1
                    join user_access ua on ua.access_code = a1.code
                    where ua.user_id = :uid
                    union all
                    select a2.code, a2.parent
                    from {urdr}.access a2
                    join cte_access cte on cte.code = a2.parent
                )
                select count(*) from {urdr}.table_access ta
                where database_name = :db and table_name = :table and
                      write_access in (select code from cte_access)
                """
                with self.engine.connect() as cnxn:
                    sql, params = prepare(sql, {'uid': self.name, 'db': db_name, 'table': table})
                    count_write = cnxn.execute(sql, params).fetchone()[0]

                if count_read:
                    privilege.select = 1
                if count_write:
                    privilege.insert = 1
                    privilege['update'] = 1
                    privilege.delete = 1

        if self.engine.name in ['mysql', 'mariadb']:
            with self.engine.connect() as cnxn:
                sql, _ = prepare('show grants')
                rows = cnxn.execute(sql).fetchall()
            for row in rows:
                stmt = row[0]
                matched = re.search(r"^GRANT\s+(.+?)\s+ON\s+(.+?)\s+TO\s+",
                                    stmt)
                if not matched:
                    continue
                privs = matched.group(1).strip().lower() + ','
                privs = [priv.strip() for priv in
                         re.findall(r'([^,(]+(?:\([^)]+\))?)\s*,\s*', privs)]
                obj = matched.group(2).replace('"', '').strip()
                if obj == schema + '.' + table:
                    for priv in privilege:
                        if priv in privs:
                            privilege[priv] = 1
        elif self.engine.name == 'postgresql':
            sql = """
            select privilege_type
            from information_schema.table_privileges
            where grantee in (
                WITH RECURSIVE cte AS (
                   SELECT oid FROM pg_roles WHERE rolname = current_user

                   UNION ALL
                   SELECT m.roleid
                   FROM   cte
                   JOIN   pg_auth_members m ON m.member = cte.oid
                )
                SELECT oid::regrole::text AS rolename FROM cte
            )
            and table_schema = :schema
            and table_name = :table;
            """
            sql, params = prepare(sql, {'schema': schema, 'table': table})
            with self.engine.connect() as cnxn:
                rows = cnxn.execute(sql, params).fetchall()
            for row in rows:
                if row.privilege_type == 'SELECT':
                    privilege.select = 1
                elif row.privilege_type == 'INSERT':
                    privilege.insert = 1
                elif row.privilege_type == 'UPDATE':
                    privilege['update'] = 1
                elif row.privilege_type == 'DELETE':
                    privilege.delete = 1

        return privilege

    def is_admin(self, schema):
        if schema in self._is_admin:
            return self._is_admin[schema]
        self._is_admin[schema] = False

        cfg = Settings()
        if self.engine.name == 'sqlite' and cfg.database == 'urdr':
            return 'sysadmin' in self.access_codes
        elif self.engine.name in ['mysql', 'mariadb']:
            with self.engine.connect() as cnxn:
                sql, _ = prepare('show grants')
                rows = cnxn.execute(sql).fetchall()
            for row in rows:
                stmt = row[0]
                grant = re.search(r"^GRANT\s+(.+?)\s+ON\s+(.+?)\s+TO\s+", stmt)
                if not grant:
                    continue
                privs = grant.group(1).strip().lower() + ','
                expr = r'([^,(]+(?:\([^)]+\))?)\s*,\s*'
                privs = [priv.strip() for priv in re.findall(expr, privs)]
                obj = grant.group(2).replace('"', '').strip()
                if obj == schema + '.*' or obj == '*.*':
                    if 'all privileges' in privs:
                        self._is_admin[schema] = True
        elif self.engine.name == 'postgresql':
            sql = "select usesuper from pg_user where usename = current_user;"
            with self.engine.connect() as cnxn:
                sql, _ = prepare(sql)
                row = cnxn.execute(sql).fetchone()
            super = row[0]
            if super:
                self._is_admin[schema] = True

            # Find if user owns the database
            sql = """
            select pg_catalog.pg_get_userbyid(d.datdba) as db_owner
            from pg_catalog.pg_database d
            where d.datname = :cat
            """
            with self.engine.connect() as cnxn:
                sql, params = prepare(sql, {'cat': self.engine.url.database})
                row = cnxn.execute(sql, params).fetchone()
            if row.db_owner == self.name:
                self._is_admin[schema] = True

        elif self.engine.name == 'oracle':
            if self.user.name == self.db.schema:
                self._is_admin['schema'] = True
        else:
            self._is_admin[schema] = True

        return self._is_admin[schema]
