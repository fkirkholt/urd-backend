import re
from addict import Dict
from sqlalchemy import text, inspect
from settings import Settings


class User:

    def __init__(self, engine, name=None):
        self.name = name or engine.url.username
        self.engine = engine
        self.current = name is None
        self._is_admin = {}

    def databases(self, schema=None, cat=None):
        if self.engine.name == 'sqlite':
            sql = """
            with recursive cte_access (code, parent) as (
                select a1.code, a1.parent
                from access a1
                join user_access ua on ua.access_code = a1.code
                where ua.user_id = :uid
                union all
                select a2.code, a2.parent
                from access a2
                join cte_access cte on cte.code = a2.parent
            )
            select name, description
            from database_ db
            left join database_access dba on dba.database_name = db.name
            where read_access is null
               or read_access in (select code from cte_access)
            """

        elif self.engine.name == 'postgresql' and schema and schema != 'public':
            sql = f"""
            select '{schema}' as db_name,
                   obj_description('{schema}'::regnamespace) as db_comment
            """
        elif self.engine.name == 'postgresql':
            sql = """
            select d.datname as db_name,
                   shobj_description(d.oid, 'pg_database') as db_comment
            from pg_database d
            where datistemplate is false and datname != 'postgres'
                  and :cat is null or d.datname = :cat
            """
        elif self.engine.name == 'oracle':
            # Oracle doesn't support comments on schemas
            sql = """
            SELECT DISTINCT owner as db_name, NULL as db_comment
            FROM ALL_OBJECTS
            WHERE OBJECT_TYPE = 'TABLE'
            order by owner
            """
        elif self.engine.name == 'mysql':
            sql = """
            select schema_name as db_name, NULL as db_comment
            from information_schema.schemata
            """
        elif self.engine.name == 'mariadb':
            sql = """
            select schema_name as db_name, schema_comment as db_comment
            from information_schema.schemata
            where :schema is null or schema_name = :schema
            """
        elif self.engine.name == 'mssql':
            sql = """
            select name as db_name, NULL as db_comment
            from sys.Databases
            WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
                  and HAS_DBACCESS(name) = 1;
            """

        with self.engine.connect() as conn:
            if self.engine.name == 'sqlite':
                params = {'uid': self.name}
            else:
                params = {'schema': schema, 'cat': cat}
            rows = conn.execute(text(sql), params).fetchall()

        return rows

    def tables(self, schema):
        cfg = Settings()
        refl = inspect(self.engine)
        tbl_names = refl.get_table_names(schema)
        if self.engine.name == 'sqlite' and cfg.database == 'urdr.db':
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
                rows = cnxn.execute(text(sql), params).fetchall()

                for row in rows:
                    tbl_names.remove(row[0])

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
            else:
                sql = """
                select Role as role_name
                from mysql.roles_mapping
                where User = :user
                """

            with self.engine.connect() as conn:
                rows = conn.execute(text(sql), {'user': self.name}).fetchall()

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
                rows = cnxn.execute(text(sql), {'uid': self.name}).fetchall()

            for row in rows:
                self._access_codes.append(row.code)

        return self._access_codes

    def schema_privilege(self, schema):
        """Get user privileges"""
        privilege = Dict()
        privilege.select = 0
        privilege.insert = 0
        privilege['update'] = 0
        privilege.delete = 0
        privilege.create = 0

        cfg = Settings()

        if self.engine.name == 'sqlite' and cfg.database == 'urdr.db':
            db_path = self.engine.url.database
            db_name = self.engine.url.database.split(cfg.host)[1].lstrip('/')
            urdr = 'main' if db_path.endswith('/urdr.db') else 'urdr'

            sql = f"""
            select read_access, write_access from {urdr}.database_access
            where database_name = :db_name
            """

            with self.engine.connect() as cnxn:
                params = {'db_name': db_name}
                row = cnxn.execute(text(sql), params).first()

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
            if 'sysadmin' in self.access_codes: 
                privilege.create = 1

        elif self.engine.name in ['mysql', 'mariadb']:
            with self.engine.connect() as cnxn:
                rows = cnxn.execute(text('show grants')).fetchall()
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
            sql = """
            select pg_catalog.has_schema_privilege(current_user, nspname, 'CREATE') "create",
                   pg_catalog.has_schema_privilege(current_user, nspname, 'USAGE') "usage"
            from pg_catalog.pg_namespace
            where nspname = :schema
            """

            with self.engine.connect() as cnxn:
                param = {'schema': schema}
                priv = cnxn.execute(text(sql), param).first()
            privilege.create = priv.create
            if self.is_admin(schema):
                for priv in privilege:
                    privilege[priv] = 1
        else:
            # Privilege not implemented for oracle or mssql yet
            privilege.select = 1
            privilege.insert = 1
            privilege['update'] = 1
            privilege.delete = 1
            privilege.create = 1

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
        if self.engine.name == 'sqlite' and cfg.database == 'urdr.db':
            db_path = self.engine.url.database
            db_name = self.engine.url.database.split(cfg.host)[1].lstrip('/')
            urdr = 'main' if db_path.endswith('/urdr.db') else 'urdr'
            sql = f"""
            select count(*) from {urdr}.table_access ta
            where database_name = :db_name and table_name = :table
            """
            with self.engine.connect() as cnxn:
                params = {'db_name': db_name, 'table': table}
                count = cnxn.execute(text(sql), params).first()[0]
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
                    params = {'uid': self.name, 'db': db_name, 'table': table}
                    count_read = cnxn.execute(text(sql), params).first()[0]

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
                    params = {'uid': self.name, 'db': db_name, 'table': table}
                    count_write = cnxn.execute(text(sql), params).first()[0]

                if count_read:
                    privilege.select = 1
                if count_write:
                    privilege.insert = 1
                    privilege['update'] = 1
                    privilege.delete = 1

        if self.engine.name in ['mysql', 'mariadb']:
            with self.engine.connect() as cnxn:
                rows = cnxn.execute(text('show grants')).fetchall()
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
            params = {'schema': schema, 'table': table}
            with self.engine.connect() as cnxn:
                rows = cnxn.execute(text(sql), params).fetchall()
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
        if self.engine.name == 'sqlite' and cfg.database == 'urdr.db':
            return 'sysadmin' in self.access_codes
        elif self.engine.name in ['mysql', 'mariadb']:
            with self.engine.connect() as cnxn:
                rows = cnxn.execute(text('show grants')).fetchall()
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
                row = cnxn.execute(text(sql)).first()
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
                params = {'cat': self.engine.url.database}
                row = cnxn.execute(text(sql), params).first()
            if row[0] == self.name:
                self._is_admin[schema] = True

        elif self.engine.name == 'oracle':
            if self.user.name == self.db.schema:
                self._is_admin['schema'] = True
        else:
            self._is_admin[schema] = True

        return self._is_admin[schema]
