import pyodbc
import os
from schema import Schema
from config import config
from expression import Expression

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

        schema = Schema(self.schema)

        self.tables = schema.tables
        self.reports = schema.reports
        self.contents = schema.contents

    def get_info(self):

        branch = os.system('git rev-parse --abbrev-ref HEAD')
        branch = branch if branch else ''

        params = ['admin'] # todo: Autentisering

        q = """ 
        select count(*) from role_permission
        where role_ in (select role_ from user_role where user = ?)
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
                "tables": self.filter_tables(),
                "reports": self.reports,
                "contents": self.contents
            },
            # "config": self.config, # todo: have we got this?
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

    def filter_tables(self):
        user = 'admin' # todo: autentisering

        # Finds the tables the user has permission to view
        sql = """
        select table_, view_
        from role_permission r
        where role_ in (select role_ from user_role where user_ = ?)
          and (schema_ = '*' or schema_ = ?)
        """

        cursor = self.urd.cursor()
        rows = cursor.execute(sql, user, self.schema).fetchall()
        rights = {row.table_: row.view_ for row in rows}

        sql = """
        select table_, expression exp
        from filter f
        where schema_ = ?
          and user_ in (?, 'urd')
          and standard = '1'
        """

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
