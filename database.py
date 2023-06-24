"""Module for handling databases and connections"""
import os
import time
import pyodbc
from sqlglot import parse_one, exp
from fastapi import HTTPException
from starlette import status
import simplejson as json
from addict import Dict
from expression import Expression
from ruamel.yaml import YAML
from ruamel.yaml.compat import StringIO
from table import Table


def measure_time(func):
    def wrapper(*arg):
        t = time.time()
        res = func(*arg)
        if (time.time()-t) > 1:
            print("Time in", func.__name__,  str(time.time()-t), "seconds")
        return res

    return wrapper


class MyYAML(YAML):
    def dump(self, data, stream=None, **kw):
        inefficient = False
        if stream is None:
            inefficient = True
            stream = StringIO()
        YAML.dump(self, data, stream, **kw)
        if inefficient:
            return stream.getvalue()


class Connection:
    """Connect to database"""

    def __init__(self, cfg, db_name=None):
        self.system = cfg.db_system
        self.server = cfg.db_server
        driver = self.get_driver()
        cnxnstr = 'Driver={' + driver + '};'
        if (db_name and cfg.db_system != 'oracle'):
            path = db_name.split('.')
            cnxnstr += 'Database=' + path[0] + ';'
        if cfg.db_system == 'oracle':
            cnxnstr += "DBQ=" + cfg.db_server + ';'
        else:
            srv_parts = cfg.db_server.split(':')
            cnxnstr += 'Server=' + srv_parts[0] + ';'
            if len(srv_parts) == 2:
                cnxnstr += 'Port=' + srv_parts[1] + ';'
        cnxnstr += 'Uid=' + cfg.db_uid + ';Pwd=' + cfg.db_pwd + ';'
        pyodbc.lowercase = True
        if cfg.db_system == 'sql server':
            cnxnstr += 'ENCRYPT=no;MARS_Connection=yes;'
            pyodbc.lowercase = False
        if cfg.db_system == 'sqlite3':
            pyodbc.lowercase = False
            path = os.path.join(cfg.db_server, db_name)
            cnxnstr = 'Driver=SQLite3;Database=' + path
            if os.path.exists(path):
                cnxn = pyodbc.connect(cnxnstr)
            else:
                raise HTTPException(
                    status_code=404, detail="Database not found"
                )
        else:
            try:
                cnxn = pyodbc.connect(cnxnstr)
            except Exception as e:
                print(e)
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid authentication"
                )
        cnxn.setencoding(encoding='utf8')
        self.cursor = cnxn.cursor
        self.user = cfg.db_uid
        self.expr = Expression(self.system)
        self.string = cnxnstr

    def get_driver(self):
        """Get ODBC driver"""
        drivers = [d for d in pyodbc.drivers() if self.system in d.lower()]
        drivers.sort(reverse=True, key=lambda x: 'unicode' in x.lower())

        try:
            return drivers[0]
        except IndexError:
            raise HTTPException(
                status_code=501, detail=self.system + " ODBC driver missing"
            )

    def get_databases(self):
        """Get all databases in database system"""
        sql = self.expr.databases()
        rows = self.cursor().execute(sql).fetchall()

        return [row[0] for row in rows]


class Database:
    """Contains methods for getting data and metadata from database"""

    def __init__(self, cnxn, db_name):
        self.cnxn = cnxn
        self.name = db_name
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
        elif cnxn.system == 'sqlite3':
            self.schema = 'main'
            self.cat = None
        elif cnxn.system == 'sql server':
            path = db_name.split('.')
            self.cat = path[0]
            self.schema = 'dbo' if len(path) == 1 else path[1]
        else:
            self.schema = 'public'
            self.cat = None
        self.system = cnxn.system
        self.expr = Expression(cnxn.system)
        self.user_tables = self.get_user_tables()
        self.attrs = Dict(self.get_html_attributes('database', self.name))
        self.attrs.cache = self.attrs.pop('data-cache', None)
        if self.attrs.get('cache.config', None):
            self.config = self.attrs.cache.config
        else:
            self.config = Dict()

    def get_html_attributes(self, element=None, identifier=None):
        """Get terms from table meta_terms"""
        if not hasattr(self, 'html_attributes'):
            self.init_html_attributes()
        if not element:
            return self.html_attributes
        else:
            return self.html_attributes[element][identifier]

    def init_html_attributes(self):
        """Store terms in database object"""
        cursor = self.cnxn.cursor()
        attrs = Dict()
        if 'html_attributes' in self.user_tables:
            sql = f"""
            select element, identifier, attributes as attrs
            from {self.schema or self.cat}.html_attributes
            """
            try:
                rows = cursor.execute(sql).fetchall()
                for row in rows:
                    attrs[row.element][row.identifier] = json.loads(row.attrs)
            except Exception as e:
                print(e)

        self.html_attributes = attrs

    @measure_time
    def get_info(self):
        """Get info about database"""

        branch = os.system('git rev-parse --abbrev-ref HEAD')
        branch = branch if branch else ''

        info = {
            "branch": branch,
            "base": {
                "name": self.name,
                "system": self.cnxn.system,
                "server": self.cnxn.server,
                "schema": self.schema,
                "schemata": self.get_schemata(),
                "label": self.attrs.get('label', self.name.capitalize()),
                "tables": self.get_tables(),
                "contents": self.get_contents(),
                "description": self.attrs.get('title', None)
            },
            "user": {
                "name": 'Admin',  # TODO: Autentisering
                "id": 'admin',  # TODO: Autentisering
                "admin": self.get_privileges().create
            },
            "config": (None if not self.attrs.get('cache', None)
                       else self.attrs.cache.config)
        }

        return info

    @measure_time
    def get_privileges(self):
        """Get user privileges"""
        privilege = Dict()
        sql = self.expr.privilege()
        cursor = self.cnxn.cursor()

        if not sql:
            privilege.create = 1
        else:
            priv = cursor.execute(sql, self.schema or self.cat).fetchone()
            privilege.create = int(priv.create)
            privilege.usage = 0

        return privilege

    @measure_time
    def get_schemata(self):
        """Get all schemata in database"""
        cursor = self.cnxn.cursor()
        schemata = []

        if self.cnxn.system == 'postgres':
            sql = self.expr.schemata()
            rows = cursor.execute(sql).fetchall()
            for row in rows:
                schemata.append(row.schema_name)

        return schemata

    @measure_time
    def get_user_tables(self):
        """Get tables user has access to"""
        sql = self.expr.user_tables()
        cursor = self.cnxn.cursor()
        user_tables = []

        if self.cnxn.system == 'sqlite3':
            rows = cursor.execute(sql).fetchall()
        else:
            rows = cursor.execute(sql, self.schema or self.cat).fetchall()
        for row in rows:
            user_tables.append(row.table_name)

        return user_tables

    def create_html_tables(self):
        """Create tables holding meta data"""
        cursor = self.cnxn.cursor()
        string_datatype = self.expr.to_native_type('string')

        sql = """
        create table html_element (
        element varchar(16) not null,
        primary key (element)
        );
        """

        cursor.execute(sql)

        sql = """
        insert into html_element values (?);
        """

        params = [('database'), ('tableset'), ('table'), ('fieldset'),
                  ('field')]

        for param in params:
            cursor.execute(sql, param)

        sql = f"""
        CREATE TABLE html_attributes (
        element varchar(16) NOT NULL,
        identifier varchar(64),
        attributes {string_datatype},
        PRIMARY KEY (element, identifier),
        foreign key (element) references html_element(element)
        )
        """
        cursor.execute(sql)

        sql = """
        create index html_attributes_element on html_attributes(element)
        """

        cursor.execute(sql)

        self.attrs.cache = None
        self.user_tables.append('html_attributes')
        attributes = {
            'data-type': 'json',
            'data-format': 'yaml',
            'class': 'w7 bg-near-white'
        }

        sql = f"""
            insert into html_attributes (element, identifier, attributes)
            values ('field', 'attributes', '{json.dumps(attributes)}')
        """
        cursor.execute(sql)
        # Refresh attributes
        self.init_html_attributes()

        cursor.commit()

    @measure_time
    def get_tables(self):
        """Return metadata for every table"""
        # Return metadata from cache if set
        if (self.attrs.get('cache', None) and not self.config):
            self.tables = self.attrs.cache.tables
            return self.tables

        cursor = self.cnxn.cursor()
        tables = Dict()

        if (self.config and 'html_attributes' not in self.user_tables):
            self.create_html_tables()

        start = time.time()
        rows = cursor.tables(catalog=self.cat, schema=self.schema).fetchall()
        end = time.time()
        print('cursor.tables', end - start)

        tbl_names = [row.table_name for row in rows]

        for tbl in rows:
            tbl_name = tbl.table_name

            if tbl_name[-5:] == '_view' and tbl_name[:-5] in tbl_names:
                continue

            if tbl_name == 'sqlite_sequence':
                continue

            if tbl_name not in self.user_tables:
                continue

            hidden = tbl_name[0:1] == "_"

            table = Table(self, tbl_name)
            table.cache.pkey = self.get_pkey(tbl_name)
            main_type = tbl.table_type.lower()
            tbl_type = table.get_type(main_type)

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
                if tbl_type != self.config.tables[tbl_name].type:
                    tbl_type = self.config.tables[tbl_name].type
                else:
                    del self.config.tables[tbl_name].type
                    if not self.config.tables[tbl_name]:
                        del self.config.tables[tbl_name]

            if self.config:
                table.rowcount = table.count_rows()
                space = ' ' * (30 - len(tbl_name))
                print('gjennomgår tabell: ',
                      tbl_name + space + f"({table.rowcount})")
                table.fields = table.get_fields()
                table.relations = table.get_relations()

            view = tbl_name
            if tbl_name + '_view' in tbl_names:
                view = tbl_name + '_view'

            tables[tbl_name] = Dict({
                'name': tbl_name,
                'type': tbl_type,
                'view': view,
                'icon': None,
                'label': self.get_label('table', tbl_name),
                'rowcount': None if not self.config else table.rowcount,
                'pkey': table.cache.pkey,
                'description': tbl.remarks,
                'indexes': self.get_indexes(tbl_name),
                'fkeys': self.get_fkeys(tbl_name),
                'relations': (self.get_relations(tbl_name) if not self.config
                              else table.relations),
                'hidden': hidden,
                # fields are needed only when creating cache
                'fields': None if not self.config else table.fields,
            })

        self.tables = tables

        return tables

    def is_top_level(self, table):
        """Check if table is top level, i.e. not subordinate to other tables"""
        if (table.type == 'list'):
            return False

        for fkey in table.fkeys.values():
            if fkey.table not in self.tables:
                continue

            # Not top level if has foreign keys to other table
            # that is not a hidden table
            if fkey.table != table.name:
                fk_table = self.tables[fkey.table]
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

    @measure_time
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

    @measure_time
    def get_tbl_groups(self):
        """Group tables by prefix or relations"""
        tbl_groups = Dict()

        # If not generating cache or generating cache for databases
        # with Urdr structure. This is the default behaviour, which
        # treats databases as following the Urdr rules for self
        # documenting databases
        if (not self.config.update_cache or self.config.urd_structure):
            for tbl_name, table in self.tables.items():
                if (tbl_name[-5:] == '_grid' and table.type == 'view'):
                    continue
                if tbl_name[0:1] == "_":
                    name = tbl_name[1:]
                else:
                    name = tbl_name
                group = name.split("_")[0]

                # Don't include tables that are subordinate to other tables
                # i.e. the primary key also has a foreign key
                # These are handled in get_content_node
                subordinate = False
                tbl = Table(self, tbl_name)
                for colname in table.pkey.columns:
                    if tbl.get_fkey(colname):
                        subordinate = True
                        break

                if not subordinate:
                    if not tbl_groups[group]:
                        tbl_groups[group] = []

                    tbl_groups[group].append(tbl_name)
        else:
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

    @measure_time
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
                        name_parts[0] != fkey.table
                    ):
                        continue

                    if fkey.table not in sub_tables:
                        sub_tables[fkey.table] = []

                    sub_tables[fkey.table].append(tbl_name)
                    break

        return sub_tables

    def get_label(self, element, identifier, prefix=None, postfix=None):
        """Get label based on identifier"""
        attrs = self.get_html_attributes()
        if (
            identifier in attrs[element] and
            'data-label' in attrs[element][identifier]
        ):
            label = attrs[element][identifier]['data-label']
        else:
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
                label = self.get_label('table', subtable, prefix=tbl_name)
                node.subitems[label] = self.get_content_node(subtable)

        return node

    @measure_time
    def get_contents(self):
        """Get list of contents"""
        if (self.attrs.get('cache', None) and not self.config):
            self.contents = self.attrs.cache.contents
            return self.contents

        contents = Dict()

        tbl_groups = self.get_tbl_groups()
        self.sub_tables = self.get_sub_tables()

        for group_name, table_names in tbl_groups.items():
            if len(table_names) == 1:  # and group_name != "meta":
                tbl_name = table_names[0]
                label = self.get_label('table', tbl_name)

                contents[label] = self.get_content_node(tbl_name)

            else:
                label = self.get_label('tableset', group_name)
                table_names = list(set(table_names))

                contents[label] = Dict({
                    'class_label': "b",
                    'class_content': "ml3",
                    'count': len(table_names)
                })

                table_names.sort()
                for tbl_name in table_names:
                    # Remove group prefix from label
                    tbl_label = self.get_label('table', tbl_name,
                                               prefix=group_name)

                    contents[label].subitems[tbl_label] = \
                        self.get_content_node(tbl_name)

        if self.config:
            cursor = self.cnxn.cursor()
            sql = """
            select count(*) from html_attributes
            where element = ? and identifier = ?
            """
            count = cursor.execute(sql, 'database', self.name).fetchval()

            cache = {
                "tables": self.tables,
                "contents": contents,
                "config": self.config
            }
            self.attrs['data-cache'] = cache
            self.attrs.pop('cache')
            attrs_txt = json.dumps(self.attrs)

            if count:
                sql = """
                update html_attributes
                set attributes = ?
                where element = ? and identifier = ?
                """
            else:
                sql = """
                insert into html_attributes(attributes, element, identifier)
                values (?, ?, ?)
                """

            cursor.execute(sql, attrs_txt, 'database', self.name).commit()

        return contents

    def get_indexes(self, tbl_name):
        """Get all indexes for table"""
        if not hasattr(self, 'indexes'):
            self.init_indexes()

        return self.indexes[tbl_name]

    def get_pkey(self, tbl_name):
        """Get primary key of table"""
        if not hasattr(self, 'pkeys'):
            self.init_pkeys()

        return self.pkeys[tbl_name]

    def get_fkeys(self, tbl_name):
        """Get all foreign keys of table"""
        if not hasattr(self, 'fkeys'):
            self.init_fkeys()

        return self.fkeys[tbl_name]

    def get_relations(self, tbl_name):
        """Get all has-many relations of table"""
        if not hasattr(self, 'relations'):
            self.init_relations()

        return self.relations[tbl_name]

    def query_result(self, sql, limit):
        query = Dict()
        query.string = sql.strip()
        if len(query.string) == 0:
            return None
        t1 = time.time()
        try:
            cursor = self.query(query.string)
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

        query.success = True
        query.time = round(time.time() - t1, 4)

        if cursor.description:
            if limit:
                rows = cursor.fetchmany(limit)
            else:
                rows = cursor.fetchall()

            query.data = []

            # Find the table selected from
            query.table = str(parse_one(query.string).find(exp.Table))
            colnames = [column[0] for column in cursor.description]
            for row in rows:
                query.data.append(dict(zip(colnames, row)))
        else:
            rowcount = cursor.rowcount

            cursor.commit()

            query.rowcount = rowcount
            query.result = f"Query OK, {rowcount} rows affected"

        return query

    def query(self, sql, params=[]):
        """Execute sql query"""
        cursor = self.cnxn.cursor()
        t = time.time()
        cursor.execute(sql, params)
        if (time.time()-t) > 1:
            print("Query took " + str(time.time()-t) + " seconds")
            print('query:', sql)
        return cursor

    @measure_time
    def init_indexes(self):
        """Store all indexes in database object"""
        crsr = self.cnxn.cursor()
        indexes = Dict()
        if self.cnxn.system in ['mysql', 'oracle']:
            sql = self.expr.indexes()
            for row in crsr.execute(sql, self.cat or self.schema):
                name = row.index_name

                indexes[row.table_name][name].name = name
                indexes[row.table_name][name].unique = not row.non_unique
                if 'columns' not in indexes[row.table_name][name]:
                    indexes[row.table_name][name].columns = []
                indexes[row.table_name][name].columns.append(row.column_name)
        else:
            tbls = crsr.tables(catalog=self.cat, schema=self.schema).fetchall()
            for tbl in tbls:
                tbl_name = tbl.table_name
                for row in crsr.statistics(tbl_name):
                    name = row.index_name
                    indexes[tbl_name][name].name = name
                    indexes[tbl_name][name].unique = not row.non_unique
                    if 'columns' not in indexes[tbl_name][name]:
                        indexes[tbl_name][name].columns = []
                    indexes[tbl_name][name].columns.append(row.column_name)

        self.indexes = indexes

    @measure_time
    def init_pkeys(self):
        """Store all primary keys in database object"""
        crsr = self.cnxn.cursor()
        pkeys = Dict()
        if self.cnxn.system in ['mysql', 'oracle']:
            sql = self.expr.pkeys()
            rows = crsr.execute(sql, self.schema or self.cat)
            colnames = [column[0] for column in rows.description]
            for row in rows:
                pkeys[row.table_name].table_name = row.table_name
                pkeys[row.table_name].pkey_name = row.index_name
                if 'columns' not in pkeys[row.table_name]:
                    pkeys[row.table_name].columns = []
                    pkeys[row.table_name].data_types = []
                pkeys[row.table_name].columns.append(row.column_name)
                if 'data_type' in colnames:
                    pkeys[row.table_name].data_types.append(row.data_type)
        else:
            tbls = crsr.tables(catalog=self.cat, schema=self.schema).fetchall()
            for tbl in tbls:
                if self.cnxn.system in ['sqlite3']:
                    # Wrong order for pkeys using cursor.primaryKeys
                    sql = self.expr.pkey(tbl.table_name)
                    rows = self.query(sql)
                else:
                    rows = crsr.primaryKeys(table=tbl.table_name,
                                            catalog=self.cat,
                                            schema=self.schema)
                for row in rows:
                    pkeys[tbl.table_name].table_name = tbl.table_name
                    pkeys[tbl.table_name].pkey_name = row.pk_name
                    if 'columns' not in pkeys[tbl.table_name]:
                        pkeys[tbl.table_name].columns = []
                    pkeys[tbl.table_name].columns.append(row.column_name)

        self.pkeys = pkeys

    @measure_time
    def init_fkeys(self):
        """Store all foreign keys in database object"""
        crsr = self.cnxn.cursor()
        fkeys = Dict()
        if self.cnxn.system in ["mysql", "oracle", "postgres"]:
            sql = self.expr.fkeys()
            for row in crsr.execute(sql, self.schema or self.cat):
                name = row.fk_name
                fkeys[row.fktable_name][name].name = row.fk_name
                fkeys[row.fktable_name][name].table = row.pktable_name
                fkeys[row.fktable_name][name].base = self.cat
                fkeys[row.fktable_name][name].schema = row.pktable_schema
                fkeys[row.fktable_name][name].delete_rule = row.delete_rule
                if 'foreign' not in fkeys[row.fktable_name][name]:
                    fkeys[row.fktable_name][name].foreign = []
                    fkeys[row.fktable_name][name].primary = []
                fkeys[row.fktable_name][name].foreign.append(row.fkcolumn_name)
                fkeys[row.fktable_name][name].primary.append(row.pkcolumn_name)
        else:
            rows = crsr.tables(catalog=self.cat, schema=self.schema).fetchall()
            for row in rows:
                tbl = Table(self, row.table_name)
                fkeys[row.table_name] = tbl.get_fkeys()

        self.fkeys = fkeys

    @measure_time
    def init_relations(self):
        """Store all has-many relations in database object"""
        if not hasattr(self, 'fkeys'):
            self.init_fkeys()

        relations = Dict()

        for fktable_name, fkeys in self.fkeys.items():
            for fkey in fkeys.values():
                # For mysql fkey.schema refers to catalog/database
                if fkey.schema == self.schema or self.cat:
                    relations[fkey.table][fkey.name] = Dict({
                        "name": fkey.name,
                        "table": fktable_name,
                        "base": fkey.base or None,
                        "schema": fkey.schema,
                        "delete_rule": fkey.delete_rule,
                        "foreign": fkey.foreign,
                        "primary": fkey.primary,
                        "label": self.get_label('table', fkey.table)
                    })

        self.relations = relations

    def export_as_sql(self, dialect: str, include_recs: bool,
                      select_recs: bool):
        """Create sql for exporting a database

        Parameters:
        dialect: The sql dialect used (mysql, postgres, sqlite)
        include_recs: If records should be included
        select_recs: If included records should be selected from
                     existing database
        """
        ddl = ''
        if dialect == 'mysql':
            ddl += 'SET foreign_key_checks = 0;'
        cursor = self.cnxn.cursor()
        tbls = cursor.tables(catalog=self.cat, schema=self.schema).fetchall()
        for tbl in tbls:
            if tbl.table_name == 'sqlite_sequence':
                continue
            table = Table(self, tbl.table_name)
            ddl += table.export_ddl(dialect)
            if include_recs:
                ddl += table.export_records(select_recs)

        if dialect == 'mysql':
            ddl += 'SET foreign_key_checks = 1;'

        return ddl
