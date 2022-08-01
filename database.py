"""Module for handling databases and connections"""
import os
import time
import pyodbc
from fastapi import HTTPException
from starlette import status
import simplejson as json
from addict import Dict
from expression import Expression
from ruamel.yaml import YAML

def measure_time(func):
    def wrapper(*arg):
        t = time.time()
        res = func(*arg)
        if (time.time()-t) > 1:
            print("Time in", func.__name__,  str(time.time()-t), "seconds")
        return res

    return wrapper

class Connection:
    """Connect to database"""
    def __init__(self, cfg, db_name=None):
        self.system = cfg.db_system
        self.server = cfg.db_server
        driver = self.get_driver()
        cnxnstr = f'Driver={driver};'
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
        if (cfg.db_system == 'sqlite3'):
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
            except Exception:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication"
                )
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
        elif cnxn.system == 'sqlite3':
            self.schema = 'main'
            self.cat = None
        else:
            self.schema = 'public'
            self.cat = None
        self.system = cnxn.system
        self.expr   = Expression(cnxn.system)
        self.user_tables = self.get_user_tables()
        self.metadata = self.get_metadata()
        self.config = Dict()

    def get_metadata(self):
        """Get data from table meta_data"""
        if not hasattr(self, 'metadata'):
            self.init_metadata()
        return self.metadata

    @measure_time
    def init_metadata(self):
        """Store metadata in database object"""
        cursor = self.cnxn.cursor()
        metadata = Dict()
        if 'meta_data' in self.user_tables:
            sql = f"select * from {self.schema or self.cat}.meta_data"
            row = cursor.execute(sql).fetchone()
            colnames = [col[0] for col in cursor.description]
            if row:
                metadata = Dict(zip(colnames, row))

        if metadata.cache:
            metadata.cache = Dict(json.loads(metadata.cache))
        self.metadata = metadata

    def get_terms(self):
        """Get terms from table meta_terms"""
        if not hasattr(self, 'terms'):
            self.init_terms()
        return self.terms

    def init_terms(self):
        """Store terms in database object"""
        # from table import Table
        cursor = self.cnxn.cursor()
        terms = Dict()
        if 'meta_term' in self.user_tables:
            sql = f"select * from {self.schema or self.cat}.meta_term"
            try:
                rows = cursor.execute(sql).fetchall()
                colnames = [column[0] for column in cursor.description]
                for row in rows:
                    terms[row.term] = Dict(zip(colnames, row))
            except:
                pass

        self.terms = terms

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
                "label": self.metadata.get('label', self.name.capitalize()),
                "tables": self.get_tables(),
                "contents": self.get_contents(),
                "description": self.metadata.get('description', None),
            },
            "user": {
                "name": 'Admin', #TODO: Autentisering
                "id": 'admin', #TODO: Autentisering
                "admin": self.get_privileges().create
            },
            "config": None if not self.metadata.get('cache', None) else self.metadata.cache.config
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

    @measure_time
    def get_tables(self):
        """Return metadata for every table"""
        from table import Table
        if (self.metadata.get('cache', None) and not self.config):
            self.tables = self.metadata.cache.tables
            return self.tables
        cursor = self.cnxn.cursor()
        tables = Dict()

        if (self.config and not 'cache' in self.metadata):
            sql = """
                CREATE TABLE meta_data (
                _name varchar(30) NOT NULL,
                label varchar(30),
                description text,
                cache json,
                PRIMARY KEY (_name)
            );
            """
            cursor.execute(sql)
            label = self.get_label(self.name)

            sql = f"""
                insert into meta_data (_name, label)
                values('{self.name}', '{label}')
            """
            cursor.execute(sql)
            self.metadata.cache = None
            self.user_tables.append('meta_data')

        start = time.time()
        rows = cursor.tables(catalog=self.cat, schema=self.schema).fetchall()
        end = time.time()
        print('cursor.tables', end - start)

        for tbl in rows:
            tbl_name = tbl.table_name

            if tbl_name == 'sqlite_sequence':
                continue

            if tbl_name not in self.user_tables:
                continue

            hidden = tbl_name[0:1] == "_" or tbl_name[0:5] == "meta_"

            pkey = self.get_pkey(tbl_name)
            type_ = None
            if len(pkey):
                pkey_col_name = pkey[0]
                pkey_col = cursor.columns(catalog=self.cat, schema=self.schema,
                                        table=tbl_name, column=pkey_col_name).fetchone()
                pkey_col.type_name = pkey_col.type_name.split('(')[0]
                type_ = self.expr.to_urd_type(pkey_col.type_name)

            tbl_type = tbl.table_type.lower()
            if (tbl_name[-5:] == "_list" or tbl_name[-6:] == "_liste"):
                tbl_type = "list"
            elif tbl_name[-5:] in ("_xref", "_link"):
                tbl_type = "xref"
            elif type_ == 'string':
                tbl_type = "list"

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
                table = Table(self, tbl_name)
                table.rowcount = table.count_rows()
                space = ' ' * (30 - len(tbl_name))
                print('gjennomgår tabell: ', tbl_name + space + f"({table.rowcount})")
                table.fields = table.get_fields()
                table.relations = table.get_relations()

            tables[tbl_name] = Dict({
                'name': tbl_name,
                'type': tbl_type,
                'icon': None,
                'label': self.get_label(tbl_name),
                'rowcount': None if not self.config else table.rowcount,
                'primary_key': self.get_pkey(tbl_name),
                'description': tbl.remarks,
                'indexes': self.get_indexes(tbl_name),
                'foreign_keys': self.get_foreign_keys(tbl_name),
                'relations': self.get_relations(tbl_name) if not self.config else table.relations,
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

    def get_tbl_groups(self):
        """Group tables by prefix or relations"""
        tbl_groups = Dict()
        terms = self.get_terms()

        if (not self.metadata.get('cache') or self.config.urd_structure):
            for tbl_name, table in self.tables.items():
                if tbl_name[0:1] == "_":
                    name = tbl_name[1:]
                else:
                    name = tbl_name
                group = name.split("_")[0]

                # Don't include tables that are subordinate to other tables
                # i.e. the primary key also has a foreign key
                # These ar handled in get_content_node
                subordinate = False
                for colname in table.primary_key:
                    if self.get_col_fkey(colname, table.foreign_keys):
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
                    grouptables = self.get_relation_tables(table.name, [])

                    if table.name not in grouptables:
                        grouptables.append(table.name)
                    if len(grouptables) > 2:
                        tbl_groups[table.name] = grouptables
                    else:
                        tbl_groups['...'].extend(grouptables)
                elif table.type == 'list':
                    tbl_groups['...'].append(table.name)

            # Relocate tables between groups
            delete_groups = []
            for group_name, tbl_names in tbl_groups.items():
                for group_name2, tbl_names2 in tbl_groups.items():
                    if group_name2 == group_name:
                        continue

                    diff = set(tbl_names) - set(tbl_names2)
                    common = [tbl_name for tbl_name in tbl_names if tbl_name in tbl_names2]
                    len_combined = len(set(tbl_names + tbl_names2))


                    if len(common):
                        if (len(tbl_names) <= len(tbl_names2) and (len(diff) == 1 or len_combined < 15)):
                            tbl_groups[group_name2].extend(tbl_names)
                            delete_groups.append(group_name)
                            break
                        elif (len(tbl_names) <= len(tbl_names2) and len(common) == 1 and len(diff)) > 1:
                            # We want the common tables only in the smallest group
                            tbl_groups[group_name2].remove(common[0])
                        elif (len(tbl_names) <= len(tbl_names2)):
                            for tbl_name in common:
                                tbl_groups[group_name2].remove(tbl_name)

            for group_name in delete_groups:
                del tbl_groups[group_name]

        return tbl_groups

    def get_sub_tables(self):
        """Return Dict of tables with subordinate tables"""
        sub_tables = Dict()
        for tbl_name, table in self.tables.items():
            name_parts = tbl_name.split("_")

            for colname in table.primary_key:
                fkey = self.get_col_fkey(colname, table.foreign_keys)
                if fkey:
                    if (len(name_parts) > 1 and
                        name_parts[0] in self.tables and
                        name_parts[0] != fkey.table
                    ):
                        continue

                    if fkey.table not in sub_tables:
                        sub_tables[fkey.table] = []

                    sub_tables[fkey.table].append(tbl_name)
                    break

        return sub_tables

    def get_col_fkey(self, colname, fkeys):
        """Get foreign key based on last key column"""
        col_fkey = None
        for fkey in fkeys.values():
            if fkey.foreign[-1] == colname:
                col_fkey = fkey
                break

        return col_fkey


    def get_label(self, term):
        """Get label based on term"""
        terms = self.get_terms()
        term_parts = term.split('_')
        if term_parts[-1] in ("list", "liste", "xref", "link"):
            term = "_".join(term_parts[:-1])
        if term in terms:
            label = terms[term].label
        else:
            label = term.replace("_", " ")

        if self.config.norwegian_chars:
            label = label.replace("ae", "æ")
            label = label.replace("oe", "ø")
            label = label.replace("aa", "å")

        label = label.strip().capitalize()

        return label

    def get_attributes(self, table_name, term):
        """Get description based on term"""
        terms = self.get_terms()
        column_ref = table_name + '.' + term
        attributes = None
        yaml = YAML()
        if column_ref in terms:
            attributes = yaml.load(terms[column_ref].attributes)
        elif term in terms:
            attributes = yaml.load(terms[term].attributes)

        return attributes

    def get_content_node(self, tbl_name):
        """Return a node in the content list, based on a table"""
        if tbl_name not in self.sub_tables:
            node = "tables." + tbl_name
        else:
            node = Dict()
            node.item = "tables." + tbl_name
            node.subitems = Dict()

            for subtable in self.sub_tables[tbl_name]:
                label = subtable.replace(tbl_name + '_', '')
                label = self.get_label(label)
                node.subitems[label] = self.get_content_node(subtable)

        return node

    @measure_time
    def get_contents(self):
        """Get list of contents"""
        if (self.metadata.get('cache', None) and not self.config):
            self.contents = self.metadata.cache.contents
            return self.contents

        contents = Dict()

        tbl_groups = self.get_tbl_groups()
        self.sub_tables = self.get_sub_tables()

        for group_name, table_names in tbl_groups.items():
            if len(table_names) == 1: # and group_name != "meta":
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
                    rest = tbl_name.replace(group_name+"_", "")
                    tbl_label = self.get_label(rest)

                    contents[label].subitems[tbl_label] = self.get_content_node(tbl_name)

        if ('cache' in self.metadata and self.config):
            cursor = self.cnxn.cursor()
            # self.cache = tables
            sql = "update meta_data set cache = ?\n"
            sql+= "where _name = ?"
            cache = {
                "tables": self.tables,
                "contents": contents,
                "config": self.config
            }
            cursor.execute(sql, json.dumps(cache), self.name).commit()

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

    def get_foreign_keys(self, tbl_name):
        """Get all foreign keys of table"""
        if not hasattr(self, 'fkeys'):
            self.init_foreign_keys()

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
            print('rows', rows)
            query.data = []
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

    @measure_time
    def init_pkeys(self):
        """Store all primary keys in database object"""
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
                if self.cnxn.system == 'sqlite3':
                    # Wrong order for pkeys using cursor.primaryKeys with SQLite
                    sql = self.expr.pkey(tbl.table_name)
                    rows = self.query(sql)
                else:
                    rows = cursor.primaryKeys(table=tbl.table_name, catalog=self.cat, schema=self.schema)
                pkey = [row.column_name for row in rows]
                pkeys[tbl.table_name] = pkey

        self.pkeys = pkeys

    @measure_time
    def init_foreign_keys(self):
        """Store all foreign keys in database object"""
        cursor = self.cnxn.cursor()
        fkeys = Dict()
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
                fkeys[row.fktable_name][name].foreign.append(row.fkcolumn_name)
                fkeys[row.fktable_name][name].primary.append(row.pkcolumn_name)
        else:
            from table import Table
            rows = cursor.tables(catalog=self.cat, schema=self.schema).fetchall()
            for row in rows:
                tbl = Table(self, row.table_name)
                fkeys[row.table_name] = tbl.get_fkeys()

        self.fkeys = fkeys

    @measure_time
    def init_relations(self):
        """Store all has-many relations in database object"""
        if not hasattr(self, 'fkeys'):
            self.init_foreign_keys()

        relations = Dict()

        for fktable_name, fkeys in self.fkeys.items():
            for fkey in fkeys.values():
                if fkey.schema == self.schema:
                    relations[fkey.table][fkey.name] = Dict({
                        "name": fkey.name,
                        "table": fktable_name,
                        "base": fkey.base or None,
                        "schema": fkey.schema,
                        "delete_rule": fkey.delete_rule,
                        "foreign": fkey.foreign,
                        "primary": fkey.primary,
                        "label": self.get_label(fkey.table) #TODO: Fix
                    })

        self.relations = relations

    def export_as_sql(self, dialect, include_recs):
        from table import Table
        ddl = ''
        cursor = self.cnxn.cursor()
        tbls = cursor.tables(catalog=self.cat, schema=self.schema).fetchall()
        for tbl in tbls:
            if tbl.table_name == 'sqlite_sequence':
                continue
            table = Table(self, tbl.table_name)
            ddl += table.export_ddl(dialect)
            if include_recs:
                ddl += table.export_records()

        return ddl
