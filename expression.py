import re
from datetime import date

class Expression:
    def __init__(self, platform):
        self.platform = platform

    def concat(self, items):
        # Concat expressions and treat null as ''
        if (self.platform == 'mysql'):
            return "concat_ws('', " + ','.join(items) + ")"
        else:
            return ' || '.join(items)

    def concat_ws(self, sep, items):
        if (self.platform == 'mysql'):
            return "concat_ws('" + sep + "'," + ",".join(items) + ")"
        else:
            sep = " || '" + sep + "' || "
            return sep.join(items)

    def autoincrement(self):
        if (self.platform == 'mysql'):
            return "AUTO_INCREMENT"
        elif (self.platform == 'oracle'):
            return "GENERATED BY DEFAULT ON NULL AS IDENTITY"
        elif (self.platform == 'postgres'):
            return "SERIAL"
        elif (self.platform == 'sqlite'):
            return "AUTOINCREMENT"

    def to_native_type(self, type_, size):
        if self.platform == "mysql":
            if type_ == "string":
                return "varchar(" + str(size) + ")" if size else "longtext"
            elif type_ == "integer":
                return "int(" + str(size) + ")"
            elif type_ == "float":
                return "float(" + str(size) + ")"
            elif type_ == "date":
                return "date"
            elif type_ == "boolean":
                return "tinyint(1)"
            elif type_ == "binary":
                return "blob"
            else:
                raise ValueError(f"Type {type_} not supported yet")
        elif self.platform == "sqlite":
            if type_ in ["string", "date"]:
                return "text"
            elif type_ in ["integer", "boolean"]:
                return "integer"
            elif type_ == "float":
                return "real"
            elif type_ == "binary":
                return "blob"
        elif self.platform == 'postgres':
            if type_ == "string":
                return "varchar(" + str(size) + ")"
            elif type_ == "integer":
                return "integer"
            elif type_ == "float":
                return "float(" + str(size) + ")"
            elif type_ == "date":
                return "date"
            elif type_ == "boolean":
                return "boolean"
            elif type_ == "binary":
                return "bytea"
        else:
            raise ValueError(f"Type conversion for {self.platform} not implemented")

    def to_urd_type(self, type_):
        type_ = type_.lower()
        if self.platform == 'mysql':
            if re.search("char|text", type_):
                return "string"
            elif re.search("int", type_):
                return "integer"
            elif re.search("float|double|decimal", type_):
                return "float"
            elif re.search("date|time", type_):
                return "date"
            elif type_ == "blob":
                return "binary"
            else:
                raise ValueError(f"Type {type_} not supported yet")
        elif self.platform == "oracle":
            if type_ in ["char", "varchar2", "nvarchar2", "clob", "nclob"]:
                return "string"
            elif type_ == "number":
                return "integer"
            elif type_ in ["date", "timestamp", "timestamp(6)"]:
                return "date"
            elif type_ in ["decimal", "float"]:
                return "float"
            elif type_ in ["blob"]:
                return "binary"
            else:
                raise ValueError(f"Type {type_} not supported yet")
        else:
            if type_ in ["varchar", "text", "char", "bpchar"]:
                return "string"
            elif type_ in ["integer", "int4", "int8"]:
                return "integer"
            elif type_ in ["numeric", "float8"]:
                return "float"
            elif type_ == "blob":
                return "binary"
            elif type_ in ["date", "timestamp"]:
                return "date"
            elif type_ in ["bool"]:
                return "boolean"
            else:
                raise ValueError(f"Type {type_} not supported yet")
    
    def replace_vars(self, sql):
        # todo: Get user from logged in user
        sql = sql.replace("$user_name", "Admin")

        if "current_date" in sql.lower():
            sql = date.today().strftime("%Y-%m-%d")

        return sql
        # todo:t Må ha autentisering på plass før denne kan lages

    def databases(self):
        if self.platform == 'postgres':
            return """
            select datname from pg_database
            where datistemplate is false and datname != 'postgres'
            """
        elif self.platform == 'oracle':
            return """
            select distinct owner
            from table_privileges
            where grantee = ?
            order by owner;
            """

    def indexes(self):
        if self.platform == 'oracle':
            return """
            select i.index_name, case uniqueness when 'NONUNIQUE' then 1 else 0 end as non_unique,
                   lower(column_name) as column_name, column_position, i.table_name
            from all_indexes i
            join all_ind_columns col on col.index_name = i.index_name
            where i.table_owner = ?
            order by column_position
            """
    def pkeys(self):
        if self.platform == 'oracle':
            return """
            SELECT cols.table_name, cols.column_name, cols.position as key_seq, cons.status, cons.owner
            FROM all_constraints cons, all_cons_columns cols
            WHERE cons.constraint_type = 'P'
            AND cons.constraint_name = cols.constraint_name
            AND cons.owner = cols.owner
            AND cons.owner = ?
            AND cols.table_name = nvl(?, cols.table_name)
            ORDER BY cols.table_name, cols.position;
            """
    def fkeys(self):
        if self.platform == 'oracle':
            return """
            SELECT a.column_name as fkcolumn_name, a.position,
                   a.constraint_name as fk_name, a.table_name as fktable_name,
                    c.owner, c.delete_rule,
                    -- referenced pk
                    c.r_owner as pktable_cat, c_pk.table_name as pktable_name,
                    c_pk.constraint_name r_pk,
                    ra.column_name pkcolumn_name
            FROM all_cons_columns a
                JOIN all_constraints c
                ON a.owner = c.owner
                AND a.constraint_name = c.constraint_name
                JOIN all_constraints c_pk
                ON c.r_owner = c_pk.owner
                AND c.r_constraint_name = c_pk.constraint_name
                JOIN all_cons_columns ra
                ON ra.owner = c.owner
                AND ra.constraint_name = c_pk.constraint_name
                AND ra.position = a.position
            WHERE c.constraint_type = 'R'
            AND   a.owner = ?
            ORDER BY a.position
            """
    def columns(self):
        if self.platform == 'oracle':
            return """
            select lower(table_name) as table_name, lower(column_name) as column_name,
                   data_type as type_name, data_length as column_size,
                   case nullable when 'Y' then 1 else 0 end as nullable
            from all_tab_columns
            where owner = ? and table_name = nvl(?, table_name)
            """
