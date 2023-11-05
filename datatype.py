class Datatype:

    def __init__(self, python_type, size=None):
        self.type = python_type
        self.size = size

    def get_mysql_type(self):
        if self.type == "str":
            return f"varchar({str(self.size)})" if self.size else "longtext"
        elif self.type == "int":
            return "int(" + str(self.size) + ")"
        elif self.type == "Decimal":
            return "decimal(" + str(self.size) + ") "
        elif self.type == "float":
            if self.size:
                precision = self.size.split(',')[0]
                return "float(" + precision + ")"
            else:
                return "float"
        elif self.type in ["date", "datetime", "time"]:
            return self.type
        elif self.type == "bool":
            return "tinyint(1)"
        elif self.type == "bytes":
            return "blob"
        else:
            raise ValueError(f"Type {self.type} not supported yet")

    def get_sqlserver_type(self):
        if self.type == 'str':
            return ('varchar(' + str(self.size) + ')'
                    if (self.size and self.size > 0) else 'varchar(max)')
        elif self.type == 'int':
            return 'int'
        elif self.type == 'Decimal':
            return 'decimal(' + str(self.size) + ')'
        elif self.type == 'float':
            return 'float(' + str(self.size) + ')'
        elif self.type in ['date', 'datetime', 'time']:
            return self.type
        elif self.type == 'bool':
            return 'bit'
        elif self.type == 'bytes':
            return 'varbinary(max)'
        else:
            raise ValueError(f"Type {self.type} not supported yet")

    def get_sqlite_type(self):
        if self.type in ["str"]:
            return "varchar(" + str(self.size) + ")" if self.size else "text"
        elif self.type in ["date", "datetime", "time"]:
            return self.type
        elif self.type in ["int", "bool"]:
            return "integer"
        elif self.type == "Decimal":
            return "decimal"
        elif self.type == "float":
            return "real"
        elif self.type == "bytes":
            return "blob"
        elif self.type == "json":
            return "json"
        else:
            raise ValueError(f"Type {self.type} not supported yet")

    def get_postgres_type(self):
        if self.type == "str" and self.size:
            return "varchar(" + str(self.size) + ")"
        elif self.type == "str":
            return "text"
        elif (self.type == "int" and self.size and self.size > 11):
            return "bigint"
        elif self.type == "int":
            return "integer"
        elif self.type == "Decimal":
            return "decimal(" + str(self.size) + ")"
        elif self.type == "float":
            if self.size:
                precision = self.size.split(',')[0]
                return "float(" + precision + ")"
            else:
                return "float"
        elif self.type == "date":
            return "date"
        elif self.type == "datetime":
            return "timestamp"
        elif self.type == "time":
            return "time"
        elif self.type == "bool":
            return "boolean"
        elif self.type == "bytes":
            return "bytea"
        elif self.type == "json":
            return "json"

    def get_oracle_type(self):
        if (self.type == "str" and (not self.size or self.size > 4000)):
            return "clob"
        elif self.type == "str":
            return "varchar(" + str(self.size) + ")"
        elif (self.type == "int" and self.size > 11):
            return "number(" + str(self.size) + ", 0)"
        elif self.type == "int":
            return "integer"
        elif self.type == "float":
            if ',' in self.size:
                return f"number({self.size})"
            elif self.size:
                return "float(" + str(self.size) + ")"
            else:
                return "float"
        elif self.type == "date":
            return "date"
        elif self.type in ["datetime", "time"]:
            return "timestamp"
        elif self.type == "bool":
            return "number(1)"
        elif self.type == "bytes":
            return "blob"
        elif self.type == "Decimal":
            return f"number({self.size})"

    def to_native_type(self, platform):
        if platform in ['mysql', 'mariadb']:
            return self.get_mysql_type(self.type, self.size)
        elif platform == 'mssql':
            return self.get_sqlserver_type(self.type, self.size)
        elif platform == "sqlite":
            return self.get_sqlite_type(self.type, self.size)
        elif platform == 'postgresql':
            return self.get_postgres_type(self.type, self.size)
        elif platform == 'oracle':
            return self.get_oracle_type(self.type, self.size)
        else:
            raise ValueError(f"Type conversion for {platform} not "
                             "implemented")
