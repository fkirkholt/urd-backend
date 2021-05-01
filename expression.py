import re

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
                return "varchar(" + size + ")" if size else "longtext"
            elif type_ == "integer":
                return "int(" + size + ")"
            elif type_ == "float":
                return "float(" + size + ")"
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
            if type_ in ["char", "varchar2"]:
                return "string"
            elif type_ == "number":
                return "integer"
            elif type_ in ["date", "timestamp"]:
                return "date"
            else:
                raise ValueError(f"Type {type_} not supported yet")
        else:
            if type_ in ["varchar", "text", "char", "bpchar"]:
                return "string"
            elif type_ in ["integer", "int4"]:
                return "integer"
            elif type_ in ["numeric", "float8"]:
                return "float"
            elif type_ == "blob":
                return "binary"
            elif type_ in ["date", "timestamp"]:
                return "date"
            else:
                raise ValueError(f"Type {type_} not supported yet")
    
    def replace_vars(self, sql):
        return sql
        # todo:t Må ha autentisering på plass før denne kan lages
            
