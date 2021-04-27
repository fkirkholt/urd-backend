from table import Table
from database import Database
from schema import Schema
import re
from addict import Dict

class Record:
    def __init__(self, db, tbl_name, pk):
        self.db = db
        self.tbl = Table(self.db, tbl_name)
        self.pk = pk

    def get(self):
        # relations get parsed and values added to
        # $this -> tbl -> selects and $this -> tbl -> joins

        joins = self.tbl.get_joins()
        view  = self.tbl.get_view()

        # Get values for the table fields
        # -------------------------------
        # todo: Vurder å legge dette til egen funksjon
        
        join = "\n".join(joins)

        selects = [self.tbl.name + '.' + key for key in self.tbl.fields]
        select = ", ".join(selects)

        conditions = [self.tbl.name+'.'+key+" = '"+str(value)+"'" for key, value in self.pk.items()]
        cond = ' and '.join(conditions)

        sql = "select " + select
        sql+= "  from " + view + " " + self.tbl.name + "\n"
        sql+= join
        sql+= " where %s" % cond 

        cursor = self.db.cnxn.cursor()
        row = cursor.execute(sql).fetchone()

        # Build array over fields, with value and other properties
        # todo: Hvofor parameter self.tbl.name?
        # permission = self.tbl.get_user_permission(self.tbl.name)
        permission = Dict({"edit": True}) # todo: bruk funksjon over

        fields = {}
        for key, field in self.tbl.fields.items():
            # todo: Denne genererer feil for view-kolonner
            field.value = getattr(row, key)
            field.alias = key
            if 'editable' not in field:
                field.editable = permission.edit
            # todo: Trenger jeg å sette field['datatype'] til None?
            if key in self.tbl.foreign_keys:
                fk = self.tbl.foreign_keys[key]
                ref_schema = Schema(fk.schema)
                ref_tbl = ref_schema.tables[fk.table]
                if (ref_tbl.type == 'data' and 'expandable' not in field) or 'view' not in field:
                    field.expandable = True
                field.foreign_key = fk
            fields[key] = field
        
        # Get display value of fk columns
        # ---------------------------------------
        # todo: Vurder å legge dette til egen funksjon

        displays = {}

        for key, field in self.tbl.fields.items():
            if 'view' in field:
                displays[key] = "(%s) as %s" % (field.view, key)

        if len(displays) > 0:
            select = ', '.join(displays.values())
            print(select)

            sql = "select " + select + "\n"
            sql+= "  from " + view + " " + self.tbl.name + "\n"
            sql+= join + "\n"
            sql+= " where %s" % cond

            row = cursor.execute(sql).fetchone()

            colnames = [column[0] for column in cursor.description]
            row = dict(zip(colnames, row))
            for key, value in row.items():
                field = fields[key]
                field.text = value

                # todo: Is this necessary
                if 'foreign_key' not in field: continue

                # Don't load options if there's a reference to current table in filter
                searchable = False
                if 'filter' in field.foreign_key:
                    pat = r"\b" + self.tbl.name + r"\."
                    if re.search(pat, field.foreign_key.filter):
                        searchable = True

                if searchable: continue

                if 'view' in field:
                    if 'column_view' not in field:
                        field.column_view = field.view
                    field.options = self.tbl.get_options(field, fields)
                
                permission = self.tbl.get_user_permission(self.tbl.name)
                if permission.view == False:
                    field.expandable = False

                fields[key] = field

        # Don't let $fields be reference to self.tbl['fields']
        # todo: json encode og decode

        for key, field in fields.items():
            field['name'] = key
            # del field['alias']
            fields[key] = field

        return Dict({
            'base_name': self.db.name,
            'table_name': self.tbl.name,
            'primary_key': self.pk,
            'fields': fields
        })

    def get_relations(self, count, rel_alias):
        # todo: Dokumenter parametre
        # todo: Altfor lang og rotete funksjon
        # Don't try to get record for new records that's not saved
        print(set(self.pk))
        if hasattr(self, 'pk') and len(set(self.pk)) > 0:
            rec = self.get()
        else: return []
        
        relations = {}

        if not hasattr(self.tbl, 'relations'): return []

        for key, rel in self.tbl.relations.items():
            if rel_alias and rel_alias != key: continue

            if 'schema' not in rel:
                rel.schema = self.db.schema
            
            if rel.schema != self.db.schema:
                rel.db_name = Schema(rel.schema).get_db_name()
            else:
                rel.db_name = self.db.name

            if 'table' not in rel: rel.table = key

            db = Database(rel.db_name)
            tbl_rel = Table(db, rel.table)

            permission = tbl_rel.get_user_permission(tbl_rel.name)

            if not permission.view: continue

            fk = rel.foreign_key
            # todo: Trenger disse å være attributter til rel?
            #       Kan de ikke være vanlige variabler isteden?
            rel.fk_columns = tbl_rel.foreign_keys[fk].local
            rel.ref_columns = tbl_rel.foreign_keys[fk].foreign

            # Add condition to fetch only rows that link to record
            for idx, col in enumerate(rel.fk_columns):
                ref_key = rel.ref_columns[idx]

                val = rec.fields[ref_key].value if len(self.pk) else None
                tbl_rel.add_condition("%s.%s = '%s'" % (rel.table, col, val))
            
            if rel.get('filter', None):
                tbl_rel.add_condition(rel.filter)
            
            if (count):
                # todo: Burde vel være unødvendig med egen kode for å telle. Skulle vel kunne kjøre spørringene og kun returnere antallet dersom count == True

                # Filter on highest level
                # todo: Altfor rotete kode
                # todo: Hvorfor filtrere på øvrste nivå kun ved count?
                if hasattr(tbl_rel, 'expansion_column') and tbl_rel.name != self.tbl.name:
                    fk = tbl_rel.get_parent_fk()
                    parent_col = tbl_rel.fields[fk.alias]
                    tbl_rel.add_condition(tbl_rel.name+'.'+parent_col.alias + (" = " + parent_col.default if 'default' in parent_col else " IS NULL"))

                conditions = tbl_rel.get_conditions()
                condition = "where " + (" and ".join(conditions)) if len(conditions) else ""
                count_records = tbl_rel.get_record_count(condition)
                relation = Dict({
                    'count_records': count_records,
                    'name': rel.table,
                    'conditions': conditions,
                    'base_name': rel.db_name
                })     
            else:
                # todo: Are these necessary?
                tbl_rel.limit = 500
                tbl_rel.offset = 0
                tbl_rel.csv = False

                # Filter the list on highest level when necessary
                if self.tbl.name != tbl_rel.name:
                    tbl_rel.user_filtered = False
                
                relation = tbl_rel.get_grid()

                # Find condition for relation
                # todo: Har håndtert at pk ikke er satt i php-koden
                values = [rec.fields[key] for key in rel.ref_columns]

                for idx, col in enumerate(rel.fk_columns):
                    relation.fields[col].default = values[idx]
                    relation.fields[col].defines_relation = True
                
            relations[key] = relation

        return relations


