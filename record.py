from database import Database
import re
from addict import Dict
import json
from datetime import datetime
import time

class Record:
    def __init__(self, db, tbl, pk):
        self.db = db
        self.tbl = tbl
        self.pk = pk

    def get(self):
        values = self.get_values()
        displays = self.get_display_values()

        new = True if not values else False

        fields = {}
        tbl_fields = self.tbl.get_fields()

        for key, field in tbl_fields.items():
            field.value = values.get(key, None)
            field.text = displays.get(key, None)
            # todo: editable
            if not 'editable' in field:
                field.editable = True
            field.alias = field.name

            fields[key] = field

        return Dict({
            'base_name': self.db.name,
            'table_name': self.tbl.name,
            'primary_key': self.pk,
            'fields': fields,
            'new': new
        })

    def get_relation_count(self, types: list = None):
        from table import Table
        relations = {}
        for key, rel in self.tbl.get_relations().items():
            db = Database(rel.base or rel.schema)
            tbl_rel = Table(db, rel.table)

            # todo: filtrate on highest level

            # Don't get values for new records that's not saved
            if hasattr(self, 'pk') and len(set(self.pk)):
                rec_values = self.get_values()

            # Add condition to fetch only rows that link to record
            for idx, col in enumerate(rel.foreign):
                ref_key = rel.local[idx].lower()
                val = None if len(self.pk) == 0 else rec_values[ref_key]
                tbl_rel.add_cond(f"{rel.table}.{col}", "=", val)

            if (len(self.pk)):
                count_records = tbl_rel.get_record_count()
            else:
                count_records = 0
            
            relation = Dict({
                'count_records': count_records,
                'name': rel.table,
                'conditions': tbl_rel.get_client_conditions(),
                'base_name': rel.base,
                'relationship': rel.type
            })
            
            parts = tbl_rel.name.split("_")
            suffix = parts[-1]
            if types and (len(types) and suffix in types):
                show_if = {'type_': suffix}
            else:
                show_if = None

            if show_if:
                relation.show_if = show_if

            relations[key] = relation

        return relations

    def get_relation(self, alias: str):
        from table import Table
        rel = self.tbl.get_relation(alias)
        db = Database(rel.base or rel.schema)
        tbl_rel = Table(db, rel.table)
        tbl_rel.limit = 500 # todo: burde ha paginering istedenfor
        
        # todo: filter

        # Don't get values for new records that's not saved
        if hasattr(self, 'pk') and len(set(self.pk)):
            rec_values = self.get_values()

        # Add condition to fetch only rows that link to record
        for idx, col in enumerate(rel.foreign):
            ref_key = rel.local[idx].lower()
            val = None if len(self.pk) == 0 else rec_values[ref_key]
            tbl_rel.add_cond(f"{rel.table}.{col}", "=", val)

        relation = tbl_rel.get_grid()

        # Don't get values for new records that's not saved
        if hasattr(self, 'pk') and len(set(self.pk)):
            rec_values = self.get_values()

        values = [None if len(self.pk) == 0 else rec_values[key]
                  for key in rel.local]
        
        for idx, col in enumerate(rel.foreign):
            relation.fields[col].default = values[idx]
            relation.fields[col].defines_relation = True

        pk = {}
        # Add condition to fetch only rows that link to record
        # todo: Hvorfor er dette nødvendig her og ikke for
        #       telling av relasjoner?
        for idx, col in enumerate(rel.foreign):
            ref_key = rel.local[idx]
            val = None if len(self.pk) == 0 else rec_values[ref_key]
            tbl_rel.add_cond(f"{rel.table}.{col}", "=", val)
            pk[col] = val

        tbl_rel.pkey = tbl_rel.get_primary_key()

        # If foreign key columns contains primary key
        if (set(tbl_rel.pkey) <= set(rel.foreign)):
            rec = Record(self.db, tbl_rel, pk)
            relation.records = [rec.get()]
            relation.relationship == "1:1"
        else:
            relation.relationship == "1:M"
            
        return relation

    def get_relations(self, count = False, alias: str = None, types: list = None):
        """
        Get all back references to record

        Params:
        - count: return just number of records
        - alias: return only relation with this alias
        - types: set condition for showing relation based on type
        """
        # todo: Altfor lang og rotete funksjon
        from table import Table

        # Don't get values for new records that's not saved
        if hasattr(self, 'pk') and len(set(self.pk)):
            rec_values = self.get_values()      
        
        relations = {}

        for key, rel in self.tbl.get_relations().items():
            if alias and alias != key: continue

            db = Database(rel.base)
            tbl_rel = Table(db, rel.table)

            # todo: too slow
            # permission = tbl_rel.get_user_permission(tbl_rel.name)
            # if not permission.view: continue

            tbl_rel.pkey = tbl_rel.get_primary_key()

            # If foreign key columns contains primary key
            if (set(tbl_rel.pkey) <= set(rel.foreign)):
                rel.type = '1:1'
            else:
                rel.type = '1:M'

            parts = tbl_rel.name.split("_")
            suffix = parts[-1]
            if types and (len(types) and suffix in types):
                show_if = {'type_': suffix}
            else:
                show_if = None

            pk = {}

            # Add condition to fetch only rows that link to record
            for idx, col in enumerate(rel.foreign):
                ref_key = rel.local[idx]

                val = None if len(self.pk) == 0 else rec_values[ref_key]
                tbl_rel.add_cond(f"{rel.table}.{col}", "=", val)

                pk[col] = val
            
            if rel.get('filter', None):
                tbl_rel.add_cond(rel.filter)

            if (count):
                # todo: Burde vel være unødvendig med egen kode for å telle. Skulle vel kunne kjøre spørringene og kun returnere antallet dersom count == True

                # Filter on highest level
                # todo: Altfor rotete kode
                # todo: Hvorfor filtrere på øvrste nivå kun ved count?
                if hasattr(tbl_rel, 'expansion_column') and tbl_rel.name != self.tbl.name:
                    fk = tbl_rel.get_parent_fk()
                    parent_col = tbl_rel.fields[fk.alias]
                    
                    tbl_rel.add_cond(tbl_rel.name+'.'+parent_col.alias, "=", parent_col.get('default'))

                if (len(self.pk)):
                    count_records = tbl_rel.get_record_count()
                else:
                    count_records = 0
                relation = Dict({
                    'count_records': count_records,
                    'name': rel.table,
                    'conditions': tbl_rel.get_client_conditions(),
                    'base_name': rel.base,
                    'relationship': rel.type
                })
                if show_if:
                    relation.show_if = show_if
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
                values = [None if len(self.pk) == 0 else  rec_values[key] for key in rel.local]

                for idx, col in enumerate(rel.foreign):
                    relation.fields[col].default = values[idx]
                    relation.fields[col].defines_relation = True

                if rel.type == "1:1":
                    rec = Record(self.db, tbl_rel, pk)
                    relation.records = [rec.get()]
                    relation.relationship == "1:1"
                else:
                    relation.relationship == "1:M"
                
            relations[key] = relation

        return relations

    def get_values(self):
        conds = [f"{key} = ?" for key in self.pk]
        cond = " and ".join(conds)
        params = [val for val in self.pk.values()]

        sql = f"""
        select * from {self.db.schema or self.db.cat}.{self.tbl.name}\n
        where {cond}
        """
        cursor = self.db.cnxn.cursor()
        row = cursor.execute(sql, params).fetchone()
        colnames = [col[0] for col in cursor.description]

        if not row:
            return Dict()

        return Dict(zip(colnames, row))

    def get_display_values(self):
        displays = {}

        join = self.tbl.get_join()

        for key, field in self.tbl.get_fields().items():
            if 'view' in field:
                displays[key] = f"({field.view}) as {key}"

        if len(displays) == 0:
            return Dict()
        
        select = ', '.join(displays.values())

        conds = [f"{self.tbl.name}.{key} = ?" for key in self.pk]
        cond = " and ".join(conds)
        params = [val for val in self.pk.values()]

        sql = "select " + select + "\n"
        sql += f"from {self.db.schema or self.db.cat}.{self.tbl.name}\n"
        sql += join + "\n"
        sql += " where " + cond

        cursor = self.db.cnxn.cursor()
        row = cursor.execute(sql, params).fetchone()
        colnames = [column[0] for column in cursor.description]

        if not row:
            return Dict()
    
        return Dict(zip(colnames, row))

    def get_children(self):
        rec = self.get()

        relations = self.tbl.get_relations()
        rel = [rel for rel in relations if rel.table == self.tbl.name][0]

        for idx, colname in enumerate(rel.foreign):
            foreign = rel.local[idx]
            value = rec.fields[foreign].value
            self.tbl.add_cond(f"{rel.table}.{colname}", "=", value)
        
        relation = self.tbl.get_grid()

        return relation['records']
    
    def insert(self, values):
        fields = self.tbl.get_fields()

        # todo: Get values for auto and auto_update fields

        # Get autoinc values for compound primary keys
        pkey = self.tbl.get_primary_key()
        inc_col = pkey[-1]
        if (
            inc_col not in values and
            len(pkey) > 1 and
            fields[inc_col].extra == "auto_increment"
        ):
            s = slice(0, len(pkey) - 1)
            cols = pkey[s]

            conditions = []
            params = []
            for col in cols:
                conditions.append(f"{col} = ?")
                params.append(values[col])
            
            sql = f"select case when max({inc_col}) is null then 1 else max({inc_col}) +1 end from {self.tbl.name} where " + " and ".join(conditions)

            print(sql)

            values[inc_col] = self.db.query(sql, params).fetchval()
            self.pk[inc_col] = values[inc_col]

        # Array of values to be inserted
        inserts = {}

        for key, value in values.items():

            if value == "":
                value = None

            if str(value).upper() in ['CURRENT_TIMESTAMP']:
                value = datetime.now()

            inserts[key] = value

        # todo: Vet ikke om jeg trenger å håndtere autoinc igjen

        sql = f"""
        insert into {self.tbl.name} ({','.join(inserts.keys())})
        values ({', '.join(["?" for key in inserts])})
        """

        result = self.db.query(sql, list(inserts.values())).commit()

        return self.pk

    def update(self, values):
        set_values = {}
        # todo: get values for auto update fields
        for field in self.tbl.get_fields().values():
            if field.get('extra', None) == "auto_update":
                 set_values[field.name] = self.db.expr.replace_vars(field.default)

        for key, value in values.items():
            if value == "":
                value = None
            
            set_values[key] = value
        
        sets = [f"{key} = ?" for key, val in set_values.items()]
        set_str = ",\n".join(sets)
        params = set_values.values()

        wheres = [f"{key} = ?" for key in self.pk]
        where_str = " and ".join(wheres)
        params = list(params) + list(self.pk.values())

        sql = f"""
        update {self.tbl.name}\n
        set {set_str}\n
        where {where_str}
        """

        result = self.db.query(sql, params).commit()

        # Update primary key
        for key, value in values.items():
            if key in self.pk:
                self.pk[key] = value

        return result

    def delete(self):
        """ Deletes a record.

        Deletion of subordinate records are handled by the database
        with ON DELETE CASCADE on the foreign key
        """

        wheres = [f"{key} = ?" for key in self.pk]
        where_str = " and ".join(wheres)

        sql = f"""
        delete from {self.tbl.name}
        where {where_str}
        """

        result = self.db.query(sql, list(self.pk.values())).commit()

        return result

    # todo: def get_file_path


