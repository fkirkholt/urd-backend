import json
from config import config
import pyodbc
from addict import Dict
import re
import os

class Schema:
    def __init__(self, name):
        with open('schemas/' + name + '/schema.json') as myfile:
            data = myfile.read()

        schema = Dict(json.loads(data))

        self.name = name
        self.tables = schema["tables"]
        self.reports = schema.get("reports", [])
        self.contents = schema.get("contents", [])
        self.config = schema.get("config", {})

    def get_db_name(self):
        sql = """
        select name
        from datatabase_
        where schema_ = ?
        """
        cnxnstr = config['db']['connection_string']
        urd_cnxn = pyodbc.connect(cnxnstr)
        cursor = urd_cnxn.cursor()

        base = cursor.execute(sql, self.name).fetchval()

        return base

    def update(self, db, config):
        config = Dict(config)
        self.config = config

        threshold = int(config.threshold) / 100

        if config.replace:
            self.tables = Dict()
            self.reports = Dict()
            self.contents = Dict()

        # todo: progress

        report   = Dict()
        drops    = Dict()
        modules  = []
        warnings = []

        cursor = db.cnxn.cursor()
        tables = [row.table_name for row in cursor.tables()]

        # Build dict of table keys and remove tables that doesn't exist
        tbl_keys = Dict()
        for key, table in self.tables.items():
            tbl_keys[table.name] = key
            if table.name not in tables:
                del self.tables[key]

        terms = Dict()
        if 'meta_terminology' in tables:
            sql = "select * from meta_terminology"
            cursor.execute(sql)
            colnames = [column[0] for column in cursor.description]
            for row in cursor:
                terms[row.term] = Dict(zip(colnames, row))
        self.terms = terms

        # Used for progress indication
        total = len(tables)
        processed = -1

        for tbl_name in tables:

            report[tbl_name] = Dict({
                'empty_columns': [],
                'almost_empty_columns': []
            })

            # todo: track progress

            if tbl_name in tbl_keys:
                tbl_key = tbl_keys[tbl_name]
            else:
                tbl_key = tbl_name.lower() 
            
            pk = [row.column_name.lower() for row in cursor.primaryKeys(tbl_name)]

            if len(pk) == 0:
                warnings.append("Tabell {tbl_name} mangler primærnøkkel")
            
            term = None if tbl_name not in terms else terms[tbl_name]
            if tbl_key not in self.tables:
                table = Dict({
                    'name': tbl_name.lower(),
                    'icon': None,
                    'label': None if not term else term.label,
                    'primary_key': pk,
                    'description': None if not term else term.description,
                    'relations': {},
                    'hidden': False
                })
            else:
                table = self.tables[tbl_key]

                if 'hidden' not in table:
                    table.hidden = False

                if len(pk):
                    table.primary_key = pk
            
            if term:
                table.label = term.label
                table.description = term.description
            
            # Hides table if user has marked the table to be hidden
            if 'hidden' in config.dirty[table.name]:
                table.hidden = config.dirty[table.name].hidden
            
            # Updates indexes
            table.indexes = {}

            index_names = []

            for row in cursor.statistics(tbl_name):
                name = row.index_name

                if name not in table.indexes:
                    table.indexes[name] = Dict({
                        'name': name,
                        'unique': not row.non_unique,
                        'columns': []
                    })
                if name not in index_names:
                    index_names.append(name)
                
                table.indexes[name].columns.append(row.column_name)
            
            grid_idx = table.indexes.get(tbl_name + '_grid_idx', None)

            sort_idx = tbl_name + '_sort_idx'
            if sort_idx in table.indexes:
                sort_cols = table.indexes[sort_idx].columns
            elif grid_idx:
                sort_cols = grid_idx.columns[0:3]
            else:
                sort_cols = []
            
            sum_idx = tbl_name + '_summation_idx'
            if sum_idx in table.indexes:
                sum_cols = table.indexes[sum_idx].columns
            else:
                sum_cols = []
            
            # Remove dropped indexes
            for key, index in table.indexes.items():
                if index.name not in index_names:
                    del table.indexes[key]

            # Update foreign keys

            table.foreign_keys = {}
            keys = {}

            for row in cursor.foreignKeys(foreignTable=tbl_name):

                name = row.fk_name.lower()
                if name not in keys:
                    keys[name] = Dict({
                        'name': name,
                        'table': row.pktable_name.lower(),
                        # todo: Dette er "databasen"
                        'schema': row.pktable_cat.lower(),
                        'local': [],
                        'foreign': []
                    })
                keys[name].local.append(row.fkcolumn_name.lower())
                keys[name].foreign.append(row.pkcolumn_name.lower())

            for fk in keys.values():
                alias = fk.local[-1]
                if alias in table.foreign_keys:
                    alias = alias + '_2'
                fk.name = f"{table.name}_{alias}_fkey"
                table.foreign_keys[alias] = fk

                # todo: Sjekk om referensetabell eksisterer

                # Add to relations of relation table
                fk_table_alias = fk.table if fk.table not in tbl_keys else tbl_keys[fk.table]

                if fk.table in tables and fk_table_alias not in self.tables:
                    self.tables[fk_table_alias] = Dict({
                        'name': fk.table,
                        'relations': []
                    })
                
                # Check if relation defines this as an extension table
                if fk.local == pk:
                    table.extends = fk.table
                
                # Find index associated with the foreign key
                fk_index = None
                for index in table.indexes.values():
                    if index.columns == fk.local:
                        fk_index = index
                
                # Find if there exists an index to find local key
                index_exists = False
                s = slice(0, len(fk.local))
                for index in table.indexes.values():
                    if len(index.columns) >= len(fk.local) and index.columns[s] == fk.local:
                        index_exists = True
                
                # Find label for has-many relations
                if config.urd_structure and fk_index:
                    lbl = re.sub(r"^(fk_|idx_)", "", fk_index.name)
                    lbl = re.sub(f"({fk.table})"+r"(_fk|_idx)?$", "", lbl)
                    local = '_'.join(fk.local)
                    lbl = re.sub("^"+local+"$", "", lbl)
                    replace = "" if fk.table == local else f" ({local})"
                    lbl = re.sub("(_"+local+r")(_fk|_idx)?$", replace, lbl)
                    lbl = re.sub(r"(_fk|_idx)$", "", lbl)
                    lbl = lbl.replace("_", " ")
                else:
                    lbl = tbl_key.replace("_", " ")

                # Avoid "Primary" as label
                if 'extends' in table:
                    lbl = table.name.replace(f"{table.extends}_", "")
                
                if lbl == "":
                    lbl = tbl_key
                
                if config.norwegian_chars:
                    lbl = lbl.replace("ae", "æ")
                    lbl = lbl.replace("oe", "ø")
                    lbl = lbl.replace("aa", "å")
                
                lbl = lbl.capitalize()

                if fk.name in self.tables[fk_table_alias].relations:
                    rel = self.tables[fk_table_alias].relations[fk.name]
                else:
                    rel = Dict()
                
                if fk.table in tables:
                    self.tables[fk_table_alias].relations[fk.name] = Dict({
                        'table': tbl_name,
                        'foreign_key': alias,
                        'label': lbl,
                        'filter': None if filter not in rel else rel.filter,
                        'hidden': True if (not index_exists and config.urd_structure) or table.get('hidden', False) or rel.get('hidden', False) else False
                    })

            # Count table rows
            count_rows = db.query(f"select * from {tbl_name}").rowcount
            report[tbl_name].rows = count_rows
            if config.count_rows:
                table.count_rows = count_rows
            elif 'count_rows' in table:
                del table.count_rows

            if 'fields' not in table:
                table.fields = Dict()

            # todo: Delete columns that doesn't exist anymore

            for col in cursor.columns(table=table.name):
                cname = col.column_name.lower()
                tbl_col = tbl_name + '.' + cname
                
                type_ = db.expr.to_urd_type(col.type_name)
                # todo: check boolean

                drop_me = False
                ratio_comment = ""
                hidden = False

                # todo: Find if column is (largely) empty

                # todo: Find distinct values for some column types

                # todo: if drop_me

                # Find alias of existing column in schema
                if cname in table.fields:
                    alias = cname
                else:
                    for key, field in table.fields.items():
                        if field.name == cname:
                            alias = key
                            break
                
                # Desides what sort of input should be used
                if not config.urd_structure and 'element' in table.fields[alias]:
                    element = table.fields[alias].element
                elif type_ == 'date':
                    element = 'input[type=date]'
                elif type_ == 'boolean':
                    if col.nullable:
                        element = 'select'
                        options = [
                            {
                                'value': 0,
                                'label': 'Nei'
                            },
                            {
                                'value': 1,
                                'label': 'Ja'
                            }
                        ]
                    else:
                        element = 'input[type=checkbox]'
                elif cname in table.foreign_keys:
                    element = 'select'
                    options = []
                elif type_ == 'binary' or (type_ == 'string' and (col.display_size > 255)):
                    element = "textarea"
                else:
                    element = "input[type=text]"
                
                label = cname if cname not in terms else terms[cname].label

                if config.norwegian_chars:
                    label = label.replace("ae", "æ")
                    label = label.replace("oe", "ø")
                    label = label.replace("aa", "å")
                
                label = label.capitalize()

                urd_col = Dict({
                    'name': cname,
                    'element': element,
                    'datatype': type_,
                    'nullable': col.nullable == True,
                    'label': label,
                    'description': None if not cname in terms else terms[cname].description
                })

                if type_ not in ["boolean", "date"]:
                    urd_col.size = col.display_size
                if col.auto_increment:
                    urd_col.extra = "auto_increment"
                if element == "select" and len(options):
                    urd_col.options = options
                
                if col.column_def and not col.auto_increment:
                    def_vals = col.column_def.split('::')
                    default = def_vals[0]
                    default = default.replace("'", "")

                    # todo: Sjekk om jeg trenger å endre current_timestamp()

                    urd_col.default = default
                
                if hidden:
                    urd_col.hidden = True
                
                if not alias:
                    table.fields[cname] = urd_col
                else:
                    table.fields[alias] = table.fields[alias] | urd_col
            
            # Try to decide if the table is a reference table
            if config.urd_structure:
                index_cols = []
                for index in table.indexes.values():
                    if index.unique:
                        index_cols = index_cols + index.columns
                
                if len(set(index_cols)) == len(table.fields):
                    table.type = 'reference'
                elif tbl_name[0:4] == "ref_" or tbl_name[:-4] == "_ref" or tbl_name[0:5] == "meta_":
                    table.type = "reference"
                else:
                    table.type = "data"
            else:
                # use number of visible fields to decide if table is reference table
                count = len([key for key, field in table.fields if not field.get('hidden', False)])

                if 'type' in table and count < 4 and not len(table.foreign_keys):
                    table.type = "reference"
                else:
                    if 'type' in config.dirty[table.name]:
                        table.type = config.dirty[table.name].type
                    else:
                        table.type = "data"
            
            # Decide which columns should be shown in grid
            if grid_idx:
                table.grid = Dict({
                    'columns': grid_idx.columns,
                    'sort_columns': sort_cols,
                    'summation_columns': sum_cols
                })
            else:
                cols = []
                for key, field in table.fields.items():
                    if field.name[0:1] == "_": continue
                    if field.get('hidden', False): continue
                    if 'extra' in field and field.extra == "auto_increment" and table.type != "reference":
                        continue
                    else:
                        cols.append(key)

                table.grid = Dict({
                    'columns': cols[0:5],
                    'sort_columns': sort_cols,
                    'summation_columns': sum_cols
                })
            
            # Make action for displaying files
            filepath_idx = tbl_name + '_file_path_idx'
            if filepath_idx in table.indexes:
                last_col = table.indexes[filepath_idx].columns

                action = Dict({
                    'label': "Vis fil", # todo: tillat engelsk
                    'url': "/file",
                    'icon': "external-link",
                    'communication': "download",
                    'disabled': f"({last_col} is null"
                })

                table.actions = Dict({
                    'vis_fil': action # todo: tillat engelsk
                })

                table.grid.columns.append("actions.vis_fil")

            # Make form

            form = Dict({'items': {}}) # todo: vurder 'subitems'
            col_groups = Dict()

            # group fields according to first part of field name
            for field in table.fields.values():
                # Don't add to form pk column not shown in grid
                if field.name in pk and ('grid' in table and field.name not in table.grid.columns) or 'grid' not in table: continue

                # Group by prefix
                group = field.name.split('_')[0]

                # Don't add fields that start with _
                # They are treated as hidden fields
                if group == "": field.hidden = True
                if field.get('hidden', False): continue

                if group not in col_groups: col_groups[group] = []
                col_groups[group].append(field.name)
            
            for group_name, col_names in col_groups.items():
                if len(col_names) == 1:
                    label = col_names[0].replace("_", " ").capitalize()
                    if config.norwegian_chars:
                        label = label.replace("ae", "æ")
                        label = label.replace("oe", "ø")
                        label = label.replace("aa", "å")
                    form['items'][label] = col_names[0]
                else:
                    inline = False
                    colnames = Dict() # todo: tullete med colnames og col_names
                    for idx, colname in enumerate(col_names):
                        # removes group name prefix from column name and use the rest as label
                        rest = colname.replace(group_name+"_", "")
                        if rest in terms: 
                            label = terms[rest].label
                        else: 
                            label = rest.replace("_", " ").capitalize()

                        colnames[label] = colname

                        if 'separator' in table.fields[colname]:
                            inline = True
                    
                    if group_name in terms:
                        group_label = terms[group_name].label
                    else:
                        group_label = group_name.capitalize()
                    form['items'][group_label] = Dict({
                        'inline': inline,
                        'items': colnames # todo vurder 'subitems'
                    })
            
            table.form = form
            self.tables[tbl_key] = table

            # Update records for reference table
            if not config.add_ref_records:
                if 'records' in table:
                    del table.records
                continue
            if tbl_key not in db.tables: continue
            if table.type != "reference": continue
            sql = "select * from " + table.name
            # todo: Forsikre meg om lowercase
            table.records = db.query(sql).fetchall() # todo: db.fetchall()

            # Updates table definition with records
            self.tables[tbl_key] = table

        # Add form data from associated tables
        for tbl_key, table in self.tables.items():         

            # Add relations to form
            if 'relations' in table:
                relations_to_delete = []
                for alias, relation in table.relations.items():
                    if relation.table not in self.tables:
                        relations_to_delete.append(alias)
                        continue

                    rel_table = self.tables[relation.table]

                    if rel_table.get('hidden', False):
                        table.relations[alias].hidden = True
                        relation.hidden = True
                    
                    # Remove relations to foreign keys that doesn't exist
                    if alias not in [key.name for key in rel_table.foreign_keys.values()]:
                        relations_to_delete.append(alias)
                        continue
                        
                    fk = rel_table.foreign_keys[relation.foreign_key]

                    # Find indexes that can be used to get relation
                    # todo: Har jeg ikke gjort liknende lenger opp?
                    # Se "Find if there exists an index to find local key"
                    index_exist = False
                    s = slice(0, len(fk.local))
                    for index in rel_table.indexes.values():
                        if index.columns[s] == fk.local:
                            index_exist = True
                    
                    if index_exist and not relation.get('hidden', False):
                        if 'label' in relation:
                            label = relation.label.capitalize()
                        else:
                            label = alias.replace("_", " ").capitalize()
                        table.form['items'][label] = "relations." + alias
                    
                    rf_name = fk.local[-1]
                    ref_field = self.tables[relation.table].fields[rf_name]
                    ref_tbl_col = relation.table + "." + rf_name

                    # Don't show relations coming from hidden fields
                    if not config.urd_structure and ref_field.hidden:
                        relation.hidden = True
                        if label: del table.form['items'][label]
                    
                    # Don't show fields referring to hidden table
                    if table.hidden and rf_name not in rel_table.primary_key:
                        ref_field.hidden = True
                    elif 'hidden' in config.dirty[table.name] and ref_tbl_col not in drops:
                        # show columns where fk table is shown again
                        # and where the column is not hidden for other reasons
                        ref_field.hidden = False

                    self.tables[relation.table].fields[rf_name] = ref_field

                    table.relations[alias] = relation

                for rel in relations_to_delete:
                    del table.relations[rel]

            # todo: Add drop table statement if hidden or less than 2 rows

            # todo: Add delete statements for unreferenced records in reference tables

            # Find how tables are grouped in modules

            top_level = True

            # Reference tables should not be used to group tables in modules
            if table.type == "reference":
                top_level = False
            
            for key, fk in table.foreign_keys.items():
                if fk.table not in self.tables: continue

                if fk.table != table.name and table.fields[key].hidden:
                    fk_table = self.tables[fk.table]
                    if fk_table.type != "reference":
                        top_level = False
                
                if key not in table.fields: continue

                field = table.fields[key]
                if fk.schema != self.name:
                    ref_schema = Schema(fk.schema)
                    ref_tbl = ref_schema.tables[fk.table]
                else:
                    ref_tbl = self.tables[fk.table]
                
                for index in ref_tbl.indexes.values():
                    if not index.primary and index.unique:
                        cols = [key+"."+col for col in index.columns]
                        field.view = " || ".join(cols)
                        break
            
            if top_level:
                rel_tables = self.get_relation_tables(tbl_key, [])
                rel_tables.append(table.name)

                module_id = None
                for idx, module in enumerate(modules):
                    common = [val for val in rel_tables if val in module]
                    if len(common):
                        if module_id == None:
                            modules[idx] = list(set(module + rel_tables))
                            module_id = idx
                        else:
                            modules[module_id] = list(set(module + modules[module_id]))
                            del modules[idx]
                
                if module_id == None:
                    modules.append(rel_tables)

            self.tables[tbl_key] = table

        # Check if reference table is attached to only one mudule
        # and group tables
        tbl_groups = Dict()
        sub_tables = Dict()
        for tbl_key, table in self.tables.items():
            # Add table to table group
            if config.urd_structure:
                group = tbl_key.split("_")[0]

                # Check if this is a crossreference table
                # todo: Brukes dette til noe?
                last_pk_col = table.primary_key[-1]
                if last_pk_col in table.foreign_keys and 'extends' not in table:
                    table.type = "xref"
                
                # Find if the table is subordinate to other tables
                # i.e. the primary key also has a foreign key
                subordinate = False
                if not table.primary_key: subordinate = True

                for colname in table.primary_key:
                    if colname in table.foreign_keys:
                        subordinate = True
                        key = table.foreign_keys[colname]

                        if table.type == "xref": 
                            break

                        if key.table not in sub_tables:
                            sub_tables[key.table] = []
                        
                        sub_tables[key.table].append(tbl_key)
                        break
            else:
                group = tbl_key
                subordinate = False

            # Only add tables that are not subordinate to other tables
            if not subordinate:
                # Remove group prefix from label
                rest = tbl_key.replace(group+"_", "")
                if rest in terms:
                    label = terms[rest].label
                else:
                    label = rest.replace("_", " ").capitalize()
                
                #// if group not in tbl_groups: tbl_groups[group] = []
                tbl_groups[group][label] = tbl_key

            if table.type != "reference": continue
            in_modules = []

            for rel in table.relations.values():
                if rel.get('hidden', False): continue

                for idx, module in enumerate(modules):
                    if rel.table in module:
                        in_modules.append(idx)

            in_modules = list(set(in_modules))

            if len(in_modules) == 1:
                mod = in_modules[0]
                modules[mod].append(tbl_key)
                modules[mod] = list(set(modules[mod]))

        main_module = max(modules, key=len) # Find module with most tables

        # todo: Generate drop statements for tables not connected to other tables

        # todo sort drops

        # Makes contents

        contents = Dict()

        # Sort modules so that modules with most tables are listed first
        modules.sort(key=len, reverse=True)

        for group_name, table_names in tbl_groups.items():
            if len(table_names) == 1 and group_name != "meta":
                table_alias = list(table_names.values())[0]
                if table_alias in terms:
                    label = terms[table_alias].label
                else:
                    label = table_alias.replace("_", " ").capitalize()
                
                if config.norwegian_chars:
                    label = label.replace("ae", "æ")
                    label = label.replace("oe", "ø")
                    label = label.replace("aa", "å")
                
                # Loop through modules to find which one the table belongs to
                placed = False

                if config.urd_structure:
                    contents = self.get_content_items(table_alias, sub_tables, contents)
                    continue

                for idx, module in enumerate(modules):
                    if len(module) > 2 and table_alias in module:
                        mod = "Modul " + str(idx + 1)
                        contents[mod].class_label = "b"
                        contents[mod].class_content = "ml3"
                        contents[mod].subitems[label] = "tables." + table_alias
                        if 'count' not in contents[mod]: 
                            contents[mod].count = 0
                        contents[mod].count += 1
                        placed = True

                if not placed:
                    if 'Andre' not in contents:
                        contents['Andre'] = Dict({
                            'class_label': "b",
                            'class_content': "ml3",
                            'subitems': [],
                            'count': 0
                        })
                    contents['Andre'].subitems[label] = "tables." + table_alias
                    contents['Andre'].count += 1
            else:
                if group_name in terms:
                    label = terms[group_name].label
                else:
                    label = group_name.capitalize()

                if config.urd_structure:
                    contents[label] = Dict({
                        'class_label': "b",
                        'class_content': "ml3",
                        'subitems': {key: 'tables.' + name for key, name in table_names.items()}
                    })
                    continue

                placed = False
                for idx, module in enumerate(modules):
                    if len(module) > 2 and len([val for val in table_names if val in module]):
                        mod = "Modul " + str(idx + 1)
                        contents[mod].class_label = "b"
                        contents[mod].class_contents = "ml3"
                        contents[mod].subitems[label] = Dict({
                            'class_label': "b",
                            'class_content': "ml3",
                            'subitems': table_names
                        })
                        if 'count' not in contents[mod]:
                            contents[mod].count = 0
                        contents[mod].count += len(table_names)
                        placed = True

                if not placed:
                    if 'Andre' not in contents:
                        contents['Andre'] = Dict({
                            'class_label': "b",
                            'class_content': "ml3",
                            'subitems': [],
                            'count': 0
                        })
                    contents['Andre'].subitems[label] = Dict({
                        'class_label': "b",
                        'class_content': "ml3",
                        'subitems': table_names
                    })
                    contents['Andre'].count += len(table_names)

        #// contents = sorted(contents.items())

        # Move 'Andre' last
        if 'Andre' in contents:
            other = contents['Andre']
            del contents['Andre']
            contents['Andre'] = other
        
        self.contents = contents

        # todo: progress = 100

        if not os.path.isdir(f"schemas/{db.schema}"):
            # todo: try catch
            os.mkdir(f"schemas/{db.schema}")

        # remove attributes that shouldn't be written to schema.json

        json_string = json.dumps(vars(self), indent=4)

        # todo: if json_string == False

        schema_file = f"schemas/{db.schema}/schema.ny.json"
        drop_file = f"schemas/{db.schema}/drop.sql"

        # todo: try catch
        fh_schema = open(schema_file, "w")
        fh_drop = open(drop_file, "w")

        fh_schema.write(json_string)
        fh_drop.write("\n".join(drops))

        return {
            'success': True,
            'msg': "Skjema oppdatert",
            'warn': warnings
        }


    def get_relation_tables(self, table_name, relation_tables):
        table = self.tables[table_name]

        for relation in table.relations.values():
            if relation.get('hidden', False): continue

            if relation.table in relation_tables:
                relation_tables.append(relation.table)
                relation_tables = self.get_relation_tables(relation.table, relation_tables)
            
        return relation_tables

    def get_content_items(self, tbl_alias, sub_tables, contents):
        if tbl_alias in self.terms:
            label = self.terms[tbl_alias].label
        else:
            label = tbl_alias.replace("_", " ")
        
        if self.config.norwegian_chars:
            label = label.replace("ae", "æ")
            label = label.replace("oe", "ø")
            label = label.replace("aa", "å")

        label = label.capitalize()

        if tbl_alias not in sub_tables:
            contents[label] = "tables." + tbl_alias
        else:
            contents[label] = Dict()
            contents[label].item = "tables." + tbl_alias
            contents[label].subitems = Dict()

            for subtable in sub_tables[tbl_alias]:
                contents[label].subitems = self.get_content_items(subtable, sub_tables, contents[label].subitems)
            
        return contents

                        





                
 
                
                








            





                







                













    
