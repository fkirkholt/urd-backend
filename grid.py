import re
import math
import time
from addict import Dict


def measure_time(func):
    def wrapper(*arg):
        t = time.time()
        res = func(*arg)
        if (time.time()-t) > 1:
            print("Time in", func.__name__,  str(time.time()-t), "seconds")
        return res

    return wrapper


class Grid:
    """Contains methods for returning metadata and data for grid"""

    def __init__(self, table):
        self.tbl = table
        self.db = table.db
        self.user_filtered = False
        self.sort_columns = []
        self.cond = Dict({
            'prep_stmnts': [],
            'params': [],
            'stmnts': []
        })

    def get_select_expression(self, col):
        """Get select expression for column in grid"""
        select = ''
        col.ref = f'"{self.tbl.grid_view}"."{col.name}"'

        if 'view' in col:
            select = col.view
        elif col.element == 'textarea':
            select = "substr(" + col.ref + ', 1, 255)'
        else:
            select = col.ref

        return select

    @measure_time
    def get(self, pkey_vals=None):
        from table import Table
        """Return all metadata and data to display grid"""
        selects = {}  # dict of select expressions
        pkey = self.tbl.get_pkey()
        user_tables = self.db.get_user_tables()
        has_view = self.tbl.name + '_grid' in user_tables
        fields = self.tbl.get_fields()

        for col in pkey.columns:
            selects[col] = f'"{self.tbl.view}"."{col}"'

        expansion_column = self.get_expansion_column()
        if expansion_column:
            fkey = self.tbl.get_parent_fk()
            rel_column = fields[fkey.foreign[-1]]
            selects['count_children'] = self.select_children_count(fkey)

            # Filters on highest level if not filtered by user
            if (not self.user_filtered and len(self.cond.prep_stmnts) == 0):
                self.add_cond(self.tbl.view + '.' + rel_column.name, "IS NULL")

        actions = self.get_actions()

        if actions:
            for key, action in actions.items():
                if (not action.disabled or isinstance(action.disabled, bool)):
                    continue
                selects[key] = action.disabled

        if has_view:
            view_name = self.tbl.name + '_grid'
            view = Table(self.db, view_name)
            cols = view.get_columns()
            grid_columns = [col.column_name for col in cols]
            view_fields = view.get_fields()
            for field_name, field in view_fields.items():
                if field_name not in fields:
                    field.virtual = True
                    field.table_name = view_name
                    fields[field_name] = field
        else:
            grid_columns = self.get_grid_columns()

        # Uses grid_columns from view if exists
        for colname in grid_columns:
            col = fields[colname]
            selects[colname] = self.get_select_expression(col)

        display_values = self.get_display_values(selects)

        values = self.get_values(selects)
        recs = self.get_records(display_values, values)

        data = Dict({
            'name': self.tbl.name,
            'type': self.tbl.get_type(),
            'records': recs,
            'count_records': self.get_rowcount(),
            'fields': fields,
            'grid': {
                'columns': grid_columns,
                'sums': self.get_sums(),
                'sort_columns': self.sort_columns,
                'actions': ["show_file"] if "show_file" in actions else []
            },
            'form': self.get_form(),
            'privilege': self.tbl.user_privileges(),
            'hidden': self.tbl.is_hidden(),
            'pkey': pkey.columns,
            'fkeys': self.tbl.get_fkeys(),
            'indexes': self.tbl.get_indexes(),
            'label': self.db.get_label('table', self.tbl.name),
            'actions': actions,
            'limit': self.tbl.limit,
            'offset': self.tbl.offset,
            'selection': self.get_selected_idx(pkey_vals, selects),
            'conditions': self.cond.stmnts,
            'expansion_column': expansion_column,
            'relations': self.tbl.get_relations(),
            'saved_filters': []  # Needed in frontend
        })

        return data

    def get_records(self, display_values, values):
        """"Return formatted records from values and display values"""
        recs = []
        for row in display_values:
            cols = {k: {'text': text} for k, text in row.items()}
            if 'count_children' in row:
                recs.append({
                    'count_children': row['count_children'],
                    'columns': cols
                })
                del row['count_children']
            else:
                recs.append({'columns': cols})

        pkey = self.tbl.get_pkey()
        for index, row in enumerate(values):
            for col, val in row.items():
                recs[index]['columns'][col]['value'] = val
            recs[index]['pkey'] = {key: row[key] for key in pkey.columns}

        row_formats = self.get_format()
        for idx, row in enumerate(row_formats.rows):
            classes = []
            for key, value in row.items():
                id_ = int(key[1:])
                if int(value):
                    classes.append(row_formats.formats[id_]['class'])
            class_ = " ".join(classes)
            recs[idx]['class'] = class_

        return recs

    def get_selected_idx(self, pkey_vals, selects):
        """Return rowindex for record selected in frontend"""
        if not pkey_vals:
            return None

        prep_stmnts = []
        params = []
        for colname, value in pkey_vals.items():
            prep_stmnts.append(f"{colname} = ?")
            params.append(value)

        # rec_conds = [f"{colname} = '{value}'" for colname, value
        #              in pkey_vals.items()]
        rec_cond = " WHERE " + " AND ".join(prep_stmnts)
        join = self.tbl.get_join()

        cond = ''
        if len(self.cond.prep_stmnts):
            cond = "WHERE " + " AND ".join(self.cond.prep_stmnts)

        order_by = self.make_order_by()

        sql = f"""
        select rownum - 1
        from   (select row_number() over ({order_by}) as rownum,
                       {self.tbl.view}.*
                from   {self.tbl.view}
                {join}
                {cond}) tab
        {rec_cond};
        """

        params = self.cond.params + params
        idx = self.db.query(sql, params).fetchval()
        if idx is not None:
            page_nr = math.floor(idx / self.tbl.limit)
            self.tbl.offset = page_nr * self.tbl.limit
            row_idx = idx - self.tbl.offset
        else:
            row_idx = 0

        return row_idx

    def select_children_count(self, fkey):
        """ number of relations to same table for expanding row"""
        wheres = []

        for idx, colname in enumerate(fkey.foreign):
            primary = fkey.primary[idx]
            wheres.append(colname + ' = ' + self.tbl.name + '.' + primary)

        where = ' and '.join(wheres)

        return f"""(
            select count(*)
            from {self.db.schema or self.db.cat}.{self.tbl.name} child_table
            where {where}
            )"""

    def get_expansion_column(self):
        """Return column that should expand a hierarchic table"""
        self_relation = False
        for rel in self.tbl.get_relations().values():
            if rel.table == self.tbl.name:
                self_relation = True
                break

        if not self_relation:
            return None

        ident_cols = None
        for idx in self.tbl.get_indexes().values():
            if idx.columns != self.tbl.get_pkey() and idx.unique:
                ident_cols = idx.columns
                if idx.name.endswith("_sort_idx"):
                    break

        if not ident_cols:
            return None

        ident_col = None
        fields = self.tbl.get_fields()
        for colname in ident_cols:
            col = fields[colname]
            if col.datatype == 'string':
                ident_col = colname

        return ident_col

    def get_actions(self):
        # Make action for displaying files
        indexes = self.tbl.get_indexes()
        filepath_idx = indexes.get(self.tbl.name + "_filepath_idx", None)
        actions = Dict()
        if filepath_idx:
            last_col = filepath_idx.columns[-1]

            action = Dict({
                'label': "Show file",
                'url': "/file",
                'icon': "external-link",
                'communication': "download",
                'disabled': f"({last_col} is null)",
            })

            actions.show_file = action

        return actions

    def get_grid_columns(self):
        """Return columns belonging to grid"""
        indexes = self.tbl.get_indexes()
        grid_idx = indexes.get(self.tbl.name + "_grid_idx", None)
        type_ = self.tbl.get_type()
        if grid_idx:
            columns = grid_idx.columns
        else:
            pkey = self.tbl.get_pkey()
            fkeys = self.tbl.get_fkeys()
            hidden = self.tbl.is_hidden()
            columns = []
            for key, field in self.tbl.get_fields().items():
                # Don't show hdden columns
                if (
                    field.name[0:1] == '_' or
                    field.name[0:6].lower() == 'const_'
                ):
                    continue
                if field.size and (field.size < 1 or field.size >= 255):
                    continue
                if field.datatype == 'json':
                    continue
                if (
                    [field.name] == pkey.columns
                    and field.datatype == "integer"
                    and type_ != 'list'
                    and field.name not in fkeys
                    and hidden is False
                ):
                    continue
                columns.append(key)
                if len(columns) == 5:
                    break

        return columns

    def make_order_by(self):
        """Return 'order by'-clause"""
        pkey = self.tbl.get_pkey()

        order = "order by "
        sort_fields = Dict()
        if len(self.sort_columns) == 0:
            self.sort_columns = self.get_sort_columns()
        for sort in self.sort_columns:
            # Split into field and sort order
            parts = sort.split(' ')
            key = parts[0]
            direction = 'asc' if len(parts) == 1 else parts[1]
            if key in self.tbl.get_fields():
                tbl_name = self.tbl.view
            else:
                tbl_name = self.tbl.name + '_grid'
            sort_fields[key].field = tbl_name + "." + key
            sort_fields[key].order = direction

        if (len(pkey.columns) == 0 and len(sort_fields) == 0):
            return ""

        for sort in sort_fields.values():
            if self.db.cnxn.system == 'mysql':
                order += f"isnull({sort.field}), {sort.field} {sort.order}, "
            elif self.db.cnxn.system in ['oracle', 'postgres']:
                order += f"{sort.field} {sort.order}, "
            elif self.db.cnxn.system == 'sqlite3':
                order += f"{sort.field} is null, {sort.field} {sort.order}, "

        for field in pkey.columns:
            order += f'"{self.tbl.view}"."{field}", '

        order = order[0:-2]

        if self.db.cnxn.system in ['oracle', 'postgres']:
            order += " nulls last"

        return order

    @measure_time
    def get_values(self, selects):
        """Return values for columns in grid"""
        cols = []
        fields = self.tbl.get_fields()
        for key in selects.keys():
            if (
                (key in fields or key == 'rowid') and
                'source' not in fields[key]
            ):
                cols.append(f'"{self.tbl.grid_view}"."{key}"')

        select = ', '.join(cols)
        join = self.tbl.get_join()
        cond = self.get_cond_expr()
        order = self.make_order_by()

        user_tables = self.db.get_user_tables()
        if (self.tbl.name + '_grid') in user_tables:
            pkey = self.tbl.get_pkey()
            view_alias = self.tbl.name + "_grid"
            join_view = "join " + self.tbl.name + "_grid on "

            ons = [f'"{view_alias}"."{col}" = "{self.tbl.view}"."{col}"'
                   for col in pkey.columns]
            join_view += ' AND '.join(ons) + ' '
        else:
            join_view = ""

        sql = "select " + select + "\n"
        sql += "from " + (self.db.schema or self.db.cat)
        sql += '."' + self.tbl.view + '"\n'
        sql += join + "\n"
        sql += join_view
        sql += "" if not cond else "where " + cond + "\n"
        sql += order

        cursor = self.db.cnxn.cursor()
        cursor.execute(sql, self.cond.params)
        cursor.skip(self.tbl.offset)
        rows = cursor.fetchmany(self.tbl.limit)

        result = []
        colnames = [column[0] for column in cursor.description]
        for row in rows:
            result.append(dict(zip(colnames, row)))

        return result

    def get_rowcount(self):
        """Return rowcount for grid"""
        conds = self.get_cond_expr()
        join = self.tbl.get_join()
        namespace = self.db.schema or self.db.cat

        user_tables = self.db.get_user_tables()
        if (self.tbl.name + '_grid') in user_tables:
            pkey = self.tbl.get_pkey()
            view_alias = self.tbl.name + '_grid'
            join_view = f"join {view_alias} on "
            ons = [f'"{view_alias}"."{col}" = "{self.tbl.view}"."{col}"'
                   for col in pkey.columns]
            join_view += ' AND '.join(ons) + ' '
        else:
            join_view = ""

        sql = "select count(*)\n"
        sql += f'from {namespace}."{self.tbl.view}"\n'
        sql += join + "\n"
        sql += join_view
        sql += "" if not conds else f"where {conds}\n"

        cursor = self.db.cnxn.cursor()
        count = cursor.execute(sql, self.cond.params).fetchval()

        return count

    def get_display_values(self, selects):
        """Return display values for columns in grid"""

        order = self.make_order_by()
        join = self.tbl.get_join()
        conds = self.get_cond_expr()

        alias_selects = {}
        for key, value in selects.items():
            alias_selects[key] = f'{value} as "{key}"'
        select = ', '.join(alias_selects.values())

        user_tables = self.db.get_user_tables()
        if (self.tbl.name + '_grid') in user_tables:
            pkey = self.tbl.get_pkey()
            view_alias = self.tbl.name + "_grid"
            join_view = "join " + self.tbl.view + " on "

            ons = [f'"{view_alias}"."{col}" = "{self.tbl.view}"."{col}"'
                   for col in pkey.columns]
            join_view += ' AND '.join(ons) + "\n"
        else:
            join_view = ""

        sql = "select " + select + "\n"
        sql += f'from {(self.db.schema or self.db.cat)}."{self.tbl.grid_view}"\n'
        sql += join_view
        sql += join + "\n"
        sql += "" if not conds else "where " + conds + "\n"
        sql += order

        cursor = self.db.cnxn.cursor()
        cursor.execute(sql, self.cond.params)
        cursor.skip(self.tbl.offset)
        rows = cursor.fetchmany(self.tbl.limit)

        result = []
        colnames = [column[0] for column in cursor.description]
        for row in rows:
            result.append(dict(zip(colnames, row)))

        return result

    def get_sums(self):
        """Return list of sums for summation columns"""
        sums = []

        cols = self.get_summation_columns()
        join = self.tbl.get_join()
        cond = self.get_cond_expr()
        params = self.cond.params

        if len(cols) > 0:
            selects = []
            for col in cols:
                selects.append(f"sum({self.tbl.name}.{col}) as {col}")
            select = ', '.join(selects)

            sql = "select " + select + "\n"
            sql += f"from {self.tbl.name}\n"
            sql += join + "\n"
            sql += "" if not cond else "where " + cond

            cursor = self.db.cnxn.cursor()
            row = cursor.execute(sql, params).fetchone()
            cols = [col[0] for col in cursor.description]
            sums = dict(zip(cols, row))

        return sums

    def get_sort_columns(self):
        """Return columns for default sorting of grid"""
        indexes = self.tbl.get_indexes()
        sort_idx = indexes.get(self.tbl.name + "_sort_idx", None)
        grid_idx = indexes.get(self.tbl.name + "_grid_idx", None)
        if sort_idx:
            columns = sort_idx.columns
        elif grid_idx:
            columns = grid_idx.columns[0:3]
        else:
            columns = []

        return columns

    def get_summation_columns(self):
        """Return columns that should be summed"""
        indexes = self.tbl.get_indexes()
        sum_idx = indexes.get(self.tbl.name + "_summation_idx", None)

        return [] if not sum_idx else sum_idx.columns

    def set_search_cond(self, query):
        """Set search conditions for grid queries"""
        filters = query.split(" AND ")
        for fltr in filters:
            parts = re.split(r"\s*([=<>]|!=| IN| LIKE|NOT LIKE|"
                             r"IS NULL|IS NOT NULL)\s*", fltr, 2)
            if len(parts) == 1:
                # Simple search in any text field
                value = parts[0]
                case_sensitive = value.lower() != value
                value = '%' + value + "%"

                fields = self.tbl.get_fields()
                conds = []
                params = []
                for field in fields.values():
                    if field.fkey:
                        view = field.name if not field.view else field.view
                        if case_sensitive:
                            conds.append(f"{view} LIKE ?")
                        else:
                            conds.append(f"lower({view}) LIKE ?")
                        params.append(value)
                    elif field.datatype == "string":
                        if case_sensitive:
                            conds.append(f"{self.tbl.view}.{field.name}"
                                         " LIKE ?")
                        else:
                            conds.append(f"lower({self.tbl.view}.{field.name})"
                                         " LIKE ?")
                        params.append(value)
                expr = "(" + " OR ".join(conds) + ")"
                self.add_cond(expr=expr, value=params)
            else:
                field = parts[0]
                if "." not in field:
                    if field in self.tbl.get_fields():
                        tbl_name = self.tbl.view
                    else:
                        tbl_name = self.tbl.name + '_grid'
                    field = tbl_name + "." + field
                operator = parts[1].strip()
                value = parts[2].replace("*", "%")
                case_sensitive = value.lower() != value
                if (not case_sensitive and value.lower() != value.upper()):
                    field = f"lower({field})"
                if operator == "IN":
                    value = value.strip().split(",")
                if value == "":
                    value = None
                self.add_cond(field, operator, value)

    def add_cond(self, expr, operator=None, value=None):
        """Add condition used in grid queries"""
        if value is None:
            if operator in ["IS NULL", "IS NOT NULL"]:
                self.cond.prep_stmnts.append(f"{expr} {operator}")
            elif operator == "=":
                self.cond.prep_stmnts.append(f"{expr} IS NULL")
            else:
                self.cond.prep_stmnts.append(expr)
        elif operator == "IN":
            marks = ",".join(['?' for val in value])
            self.cond.prep_stmnts.append(f"{expr} {operator} ({marks})")
            self.cond.params.extend(value)
            value = "('" + "','".join(str(value)) + "')"
            self.cond.stmnts.append(f"{expr} {operator} {value}")
        elif operator is None:
            self.cond.prep_stmnts.append(expr)
            if isinstance(value, list):
                self.cond.params.extend(value)
            else:
                self.cond.params.append(value)

        else:
            self.cond.prep_stmnts.append(f"{expr} {operator} ?")
            self.cond.params.append(value)
            self.cond.stmnts.append(f"{expr} {operator} {value}")

    def get_cond_expr(self):
        """Return expression with all query conditions"""
        return " and ".join(self.cond.prep_stmnts)

    def get_client_conditions(self):
        """Return all conditions visible for client"""
        return self.cond.stmnts

    def get_field_groups(self, fields):
        """Group fields according to first part of field name"""
        col_groups = Dict()
        for field in fields.values():
            # Don't add column to form if it's part of
            # primary key but not shown in grid
            if (
                field.name in self.tbl.get_pkey().columns and
                field.name not in self.get_grid_columns()
            ):
                field.hidden = True

            if (hasattr(field, 'use') and field.use == 0):
                field.hidden = True
                continue

            # Group by prefix
            parts = field.name.split("_")
            group = parts[0]

            # Don't add fields that start with _
            # They are treated as hidden fields
            if group == "":
                field.hidden = True
                continue

            if group not in col_groups:
                col_groups[group] = []

            col_groups[group].append(field.name)

        return col_groups

    def get_form(self):
        """Return form as Dict for displaying record"""

        form = Dict({'items': {}})
        fields = self.tbl.get_fields()
        field_groups = self.get_field_groups(fields)

        attrs = self.db.get_html_attributes()
        if attrs.table[self.tbl.name]['data-form']:
            return attrs.table[self.tbl.name]['data-form']

        for group_name, col_names in field_groups.items():
            if len(col_names) == 1:
                cname = col_names[0]
                fkey = fields[cname].fkey
                postfix = None
                if fkey and cname == f"{fkey.table}_{'_'.join(fkey.primary)}":
                    postfix = '_'.join(fkey.primary)
                label = self.db.get_label('field', col_names[0], postfix)
                form['items'][label] = col_names[0]
            else:
                inline = False
                subitems = Dict()
                sum_size = 0
                for colname in col_names:
                    # removes group name prefix from column name
                    # and use the rest as label
                    label = self.db.get_label('field', colname,
                                              prefix=group_name)
                    subitems[label] = colname

                    field = fields[colname]
                    if 'size' in field:
                        sum_size += field.size
                    elif field.datatype in ["date", "integer"]:
                        sum_size += 10

                if sum_size <= 50:
                    inline = True

                group_label = self.db.get_label('fieldset', group_name)

                form['items'][group_label] = Dict({
                    'name': group_name,
                    'inline': inline,
                    'expandable': True,
                    'items': subitems
                })

        form = self.relations_form(form)

        return form

    def get_format(self):

        if 'meta_format' not in self.db.user_tables:
            return Dict({
                'formats': [],
                'rows': []
            })

        sql = """
        select id, class, filter
        from   meta_format
        where  table_ = ?
        """

        cursor = self.db.cnxn.cursor()
        rows = cursor.execute(sql, self.tbl.name).fetchall()
        colnames = [column[0] for column in cursor.description]
        selects = []
        formats = {}
        for row in rows:
            selects.append("(" + row.filter + ") AS f" + str(row.id))
            formats[row.id] = dict(zip(colnames, row))

        if len(selects) == 0:
            return Dict({
                'formats': [],
                'rows': []
            })

        select = ", ".join(selects)
        join = self.tbl.get_join()
        conds = self.get_cond_expr()
        cond = "" if not conds else f"where {conds}\n"
        params = self.cond.params
        ordr = self.make_order_by()

        sql = f"""
        select {select}
        from {self.tbl.name}
        {join}
        {cond}
        {ordr}
        """

        cursor = self.db.cnxn.cursor()
        cursor.execute(sql, params)
        cursor.skip(self.tbl.offset)
        rows = cursor.fetchmany(self.tbl.limit)

        result = []
        colnames = [column[0] for column in cursor.description]
        for row in rows:
            result.append(dict(zip(colnames, row)))

        return Dict({
            'formats': formats,
            'rows': result
        })

    def relations_form(self, form):
        """Add relations to form"""
        from table import Table
        relations = self.tbl.get_relations()
        rel_tbl_names = self.tbl.get_rel_tbl_names()

        for alias, rel in relations.items():
            rel.order = 10
            rel_table = Table(self.db, rel.table)
            name_parts = rel.table.split("_")

            if rel.table not in self.db.user_tables:
                rel.hidden = True

            # Find indexes that can be used to get relation
            index_exist = False
            slice_obj = slice(0, len(rel.foreign))
            rel_indexes = rel_table.get_indexes()
            for index in rel_indexes.values():
                if index.columns[slice_obj] == rel.foreign:
                    index_exist = True

            if index_exist and not rel.get('hidden', False):
                rel_pkey = rel_table.get_pkey()

                if set(rel_pkey.columns) <= set(rel.foreign):
                    # Put 1:1 relations first
                    rel.order = 1
                    rel.relationship = '1:1'
                else:
                    rel.relationship = '1:M'
                if set(rel_pkey.columns) > set(rel.foreign):
                    # Set order priority so that tables higher up in hierarchy
                    # comes before tables further down
                    rel.order = len(rel_pkey.columns) - \
                        rel_pkey.columns.index(rel.foreign[-1])

                rel.label = self.db.get_label('table', rel_table.name,
                                              prefix=self.tbl.name + '_',
                                              postfix='_' + self.tbl.name)

                # Add name of foreign key column if other than name
                # of reference table
                if rel.foreign[-1] not in self.tbl.name:
                    col = rel.foreign[-1]
                    postfix = None
                    if col == f"{self.tbl.name}_{'_'.join(rel.primary)}":
                        postfix = '_'.join(rel.primary)
                    colname = self.db.get_label('field', col, postfix).lower()

                    rel.label += " (" + colname + ")"
            else:
                rel.hidden = True

            relations[alias] = rel

        sorted_rels = dict(sorted(relations.items(),
                           key=lambda tup: tup[1].order))

        for alias, rel in sorted_rels.items():
            name_parts = rel.table.split("_")
            if (len(name_parts) > 1 and name_parts[0] in rel_tbl_names):
                continue
            if not rel.hidden:
                form['items'][rel.label] = "relations." + alias

        return form
