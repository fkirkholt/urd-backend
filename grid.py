import re
import math
from addict import Dict
from sqlalchemy import text


class Grid:
    """Contains methods for returning metadata and data for grid"""

    def __init__(self, table):
        self.tbl = table
        self.db = table.db
        self.user_filtered = False
        self.cond = Dict({
            'prep_stmnts': [],
            'params': {},
            'stmnts': []
        })

    def get_select_expression(self, col):
        """Get select expression for column in grid"""
        select = ''
        col.ref = f'{self.tbl.grid_view}.{col.name}'

        if 'view' in col:
            select = col.view
        elif col.element == 'textarea':
            select = "substr(" + col.ref + ', 1, 255)'
        else:
            select = col.ref

        return select

    def get(self, pkey_vals=None):
        """Return all metadata and data to display grid"""
        selects = {}  # dict of select expressions

        for col in self.tbl.pkey.columns:
            selects[col] = f'{self.tbl.view}.{col}'

        expansion_column = self.get_expansion_column()
        if expansion_column:
            fkey = self.tbl.get_parent_fk()
            rel_column = fkey.constrained_columns[-1]
            ref_column = fkey.referred_columns[-1]
            selects['count_children'] = self.select_children_count(fkey)

            # Filters on highest level if not filtered by user
            if (not self.user_filtered and len(self.cond.prep_stmnts) == 0):
                expr = f"""
                    {self.tbl.view}.{rel_column} IS NULL
                    or {self.tbl.view}.{rel_column} = {self.tbl.view}.{ref_column}
                """
                self.cond.prep_stmnts.append(expr)

        actions = self.get_actions()

        if actions:
            for key, action in actions.items():
                if (not action.disabled or isinstance(action.disabled, bool)):
                    continue
                selects[key] = action.disabled

        # Uses grid_columns from view if exists
        for colname in self.columns:
            col = self.tbl.fields[colname]
            selects[colname] = self.get_select_expression(col)

        display_values = self.get_display_values(selects)

        values = self.get_values(selects)
        recs = self.get_records(display_values, values)

        data = Dict({
            'name': self.tbl.name,
            'type': self.tbl.type,
            'records': recs,
            'count_records': self.get_rowcount(),
            'fields': self.tbl.fields,
            'grid': {
                'columns': self.columns,
                'sums': self.get_sums(),
                'sort_columns': self.sort_columns,
                'actions': ["show_file"] if "show_file" in actions else []
            },
            'form': self.get_form(),
            'privilege': self.db.user.table_privilege(self.db.schema,
                                                      self.tbl.name),
            'hidden': self.tbl.is_hidden(),
            'pkey': self.tbl.pkey.columns or None,
            'fkeys': self.tbl.fkeys,
            'indexes': self.tbl.indexes,
            'label': self.db.get_label(self.tbl.name),
            'actions': actions,
            'limit': self.tbl.limit,
            'offset': self.tbl.offset,
            'selection': self.get_selected_idx(pkey_vals, selects),
            'conditions': self.cond.stmnts,
            'expansion_column': expansion_column,
            'relations': self.tbl.relations,
            'saved_filters': []  # Needed in frontend
        })

        return data

    def get_records(self, display_values, values):
        """"Return records from values and display values"""
        recs = []
        for row in display_values:
            cols = {k: {'text': text} for k, text in row.items()}
            if 'count_children' in row:
                recs.append({
                    'count_children': row['count_children'],
                    'columns': cols
                })
            else:
                recs.append({'columns': cols})

        for index, row in enumerate(values):
            for col, val in row.items():
                recs[index]['columns'][col]['value'] = val
            recs[index]['pkey'] = {key: row[key] for key in self.tbl.pkey.columns}

        return recs

    def get_selected_idx(self, pkey_vals, selects):
        """Return rowindex for record selected in frontend"""
        if not pkey_vals:
            return None

        prep_stmnts = []
        params = {}
        for colname, value in pkey_vals.items():
            prep_stmnts.append(f"{colname} = :{colname}")
            params[colname] = value

        # rec_conds = [f"{colname} = '{value}'" for colname, value
        #              in pkey_vals.items()]
        rec_cond = " WHERE " + " AND ".join(prep_stmnts)

        cond = ''
        if len(self.cond.prep_stmnts):
            cond = "WHERE " + " AND ".join(self.cond.prep_stmnts)

        order_by = self.make_order_by()

        sql = f"""
        select rownum - 1
        from   (select row_number() over ({order_by}) as rownum,
                       {self.tbl.view}.*
                from   {self.tbl.view}
                {self.tbl.joins}
                {cond}) tab
        {rec_cond};
        """

        params = self.cond.params + params
        with self.db.engine.connect() as cnxn:
            row = cnxn.execute(text(sql), params).fetchone()
        idx = row[0] if row else None
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

        for idx, col in enumerate(fkey.constrained_columns):
            prim = fkey.referred_columns[idx]
            wheres.append(f"{col} != {prim} and {col} = {self.tbl.name}.{prim}")

        where = ' and '.join(wheres)

        return f"""(
            select count(*)
            from {self.db.schema}.{self.tbl.name} child_table
            where {where}
            )"""

    def get_expansion_column(self):
        """Return column that should expand a hierarchic table"""
        self_relation = False
        for rel in self.tbl.relations.values():
            if rel.table == self.tbl.name:
                self_relation = True
                break

        if not self_relation:
            return None
        else:
            return self.columns[0]

    def get_actions(self):
        # Make action for displaying files
        filepath_idx_name = self.tbl.name.rstrip('_') + '_filepath_idx'
        filepath_idx = self.tbl.indexes.get(filepath_idx_name, None)
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

    @property
    def columns(self):
        """Return columns belonging to grid"""
        if hasattr(self, '_columns'):
            return self._columns
        elif self.db.cache:
            return self.db.cache.tables[self.tbl.name].grid.columns
        from table import Table
        self._columns = []
        has_view = self.tbl.name + '_grid' in self.db.tablenames
        if has_view:
            view_name = self.tbl.name + '_grid'
            view = Table(self.db, view_name)
            cols = self.db.refl.get_columns(view_name)
            self._columns = [col['name'] for col in cols]
            for field_name, field in view.fields.items():
                if field_name not in self.tbl.fields:
                    field.virtual = True
                    field.table_name = view_name
                    self.tbl.fields[field_name] = field

            return self._columns

        has_view = self.tbl.name + '_view' in self.db.tablenames
        if has_view:
            view_name = self.tbl.name + '_view'
            view = Table(self.db, view_name)
            for field_name, field in view.fields.items():
                if field_name not in self.tbl.fields:
                    field.virtual = True
                    field.table_name = view_name
                    self.tbl.fields[field_name] = field

        grid_idx = self.tbl.indexes.get(self.tbl.name + "_grid_idx", None)
        if grid_idx:
            self._columns = grid_idx.columns
            return self._columns

        fkeys = self.tbl.fkeys
        hidden = self.tbl.is_hidden()
        for key, field in self.tbl.fields.items():
            if len(self._columns) == 5:
                break
            # Don't show hdden columns
            if (
                field.name[0:1] == '_' or
                field.name[0:6].lower() == 'const_'
            ):
                continue
            if field.name == 'password':
                continue
            if field.datatype == 'str' and (field.size and field.size >= 255):
                continue
            if field.datatype == 'json':
                continue
            if (
                [field.name] == self.tbl.pkey.columns
                and field.datatype == "int"
                and self.tbl.type != 'list'
                and field.name not in fkeys
                and hidden is False
            ):
                continue
            if not (hasattr(field, 'virtual') or (not grid_idx and not len(self._columns) > 4)):
                continue
            if 'use' in field and (field.use < 0.9 or field.frequency > 0.4):
                continue
            self._columns.append(key)

        return self._columns

    def make_order_by(self):
        """Return 'order by'-clause"""

        order = "order by "
        sort_fields = Dict()
        for sort in self.sort_columns.values():
            if sort.col in self.tbl.fields and not self.tbl.fields[sort.col].virtual:
                tbl_name = self.tbl.view
            else:
                tbl_name = self.tbl.name + '_grid'
            order += f"{tbl_name}.{sort.col} {sort.dir}, "

        if (len(self.tbl.pkey.columns) == 0 and len(sort_fields) == 0):
            return ""

        if len(self.sort_columns) == 0:
            for field in self.tbl.pkey.columns:
                order += f'{self.tbl.view}.{field}, '

        order = order[0:-2]

        return order

    def get_values(self, selects):
        """Return values for columns in grid"""
        cols = []
        for key in selects.keys():
            if (
                (key in self.tbl.fields or key == 'rowid') and
                'source' not in self.tbl.fields[key]
            ):
                cols.append(f'{self.tbl.grid_view}.{key}')

        sql = ''
        access_idx = self.tbl.get_access_code_idx()
        if access_idx:
            sql += self.db.cte_access

        select = ', '.join(cols)
        cond = self.get_cond_expr()
        order = self.make_order_by()

        sql += "select " + select + "\n"
        sql += f'from {self.db.schema}.{self.tbl.view}\n'
        sql += self.tbl.joins
        sql += "" if not cond else "where " + cond + "\n"
        sql += order + "\n"

        if self.db.engine.name in ['mssql', 'oracle']:
            sql += f"offset {self.tbl.offset} rows\n"
            sql += f"fetch next {self.tbl.limit} rows only"
        else:
            sql += f"limit {self.tbl.limit} offset {self.tbl.offset}"

        with self.db.engine.connect() as cnxn:
            result = cnxn.execute(text(sql), self.cond.params)
            records = result.mappings().fetchall()

        return records

    def get_rowcount(self):
        """Return rowcount for grid"""
        conds = self.get_cond_expr()

        sql = ''
        access_idx = self.tbl.get_access_code_idx()
        if access_idx:
            sql += self.db.cte_access

        if self.db.engine.name == 'sqlite':
            sql += "select * \n"
        else:
            sql += "select count(*)\n"
        sql += f'from {self.db.schema}.{self.tbl.view}\n'
        sql += self.tbl.joins + "\n"
        sql += "" if not conds else f"where {conds}\n"

        # Counting can very slow in SQLite, so we limit to 1000
        if self.db.engine.name == 'sqlite':
            sql = f"select count(*) from (\n{sql}\nlimit 1000)"

        with self.db.engine.connect() as cnxn:
            count = cnxn.execute(text(sql), self.cond.params).first()[0]

        return count

    def get_display_values(self, selects):
        """Return display values for columns in grid"""

        alias_selects = {}
        for key, value in selects.items():
            alias_selects[key] = f'{value} as {key}'
        select = ', '.join(alias_selects.values())

        sql = ''
        access_idx = self.tbl.get_access_code_idx()
        if access_idx:
            sql += self.db.cte_access
            self.cond.params.uid = self.db.user.name
            col = access_idx.table + '.' + access_idx.columns[-1]
            stmt = f'({col} IS NULL or {col} in (select code from cte_access))'
            self.cond.prep_stmnts.append(stmt)

        order = self.make_order_by()
        conds = self.get_cond_expr()

        sql += "select " + select + "\n"
        sql += f'from {self.db.schema}.{self.tbl.view}\n'
        sql += self.tbl.joins
        sql += "" if not conds else "where " + conds + "\n"
        sql += order + "\n"

        if self.db.engine.name in ['mssql', 'oracle']:
            sql += f"offset {self.tbl.offset} rows\n"
            sql += f"fetch next {self.tbl.limit} rows only"
        else:
            sql += f"limit {self.tbl.limit} offset {self.tbl.offset}"

        with self.db.engine.connect() as cnxn:
            result = cnxn.execute(text(sql), self.cond.params)
            records = result.mappings().fetchall()

        return records

    def get_sums(self):
        """Return list of sums for summation columns"""
        sums = []

        cols = self.get_summation_columns()
        cond = self.get_cond_expr()
        params = self.cond.params

        if len(cols) > 0:
            selects = []
            for col in cols:
                selects.append(f"sum({self.tbl.name}.{col}) as {col}")
            select = ', '.join(selects)

            sql = "select " + select + "\n"
            sql += f"from {self.tbl.name}\n"
            sql += self.tbl.joins + "\n"
            sql += "" if not cond else "where " + cond

            with self.db.engine.connect() as cnxn:
                sums = cnxn.execute(text(sql), params).mappings().first()

        return sums

    @property
    def sort_columns(self):
        """Return columns for default sorting of grid"""
        if hasattr(self, '_sort_columns'):
            return self._sort_columns
        sort_idx = self.tbl.indexes.get(self.tbl.name + "_sort_idx", None)
        grid_idx = self.tbl.indexes.get(self.tbl.name + "_grid_idx", None)
        if sort_idx:
            columns = sort_idx.columns
            direction = sort_idx.column_sorting or {}
        elif grid_idx:
            columns = grid_idx.columns[0:1]
            direction = grid_idx.column_sorting or {}
        else:
            columns = []
            direction = {}

        self._sort_columns = Dict()
        for idx, col in enumerate(columns):
            dir = 'ASC' if col not in direction else direction[col][0]
            sort = Dict({'col': col, 'dir': dir, 'idx': idx})
            self._sort_columns[sort.col] = sort

        return self._sort_columns

    @sort_columns.setter
    def sort_columns(self, sorting):
        self._sort_columns = sorting

    def get_summation_columns(self):
        """Return columns that should be summed"""
        sum_idx = self.tbl.indexes.get(self.tbl.name + "_summation_idx", None)

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

                conds = []
                params = {}
                for field in self.tbl.fields.values():
                    if field.fkey:
                        view = field.name if not field.view else field.view
                        if case_sensitive:
                            conds.append(f"{view} LIKE :{field.name}")
                        else:
                            conds.append(f"lower({view}) LIKE :{field.name}")
                        params[field.name] = value
                    elif field.datatype == "str":
                        if case_sensitive:
                            conds.append(f"{self.tbl.view}.{field.name}"
                                         f" LIKE :{field.name}")
                        else:
                            conds.append(f"lower({self.tbl.view}.{field.name})"
                                         f" LIKE :{field.name}")
                        params[field.name] = value
                expr = "(" + " OR ".join(conds) + ")"
                self.cond.prep_stmnts.append(expr)
                self.cond.params.update(params)
            else:
                field = parts[0]
                if "." not in field:
                    if field in self.tbl.fields:
                        tbl_name = self.tbl.view
                    else:
                        tbl_name = self.tbl.name + '_grid'
                    field = tbl_name + "." + field
                else:
                    # Use view instead of original table if exists
                    field_parts = field.split('.')
                    tbl_name = field_parts[0]
                    field_name = field_parts[1]
                    if tbl_name + '_view' in self.db.tablenames:
                        field = f"{tbl_name}_view.{field_name}"

                mark = field.replace('.', '_')
                operator = parts[1].strip()
                value = parts[2].replace("*", "%")
                case_sensitive = value.lower() != value
                field_expr = field
                if (not case_sensitive and value.lower() != value.upper()):
                    field_expr = f"lower({field})"
                if operator == "IN":
                    value = value.strip().split(",")
                if value == "":
                    value = None
                expr = f"{field_expr} {operator} :{mark}"
                self.cond.prep_stmnts.append(expr)
                self.cond.params[mark] = value

    def get_cond_expr(self):
        """Return expression with all query conditions"""
        return " and ".join(self.cond.prep_stmnts)

    def get_client_conditions(self):
        """Return all conditions visible for client"""
        return self.cond.stmnts

    def get_field_groups(self, fields):
        """Group fields according to first part of field name"""
        col_groups = Dict()
        i = 0
        for field in fields.values():
            i += 1
            # Don't add column to form if it's part of
            # primary key but not shown in grid
            if (
                field.name in self.tbl.pkey.columns and
                field.name not in self.columns
            ):
                field.hidden = True

            if (hasattr(field, 'use') and field.use == 0):
                field.hidden = True
                continue

            # Group by prefix
            parts = field.name.split("_")

            # Don't add fields that start with _
            # They are treated as hidden fields
            if field.name.startswith('_'):
                field.hidden = True
                continue

            placed = False
            for group in col_groups:
                if field.name.startswith(group + '_'):
                    col_groups[group].append(field.name)
                    placed = True

            if placed:
                continue

            group = None
            for part in parts:
                test_group = group + '_' + part if group else part
                if (
                    len(fields) > i and
                    list(fields.keys())[i].startswith(test_group+'_')
                ):
                    group = test_group
                elif group is None:
                    group = part

            if group not in col_groups:
                col_groups[group] = []

            col_groups[group].append(field.name)

        return col_groups

    def get_form(self):
        """Return form as Dict for displaying record"""

        form = Dict({'items': {}})
        field_groups = self.get_field_groups(self.tbl.fields)

        attrs = self.db.html_attrs
        if attrs.table[self.tbl.name]['data-form']:
            return attrs.table[self.tbl.name]['data-form']

        for group_name, col_names in field_groups.items():
            if len(col_names) == 1:
                cname = col_names[0]
                label = self.tbl.fields[cname].label
                form['items'][label] = col_names[0]
            else:
                inline = False
                subitems = Dict()
                sum_size = 0
                for colname in col_names:
                    # removes group name prefix from column name
                    # and use the rest as label
                    label = self.db.get_label(colname, prefix=group_name)
                    subitems[label] = colname

                    field = self.tbl.fields[colname]
                    if field.get('size', None):
                        sum_size += field.size
                    elif field.datatype in ["date", "int"]:
                        sum_size += 10

                if sum_size <= 100:
                    inline = True

                group_label = self.db.get_label(group_name)

                form['items'][group_label] = Dict({
                    'name': group_name,
                    'inline': inline,
                    'expandable': True,
                    'items': subitems,
                    'size': sum_size
                })

        form = self.relations_form(form)

        return form

    def relations_form(self, form):
        """Add relations to form"""
        from table import Table

        relations = Dict()
        for alias, rel in self.tbl.relations.items():
            rel.order = 10
            rel_tbl = Table(self.db, rel.table)

            # Remove relations that are extensions to other tables
            # and where constrained columns is a sublist of pkey column
            # representing relation further up than parents
            if (
                rel_tbl.type == 'ext' and
                set(rel.constrained_columns) < set(rel_tbl.pkey.columns)
            ):
                continue

            if rel.table not in self.db.tablenames:
                rel.hidden = True

            # Find indexes that can be used to get relation
            index_exist = False
            slice_obj = slice(0, len(rel.constrained_columns))
            for index in rel_tbl.indexes.values():
                if index.columns[slice_obj] == rel.constrained_columns:
                    index_exist = True
                    rel.index = index

            if index_exist and not rel.get('hidden', False):
                if rel.relationship == '1:1':
                    rel.order = 1
                if set(rel_tbl.pkey.columns) > set(rel.constrained_columns):
                    # Set order priority so that tables higher up in hierarchy
                    # comes before tables further down
                    rel.order = len(rel_tbl.pkey.columns) - \
                        rel_tbl.pkey.columns.index(rel.constrained_columns[-1])

                rel.label = self.db.get_label(rel_tbl.name,
                                              prefix=self.tbl.name,
                                              postfix=self.tbl.name)

                # Add name of foreign key column if other than name
                # of reference table (and primary key column)
                if rel.constrained_columns[-1] not in self.tbl.name:
                    col = rel.constrained_columns[-1]
                    ref = rel.referred_columns[-1]
                    if (
                        col != f"{self.tbl.name.rstrip('_')}_{ref.strip('_')}"
                        and col != ref
                    ):
                        colname = self.db.get_label(col).lower()
                        rel.label += " (" + colname + ")"
            else:
                rel.hidden = True

            relations[alias] = rel

        sorted_rels = dict(sorted(relations.items(),
                           key=lambda tup: tup[1].order))

        for alias, rel in sorted_rels.items():
            if not rel.hidden:
                form['items'][rel.label] = "relation." + alias

        return form
