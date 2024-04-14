---
title: Self-documenting bases
---

# Tables

## Singular or plural

Whether you want table names in the singular or plural is a matter
of taste. The name will appear in the table of contents (the list of
tables), and as relationships to a record.

## Upper and lowercase letters

Quotation marks are not used around identifiers in Urdr, so you
have to name the objects in the base in a way that take this into
account. Some databases handle this fine anyway (such as SQLite,
MySQL and SQL Server), while you for others (PostgreSQL, Oracle) must
avoid naming identifiers with a mixture of lowercase and uppercase
letters with quotation marks.

It is recommended to use underscores to separate words in an
identifier.  This is used by Urdr as a word separator in labels.

## Grouping

You can group tables together by giving them the same prefix. Tables
with the same prefix will be listed in the list of tables on the left
with the prefix as a heading.

Otherwise, you can define subordinate tables by foreign keys included
in the primary key. Such subordinate tables will be placed under the
main table so that they are displayed when this is expanded.

## Lookup tables

Lookup tables are indicated with the postfix `_list` (or `_liste` in
norwegian). These are only displayed when admin mode is activated. They
are then displayed with a list icon.

When you use postfix to specify lookup tables, you also see at once
what kind of table this is, when you look at them in a database client.

Another way to indicate that something is a lookup table is to set
the data type of the primary key column to a text type, e.g. `char`,
`varchar` or `text`.

When you set the primary key to text, you can also provide a
recognizable identifier that can be used in compressed table display
in Urdr. Then you only see the identifier and not the display value
of the item.

## Cross-reference tables

Cross-reference tables are indicated with the postfix `_xref` or
`_link`. They are never displayed in the table list.

The label for such tables when displayed as relations, is derived
from the name. If the name of the referenced table occurs in the
table name, this is removed (as well as any postfix), and then the
label is derived from what you are left with.

If you e.g. has tables `user`, `group` and a cross-reference table
`user_group_xref`, and are showing a user, then the postfix `_xref`
and `user` is removed, and you'll see the relation "Group".

If you have a cross-reference table that you still want to display
in the list of tables, you can give it the name you want, and skip
the postfix. Then the table is placed in the list of tables under
the table that is referred first in the primary key.

E.g. if you have a table `archive_creator` with a primary key
`agent, archive`, the table `archive_creator` is placed under the
table `agent`. If you want it to go under `archive` instead, set the
primary key to `archive, agent`.

You should also group the columns in the primary key so they are in
the same order as the columns in the table.

## Extension tables

If a foreign key also contains the primary key, we have what
Urdr understand as an "extension table". This is a table with 1:1
relationship to the main table. Such tables usually start with the
name of the main table.

Extension tables are particularly relevant when you have fields that
belong together, that are not mandatory. Then you can extract them
into a separate table. E.g. can you have a table `person` and a table
`person_contakt`, where the contact details are added.

1:1 relationships are not displayed as a list. When expanding the
relationship, the relevant fields are displayed instead. This is
because only one record can be added as a 1:1 relationship.

Extension tables do not appear at the top level in the list of tables;
they are listed as subordinate under their parent tables, as other
subordinate tables.  If you want an extension table not to appear in
the table of contents, you can give it the postfix `_ext`.

## Subordinate tables

Tables where the whole or parts of the primary key also represent
a foreign key to other tables, are counted as subordinate tables.
These appear in the table of contents below the main table in
the hierarchy.

Cross-reference tables are thus also treated as subordinate
tables.

## Hierarchical tables

You have a hierarchical table when a foreign key in the table refers to
the primary key in the same table. If you put an index on the foreign
key and add a unique index to another column in the table, i.e. the
column you want use to name the record (cf. [Identification][1]),
then you will bring up only the top level when looking at the table,
and then be able to expand each level.

## 1:M relations

These relations are displayed with label made from the table name.

Prefix with table name of the referred table is subtracted (so if you
e.g. have a table `archive` and `archive_series`, subtract `archive_`
and we are left wit `series`).

If the name of the last column in the foreign key is different from
the table name to which the foreign key refers, this column name is
included in parenthesis.

If we have a `file.registered_by` referring to `user`, this relation
is labeled "File(registered_by)" from the `user`-record. That way
you see exactly which relation this is.

## Hidden tables

You specify that a table should be hidden (and only displayed in
admin mode) by letting the table name start with an underscore
(`_`). This harmonizes with a practice in certain programming languages
that variables that start with an underscore should be considered
private variables.

Lookup tables are also hidden when not in admin mode. Cross-reference
tables, as indicated with postfix `_xref` or `_link`, are always hidden
in the table list.

# Columns

## Invisible columns

You mark that a column should not be displayed by putting an underscore
in front, e.g. `_connection_string`.

But note that this does not work in Oracle, as identifiers must begin
with a letter. In Oracle you can hide columns by defining them as
`invisible`.

## Invisible columns controlling which relation to show

Urdr has the ability to control which relationships are displayed by
having one invisible column with default value set. When this column
is part of a foreign key, the relationship is only displayed when
the corresponding field in the current record has this value. This
column can be named with `const_` as prefix, which indicates that it
is a constant. Such columns are not displayed.

Example: If you have a table `file` and a relation `personal_file`
with additional information for personal, then you add a column
`file.type`, and a column `personal_file.const_type` with default value
`personal`. When one makes `const_type` as part of the foreign key
that refers to `file`, the relation `personal_file` is only displayed
for records where you have registered `personal` for `file.type`.

# Foreign keys

Foreign keys are used by Urdr to display relationships.

To show has-many relationships, one must have an index to find
the relationships.

The name of the column is used as label for a foreign key column. If
the name consists of the name of referred table and name of the
primary key column, only the table name is displayed. Ex. If you
have `archive_id` which refers to `archive.id`, the label "Archive"
is displayed.

# Indexes

Urdr uses indexes in great extent to know how data should be displayed.

## Grid

To determine which columns are to be displayed in the grid, the index `<table_name>_grid_idx` is used, if it exists.

If this index does not exist, the first five columns are displayed,
with exception for text columns with 255 characters or more, hidden
columns, and any autoinc column. The latter is defined as in SQLite
as an integer primary key.

The limit of 255 characters is due to MySQL limiting the number
of characters in indexes to this number.

For lookup-tables, the autoinc column is displayed anyway.

## Sorting

Sorting of a table is determined by index `<table_name>_sort_idx`
if it exists. If it doesn't exist, and if `<table_name>_grid_idx`
exists, the first three columns of this index are used as sorting. If
this index doesn'5 exist either, the table is sorted by primary key.

Sorting direction can be specified in the indexes for the databases that support this.

## Summation

Fields included in the index `<table_name>_summation_idx` will be
summed up in the footer of the grid.

## Identification

One uses a unique index different from the primary key to determine what
to be displayed from the record in a foreign key field in a referencing table.

If you also want the records to be sorted by this index, you can
use `<table_name>_sort_idx` and set this to unique.

If you have several unique indexes, the one named `...sort_idx` is used
for identification.

## Link to file

To identify a field as a file path, one can use an index
`<table_name>_filepath_idx`.

This also allows you to assemble the file path from several columns,
e.g. a column that denotes the path to the folder where the file is
located, and one denoting filename. The index is then created on all
of these columns. You must enter the columns in the order in which
they are used in the file path.

If you use SQLite, you can specify the path relative to the path to
the SQLite database file.

If you want to generate file names from a path and a column in the
table, you can create a generated column.

## Show has-many relationships

Foreign keys should be linked to indexes when showing has-many
relations. The indexes are used to retrieve all relations. Urdr does
not show such relations unless there is an index that can be used to
find them. If no index exists on the same columns as the foreign key,
the relation is displayed only from the referencing table.

MySQL and MariaDB creates indexes automatically when generating foreign
key. But these are also the only databases that Urdr supports that
do this automatically. So when Urdr requires that an index must be
in place to show the relationship, also ensure that these indexes are
created. This is therefore completely in line with Urdr's philosophy -
to make inquiries more efficient at the same time as they define how
the base is displayed.

## Register created and updated record

To register when a record was created/changed and by whom, you can
set index `<table_name>_created_idx` and `<table_name>_updated_idx`.
The first column in the index should be the date or timestamp, and
the second column should be the username of the user.

The column that denotes date or time must have a default value set
to `current_date` or `current_timestamp`. The column that denotes
username must have the default value `current_user`.

# HTML attributes

You can define html attributes in the table `html_attributes`. This
can either be created manually, or you can create a cached version
of the database structure, which will then generate this table. The
cache is placed in this table under selector `base`.

The table has only two columns: `selector` and `attributes`. The
former is a css selector. The css selector for DOM elements can be
specified here. The various fields and the field sets have been given
names so that it should be easy to select them with a css selector.

In the `attributes` column, you can enter all possible html attributes
for selected items. These will then be assigned to the elements when
the page is generated. The attributes are entered as yaml.

Since Urdr supports [Tachyons][2], you can enter Tachyons classes
here. Most of the elements are already styled with Tachyons classes,
so the classes listed here will replace those in the code. You can
inspect an element on the page to see which classes are used, copy
these and replace the ones you wish.

Each field in the record display is enclosed by a `label` tag; this
is called an "indirect label". It has been done this way to be able
to connect the label to the input. We cannot use the `for` attribute
to associate label with the correct input, since this requires a
unique ID, and with the flexibility in Urdr you can easily get the
same id twice.

In order to be able to style the label text itself, this is entered
in a `b` tag. This tag is used in modern html to mark keywords. And
the marked part of the label is indeed a sort of keyword.

It is possible to add text before or after the keyword of a label. This
is done by adding the `data-before` or `data-after` attribute,
with the desired text in the `b` element under `label`. This makes
it possible to add a colon after the label, or an asterisk to mark
the field as mandatory. The latter can be achieved with the selector
`label b:has(+[required])` together with attribute `data-after: '＊'`.

You can also add a unit of measure to a field, by entering the unit
of measure in the `data-after` attribute of the label. The label then
consists of both the keyword itself, and the unit of measurement that
comes after the field value.

You can customize how a field is displayed by specifying `data-type`
and/or `data-format`. This assumes that you use a selector in
following patter: `label[data-field="tablename.fieldname"]`. This
selector belongs to the `label` tag, which encloses the field. You can
skip `label` and only use `[data-field="tablename.fieldname"]`. The
html element for the field is then generatede based on the values of
`data-type` and `data-format`.

The following values for `data-type` are supported:
- json
- date

The following values for `data-format` are supported:
- link
- json
- yaml
- markdown

If you set `data-type` as "json" and `data-format` as "yaml", then the
data will be stored as json in the database, but you will to see
the data as `yaml'.  This applies by default to the html attributes
themselves.

You can enter `data-type: date` if you have a text column in
the database that is used to enter date. An html `<time>` tag
allows registration of dates in more formats than many databases,
e.g. "2012-05" which stands for May 2012. These can then be registered
as text in the database.

If you want to create a url of a field, you can set the attribute
`data-format: link` to the `label` tag. Then you get a `<a>` tag
around the field value in display mode. You can set the `href`
attribute by setting attribute `data-href` to the `label` tag. You
can use column names in curly brackets to be replaced by the value
of the column. E.g. `data-href: url/to/whatever?key={name}`

You can also make the `href` by setting attribute `onmouseover` to
the `a` tag, and using `this.dataset.value` for dynamic url depending
on the value of the field. The element that displays
the field value has an attribute `data-value` in order to facilitate
this.

The selector is written as `label[data-field="table_name.field_name"] > a`,
and the attribute can be something like this:

~~~ yaml
onmouseover: "this.href='/url/to/whatever?key='+this.dataset.value"
~~~

You can also style the grid, e.g. with the background color of the
row based on values in a column. Note that you have to add a default
style, otherwise the colors will not be updated correctly when sorting
the table afterwards.

# Views

Views are displayed in the same way as tables. To be able to display individual records in a view, you must define a primary key for the view.
This is done by entering attribute `data-pkey` in `html_attributes`.

If the view has the same primary keys as the table the view is based on,
the records will show the same relations as records from this table.

## Use view to determine grid

Instead of defining a grid using index `<table_name>_grid_idx`, you can
use a view `<table_name>_grid`. This view must have all the primary key
columns of the original table. The advantage of using a view instead
of an index, is that you can define columns that are not found in
the original table. This is how you can bring in e.g. statistics,
number of child records, etc.

All extra columns in the view will also be available in the record
view, and become searchable.

## Using view for access control

If you create a view with the name `<table_name>_view`, this view will
replace the table when making queries. You can thus use this view for
access control.

Example:

~~~ sql
create view series_view as
select * from series
where series.access_group is null or
series.access_group in (
select access_group from user_access_group
where username = current_user()
);
~~~

The view must be created by select everything from the original table,
since the view shall replace this table. Metadata for the view is
retrieved from the original table.

The user is then given access to the view, but not to the original
table. This presupposes that a cached version of the database structure
is created first.

You can have a view for access management and a view for the grid
at the same time. But then the view for the grid should also have
access control.

[1]: #Identification
[2]: https://tachyons.io/
