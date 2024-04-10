# Urðr (Universal relational database reflection)

Urðr (named after one of the [Norns](https://en.wikipedia.org/wiki/Norns)
in Norse mythology) is an application that can be used to map, display and
register information in a relational database.  It uses information only from
the database itself (reflection) in order to display data and relations between
them. This requires some rules to be followed when designing the database. The
rules are often based on common principles for designing databases.

The rules are laid out here:
[self-documenting databases](./docs/self-documenting-databases.md).

The application is designed for archivists, who need to map databases, find how
the information is connected, and create access databases from the original
database.

Urðr can analyze the original database and find which tables, columns and
relations have been used the most, and create an ER diagram of these
relations.

After a original database has been analyzed, an access database can be made
based on the rules that Urðr uses to present the data.

##  Requirements

Linux or MacOS

Python >= 3.10

## Installation

~~~ sh
git clone https://github.com/fkirkholt/urd-backend urdr
cd urdr
pip install -r requirements.txt
~~~

Also install the [frontend](https://github.com/fkirkholt/urd-frontend)
before starting the application.

## Starting web server

~~~ sh
python3 main.py
~~~

## Features

- Display data for all tables, based on reflection
- Edit data in all tables
- Allow searching in all table columns
- [Group][group] tables based on relations or prefix
- Show interactive [ER-diagram](docs/er-diagram.md)
- Analyze relations and group tables by modules
- Filter relations and columns based on how much they are used
- Run sql queries
- Use [html attributes][html-attributes]
  to configure how a database is displayed
- Use the database's own authentication and authorization
- Supports [row based authorization][row-based-access] based on views
- Assign users to predefined roles
- Export database to other database systems
- Convert columns with rtf to markdown

![The data panel](/docs/assets/images/data-panel.png)

[group]: docs/self-documenting-databases.md#grouping
[html-attributes]: docs/self-documenting-databases.md#html-attributes
[row-based-access]: docs/self-documenting-databases.md#using-view-for-access-control
