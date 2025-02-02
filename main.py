import uvicorn
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.responses import (HTMLResponse, JSONResponse, StreamingResponse,
                               FileResponse)
from fastapi.staticfiles import StaticFiles
from starlette import status
import io
import urllib.parse
import re
import tempfile
import shutil
from sqlalchemy import create_engine
from settings import Settings
from database import Database
from table import Table, Grid
from record import Record
from field import Field
from util import prepare
import json
import os
import hashlib
from addict import Dict
from jose import jwt
import time
import xattr
import pyodbc
from user import User
from odbc_engine import ODBC_Engine
from starlette.background import BackgroundTask


cfg = Settings()
cfg_default = Settings()

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="static/html")
mod = os.path.getmtime("static/js/dist/bundle.js")


def cleanup(temp_file):
    os.remove(temp_file)


def get_engine(cfg, db_name=None):
    # driver = cfg.driver[cfg.db_system]
    if cfg.use_odbc:
        return ODBC_Engine(cfg, db_name)
    if cfg.system != 'duckdb':
        driver = getattr(cfg, f'{cfg.system}_driver')

    if cfg.system == 'duckdb':
        db_name = db_name.split('.')[0]
        path = os.path.join(cfg.host, db_name) + '.db'
        url = f"duckdb:///{path}"
    elif cfg.system == 'sqlite':
        path = os.path.join(cfg.host, db_name) + '.db'
        url = f"sqlite+{driver}:///{path}"
    elif cfg.system == 'oracle':
        parts = cfg.host.split('/')
        url = f"{cfg.system}+{driver}://{cfg.uid}:{cfg.pwd}@{parts[0]}"
        if len(parts) > 1:
            url += '?service_name=' + parts[1]
    elif cfg.system == 'mssql' and driver == 'pyodbc':
        drivers = [x for x in pyodbc.drivers() if 'SQL Server' in x]
        odbc_driver = drivers[0].replace(' ', '+')
        url = f"{cfg.system}+{driver}://{cfg.uid}:{cfg.pwd}@{cfg.host}/{db_name}"
        url += f"?driver={odbc_driver}&TrustServerCertificate=Yes"
    elif cfg.system == 'mssql' and driver == 'pymssql':
        url = f"{cfg.system}+{driver}://{cfg.uid}:{cfg.pwd}@{cfg.host}/{db_name}"
        url += '?tds_version=7.4'
        print('url', url)
    else:
        url = f"{cfg.system}+{driver}://{cfg.uid}:{cfg.pwd}@{cfg.host}"
        if db_name:
            path = db_name.split('.')
            url += '/' + path[0]
        elif cfg.system == 'postgresql':
            url += '/postgres'

    engine = create_engine(url)

    try:
        with engine.connect():
            pass
    except Exception as ex:
        print(ex)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication"
        )

    if cfg.system == 'sqlite' and db_name == 'urdr':
        with engine.connect() as cnxn:
            sql = """
            select count(*) from user
            where id = :id and password = :pwd
            """

            hashed_pwd = hashlib.sha256(cfg.pwd.encode('utf-8')).hexdigest()
            sql, params = prepare(sql, {'id': cfg.uid, 'pwd': hashed_pwd})
            count = cnxn.execute(sql, params).fetchone()[0]

            if count == 0:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail={
                        'msg': "Invalid authentication",
                        "system": cfg.system,
                        "host": cfg.host,
                        "database": cfg.database
                    }
                )
    elif cfg.system == 'sqlite' and cfg_default.database == 'urdr':
        with engine.connect() as cnxn:
            path = os.path.join(cfg.host, cfg_default.database + '.db')
            sql, _ = prepare('ATTACH DATABASE "' + path + '" as urdr')
            cnxn.execute(sql)

    return engine


def token():
    return jwt.encode({
        "system": cfg.system,
        "server": cfg.host,
        "uid": cfg.uid,
        "pwd": cfg.pwd,
        "database": cfg.database,
        "timestamp": time.time()
    }, cfg.secret_key)


@app.middleware("http")
async def check_login(request: Request, call_next):
    session: str = request.cookies.get("session")

    if session:
        payload = jwt.decode(session, cfg.secret_key)
        cfg.system = payload["system"]
        cfg.host = payload["server"]
        cfg.uid = payload["uid"]
        cfg.pwd = payload["pwd"]
        cfg.database = payload["database"]
    elif (
        request.url.path not in ("/login", "/") and
        not request.url.path.startswith('/static')
    ):
        return JSONResponse(content={
            "message": "login",
            "detail": {
                'system': cfg.system,
                'host': cfg.host,
                'database': cfg.database
            }
        }, status_code=401)

    response = await call_next(request)
    if (
        cfg.uid and request.url.path not in ["/logout", "/"]
        and not request.url.path.startswith('/static')
    ):
        # Update cookie to renew expiration time
        response.set_cookie(key="session", value=token(), expires=cfg.timeout)

    return response


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("urd.html", {
        "request": request, "v": mod, "base": cfg.database
    })


@app.post("/login")
def login(response: Response, system: str, server: str, username: str,
          password: str, database: str):
    cfg.system = system
    cfg.uid = username
    cfg.pwd = password
    cfg.database = database
    cfg.host = server or 'localhost'
    app.state.cnxn = None

    # cfg.timeout = None if cfg.system == 'sqlite' else cfg.timeout
    if cfg.system == 'sqlite' and cfg.database != 'urdr':
        cfg.timeout = None
    response.set_cookie(key="session", value=token(), expires=cfg.timeout)

    return {"success": True}


@app.get("/logout")
def logout(response: Response):
    response.delete_cookie("session")
    cfg.uid = None
    cfg.pwd = None
    cnxn = {
        'system': cfg_default.system,
        'host': cfg_default.host,
        'database': cfg_default.database
    }

    return {'success': True, 'cnxn': cnxn}


@app.get("/dblist")
def dblist(role: str = None):
    result = []
    useradmin = False
    if cfg.system in ('sqlite', 'duckdb'):
        if cfg.database == 'urdr':
            engine = get_engine(cfg, 'urdr')
            user = User(engine, name=cfg.uid)
            rows = user.databases()

            for row in rows:
                base = Dict()
                base.columns.name = row.name
                base.columns.label = row.name.capitalize()
                base.columns.description = row.description
                result.append(base)

        else:
            file_list = os.listdir(cfg.host)
            for filename in file_list:
                attrs = xattr.xattr(cfg.host + '/' + filename)
                comment = None
                if 'user.comment' in attrs:
                    comment = attrs.get('user.comment')
                if os.path.splitext(filename)[1] not in ('.db', '.sqlite3'):
                    continue
                base = Dict()
                base.columns.name = filename
                base.columns.label = filename.capitalize()
                base.columns.description = comment
                result.append(base)
    else:
        engine = get_engine(cfg)
        if role:
            with engine.connect() as cnxn:
                sql, _ = prepare('set default role ' + role)
                cnxn.execute(sql)
        elif cfg.system in ['mysql', 'mariadb']:
            with engine.connect() as cnxn:
                sql, _ = prepare('select current_role()')
                rows = cnxn.execute(sql).fetchall()
                role = (None if len(rows) == 0 else rows[0][0]
                        if len(rows) == 1 else 'ALL')

                if role:
                    sql, _ = prepare('set role ' + role)
                    cnxn.execute(sql)
                    cnxn.commit()

        user = User(engine)
        rows = user.databases()

        for row in rows:
            base = Dict()
            base.columns.name = row.db_name
            base.columns.label = row.db_name.capitalize()
            base.columns.description = row.db_comment
            result.append(base)

        # Find if user has useradmin privileges
        if cfg.system in ['mysql', 'mariadb']:
            with engine.connect() as cnxn:
                sql, _ = prepare('show grants')
                rows = cnxn.execute(sql).fetchall()
            for row in rows:
                stmt = row[0]
                matched = re.search(r"^GRANT\s+(.+?)\s+ON\s+(.+?)\s+TO\s+",
                                    stmt)
                if not matched:
                    continue
                privs = matched.group(1).strip().lower() + ','
                privs = [priv.strip() for priv in
                         re.findall(r'([^,(]+(?:\([^)]+\))?)\s*,\s*', privs)]
                if 'all privileges' in privs or 'create user' in privs:
                    useradmin = True

    return {'data': {
        'records': result,
        'roles': [] if cfg.system in ('sqlite', 'duckdb') else user.roles,
        'role': role,
        'useradmin': useradmin,
        'system': cfg.system
    }}


@app.get("/userlist")
def userlist():
    users = []
    roles = []
    engine = get_engine(cfg)
    with engine.connect() as cnxn:
        if engine.name in ['mysql', 'mariadb']:
            sql = """
            select user as name, Host as host
            from mysql.user
            where host not in ('%', '')
              and user not in ('PUBLIC', 'root', 'mariadb.sys', '')
            order by user
            """
            sql, _ = prepare(sql)
            rows = cnxn.execute(sql).fetchall()

            for row in rows:
                user = Dict()
                user.name = row.name
                user.host = row.host
                users.append(user)

            sql = """
            select user as name
            from mysql.user
            where host in ('%', '')
              and not length(authentication_string)
              and user != 'PUBLIC'
            """
            sql, _ = prepare(sql)
            rows = cnxn.execute(sql).fetchall()

            for row in rows:
                roles.append(row.name)

    return {'data': {'users': users, 'roles': roles}}


@app.get("/user_roles")
def user_roles(user: str, host: str):
    engine = get_engine(cfg)
    user = User(engine, user)

    return {'data': user.roles}


@app.put("/change_user_role")
def change_role(user: str, host: str, role: str, grant: bool):
    engine = get_engine(cfg)
    if grant:
        sql = f'grant {role} to {user}@{host}'
    else:
        sql = f'revoke {role} from {user}@{host}'
    with engine.connect() as cnxn:
        sql, _ = prepare(sql)
        cnxn.execute(sql)
        cnxn.commit()


@app.put("/change_password")
def change_password(base: str, old_pwd: str, new_pwd: str):
    if old_pwd != cfg.pwd:
        return {'data': 'Feil passord'}
    elif cfg.system in ['mysql', 'mariadb']:
        cfg2 = Settings()
        if None in [cfg2.system, cfg2.host, cfg2.uid, cfg2.pwd]:
            return {'data': 'Påloggingsdata mangler. Kontakt administrator.'}
        engine = get_engine(cfg2)
        with engine.connect() as cnxn:
            sql, _ = prepare(f"alter user {cfg.uid}@{cfg.host} identified by '{new_pwd}'")
            cnxn.execute(sql)
            cnxn.commit()

        return {'data': 'Passord endret'}
    elif cfg.system == 'sqlite' and cfg.database == 'urdr':
        engine = get_engine(cfg, base)
        db_path = engine.url.database
        urdr = 'main' if db_path.endswith('/urdr.db') else 'urdr'
        sql = f"update {urdr}.user set password = :pwd where id = :uid"
        pwd = hashlib.sha256(new_pwd.encode('utf-8')).hexdigest()
        with engine.connect() as cnxn:
            sql, params = prepare(sql, {'uid': cfg.uid, 'pwd': pwd})
            cnxn.execute(sql, params)
            cnxn.commit()
        return {'data': 'Passord endret'}

    else:
        return {'data': 'Ikke implementert for denne databaseplattformen'}


@app.put("/create_user")
def create_user(name: str, pwd: str):
    engine = get_engine(cfg)
    if cfg.system in ['mysql', 'mariadb']:
        sql, _ = prepare(f"create user '{name}'@'{cfg.host}' identified by '{pwd}'")
        with engine.connect() as cnxn:
            cnxn.execute(sql)
            cnxn.commit()

        return userlist()


@app.get("/database")
def db_info(base: str):
    engine = get_engine(cfg, base)
    dbo = Database(engine, base, cfg.uid)
    info = dbo.get_info()

    return {'data': info}


@app.get("/table")
async def get_table(base: str, table: str, filter: str = None,
                    limit: int = 30, offset: int = 0,
                    schema: str = None, sort: str = None,
                    compressed: bool = False, prim_key: str = None):
    engine = get_engine(cfg, base)
    if cfg.system == 'postgresql' and schema:
        base_path = base + '.' + schema
    else:
        base_path = base or schema
    dbo = Database(engine, base_path, cfg.uid)
    tbl = Table(dbo, table)
    privilege = dbo.user.table_privilege(dbo.schema, table)
    if privilege.select == 0:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No access"
        )
    grid = Grid(tbl)
    tbl.limit = limit
    tbl.offset = offset

    if sort:
        grid.sort_columns = Dict(json.loads(sort))

    if filter:
        filter = urllib.parse.unquote(filter)
        if filter.startswith('where '):
            where = filter[6:].split(';')[0]
            grid.cond.prep_stmnts.append(where)
        else:
            grid.set_search_cond(filter)

    grid.compressed = compressed

    # todo: handle sort
    pkey_vals = None
    if prim_key:
        pkey_vals = json.loads(prim_key)

    return {'data': grid.get(pkey_vals)}


@app.get("/record")
def get_record(base: str, table: str, pkey: str, schema: str = None):
    engine = get_engine(cfg, base)
    if cfg.system == 'postgresql' and schema:
        base_path = base + '.' + schema
    else:
        base_path = base or schema
    dbo = Database(engine, base_path, cfg.uid)
    tbl = Table(dbo, table)
    pk = json.loads(pkey)
    record = Record(dbo, tbl, pk)
    return {'data': record.get()}


@app.get("/children")
def get_children(base: str, table: str, pkey: str):
    engine = get_engine(cfg, base)
    dbo = Database(engine, base, cfg.uid)
    tbl = Table(dbo, table)
    tbl.offset = 0
    tbl.limit = 30
    pk = json.loads(pkey)
    record = Record(dbo, tbl, pk)
    return {'data': record.get_children()}


@app.get("/relations")
def get_relations(base: str, table: str, pkey: str, count: bool,
                  alias: str = None):
    engine = get_engine(cfg, base)
    dbo = Database(engine, base, cfg.uid)
    tbl = Table(dbo, table)
    pk = json.loads(pkey)
    record = Record(dbo, tbl, pk)
    if count:
        return {'data': record.get_relation_count()}
    else:
        relation = record.get_relation(alias)
        return {'data': {alias: relation}}


@app.put("/table")
async def save_table(request: Request):
    req = await request.json()
    base = req['base_name']
    engine = get_engine(cfg, base)
    dbo = Database(engine, base, cfg.uid)
    tbl = Table(dbo, req['table_name'])
    return {'data': tbl.save(req['records'])}


@app.get("/options")
async def get_options(request: Request):
    req = Dict({item[0]: item[1]
                for item in request.query_params.multi_items()})
    engine = get_engine(cfg, req.base)
    dbo = Database(engine, req.base, cfg.uid)
    tbl = Table(dbo, req.table)
    fld = Field(tbl, req.column)
    conds = req.condition.split(" and ") if req.condition else []
    search = None if 'q' not in req else req.q.replace("*", "%")
    fkey = tbl.get_fkey(req.column)
    if search:
        search = search.lower()
        view = None if not fkey else fld.get_view(fkey)
        view = view if view else req.column
        conds.append(f"lower(cast({view} as char)) like '%{search}%'")
    cond = " and ".join(conds)
    data = fld.get_options(cond, {})

    return data


@app.get('/urd/dialog_cache', response_class=HTMLResponse)
def dialog_cache(request: Request):
    return templates.TemplateResponse("update_cache.htm", {
        "request": request
    })


@app.get('/urd/update_cache')
async def update_cache(base: str, config: str):
    engine = get_engine(cfg, base)
    dbo = Database(engine, base, cfg.uid)
    dbo.config = Dict(json.loads(config))
    dbo.config.update_cache = True
    dbo.cache = None
    dbo.tables = Dict()
    def event_stream():
        if ('html_attributes' not in dbo.tablenames):
            dbo.create_html_attributes()
        tbl_count = len(dbo.tablenames)
        i = 0
        for tbl_name in dbo.tablenames:
            i += 1
            progress = round(i/tbl_count * 100) 
            data = json.dumps({'msg': tbl_name, 'progress': progress})
            yield f"data: {data}\n\n"

            if tbl_name[-5:] == '_view' and tbl_name[:-5] in dbo.tablenames:
                continue
            if '_fts' in tbl_name:
                continue

            table = Table(dbo, tbl_name)
            dbo.tables[tbl_name] = table.get()

        dbo.get_contents()
        data = json.dumps({'msg': 'done'})
        yield f"data: {data}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get('/export_sql')
def export_sql(dest: str, base: str, dialect: str, table_defs: bool,
               no_fkeys: bool, list_recs: bool, data_recs: bool,
               select_recs: bool, table: str = None):
    """Create sql for exporting a database

    Parameters:
    dialect: The sql dialect used (mysql, postgresql, sqlite)
    include_recs: If records should be included
    select_recs: If included records should be selected from
                 existing database
    """

    engine = get_engine(cfg, base)
    dbo = Database(engine, base, cfg.uid)
    download = True if dest == 'download' else False
    if download:
        dest = tempfile.gettempdir()
    else:
        os.makedirs(dest, exist_ok=True)

    def event_stream(dbo, dest, dialect, table_defs, no_fkeys, list_recs,
                     data_recs, select_recs, table):
        if table:
            table = Table(dbo, table)
            filepath = os.path.join(dest, f"{table.name}.{dialect}.sql")
            with open(filepath, 'w') as file:
                if dialect == 'oracle':
                    file.write("SET DEFINE OFF;\n")
                    file.write("SET FEEDBACK OFF;\n")
                    file.write("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD';\n")
                if table_defs:
                    if dialect == 'oracle':
                        ddl = f'drop table {table.name};\n'
                    else:
                        ddl = f'drop table if exists {table.name};\n'
                    ddl += table.export_ddl(dialect, no_fkeys)
                    file.write(ddl)
                if (
                    (table.type == 'list' and list_recs) or
                    (table.type != 'list' and data_recs)
                ):
                    if select_recs:
                        file.write(f'insert into {table.name}\n')
                        file.write(f'select * from {dbo.schema}.{table.name};\n')
                    else:
                        if dialect == 'oracle':
                            file.write('WHENEVER SQLERROR EXIT 1;\n')
                        table.write_inserts(file, dialect, select_recs)
                        if dialect == 'oracle':
                            file.write('WHENEVER SQLERROR CONTINUE;\n')

                if table_defs:
                    ddl = table.get_indexes_ddl()
                    file.write(ddl)
        else:
            ddl = ''
            filepath = os.path.join(dest, f"{base}.{dialect}.sql")
            ordered_tables, self_referring = dbo.sorted_tbl_names()

            with open(filepath, 'w') as file:
                if dialect == 'oracle':
                    file.write("SET DEFINE OFF;\n")
                    file.write("SET FEEDBACK OFF;\n")
                    file.write("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD';\n")
                if table_defs:
                    for view_name in dbo.refl.get_view_names(dbo.schema):
                        if dialect == 'oracle':
                            ddl += f"drop view {view_name};\n"
                        else:
                            ddl += f"drop view if exists {view_name};\n"

                    for tbl_name in reversed(ordered_tables):
                        if dialect == 'oracle':
                            ddl += f"drop table {tbl_name};\n"
                        else:
                            ddl += f"drop table if exists {tbl_name};\n"

                    file.write(ddl)
                    ddl = ''

                if dialect == 'oracle':
                    file.write('WHENEVER SQLERROR EXIT 1;\n')

                count = len(ordered_tables)
                i = 0
                for tbl_name in ordered_tables:
                    i += 1
                    progress = round(i/count * 100)
                    data = json.dumps({'msg': tbl_name, 'progress': progress})
                    yield f"data: {data}\n\n"

                    if tbl_name is None:
                        continue
                    if tbl_name == 'sqlite_sequence':
                        continue
                    if '_fts' in tbl_name:
                        continue
                    table = Table(dbo, tbl_name)
                    if table_defs:
                        file.write(table.export_ddl(dialect, no_fkeys))
                    if list_recs or data_recs:
                        self_ref = None
                        if tbl_name in self_referring:
                            self_ref = self_referring[tbl_name]
                        if (
                            (table.type == 'list' and list_recs) or
                            (table.type != 'list' and data_recs)
                        ):
                            if dialect == 'oracle':
                                file.write(f'prompt inserts into {table.name}\n')
                            if select_recs:
                                file.write(f'insert into {table.name}\n')
                                file.write(f'select * from {dbo.schema}.{table.name};\n')
                            else:
                                table.write_inserts(file, dialect, select_recs, fkey=self_ref)
                    if table_defs:
                        file.write(table.get_indexes_ddl())

                if table_defs:
                    i = 0
                    for view_name in dbo.refl.get_view_names(dbo.schema):
                        if i == 0:
                            ddl += '\n'
                        i += 1
                        try:
                            # Fails in mssql if user hasn't got permission VIEW DEFINITION
                            view_def = dbo.refl.get_view_definition(view_name, dbo.schema)
                        except Exception as e:
                            view_def = f"-- ERROR: Couldn't get definition for view {view_name} "
                            print(e)
                        if view_def:
                            ddl += f'{view_def}; \n\n'
                        else:
                            ddl += f"-- View definition not supported for {dbo.engine.name} yet\n"

                    file.write(ddl)

        if download:
            new_path = os.path.join(tempfile.gettempdir(),
                                    os.path.basename(filepath))
            os.rename(filepath, new_path)
            data = json.dumps({'msg': 'done', 'path': new_path})
            yield f"data: {data}\n\n"
        else:
            data = json.dumps({'msg': 'done'})
            yield f"data: {data}\n\n"

    event_generator = event_stream(dbo, dest, dialect, table_defs, no_fkeys,
                                   list_recs, data_recs, select_recs, table)
    return StreamingResponse(event_generator, media_type="text/event-stream")


@app.get('/download')
def download_file(path: str, media_type: str):
    filename = os.path.basename(path)
    return FileResponse(path, media_type=media_type, filename=filename,
                        background=BackgroundTask(cleanup, path))


@app.get('/export_tsv')
def export_tsv(base: str, objects: str, dest: str, table: str = None):
    engine = get_engine(cfg, base)
    dbo = Database(engine, base, cfg.uid)
    download = True if dest == 'download' else False
    if download:
        tempdir = tempfile.TemporaryDirectory()
        dest = tempdir.name
    else:
        if not os.path.exists(dest):
            os.makedirs(dest)

    def event_stream(dbo, objects, dest, table):
        if table:
            table = Table(dbo, table)
            table.offset = 0
            table.limit = None
            columns = json.loads(urllib.parse.unquote(objects))
            filepath = os.path.join(dest, 'data', table.name + '.tsv')
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            docpath = os.path.join(dest, 'documents')
            os.makedirs(docpath, exist_ok=True)
            table.write_tsv(filepath, columns=columns)
            new_path = os.path.join(tempfile.gettempdir(), table.name + '.tsv')
            os.rename(filepath, new_path)
            data = json.dumps({'msg': 'done', 'path': new_path})

            yield f"data: {data}\n\n"
            return new_path
        else:
            tables = json.loads(urllib.parse.unquote(objects))
            i = 0
            count = len(tables)
            for tbl_name in tables:
                i += 1
                progress = round(i/count * 100) 
                data = json.dumps({'msg': tbl_name, 'progress': progress})
                yield f"data: {data}\n\n"
                table = Table(dbo, tbl_name)
                table.offset = 0
                table.limit = None
                filepath = os.path.join(dest, 'data', tbl_name + '.tsv')
                table.write_tsv(filepath)
            if download:
                path = shutil.make_archive(dest, 'zip', dest)
                os.rename(path, '/tmp/data.zip')
                data = json.dumps({'msg': 'done', 'path': '/tmp/data.zip'})

                yield f"data: {data}\n\n"
            else:
                data = json.dumps({'msg': 'done'})
                yield f"data: {data}\n\n"

    return StreamingResponse(event_stream(dbo, objects, dest, table),
                             media_type="text/event-stream")


@app.get('/import_tsv')
def import_tsv(base: str, dir: str):
    engine = get_engine(cfg, base)
    dbo = Database(engine, base, cfg.uid)

    return StreamingResponse(dbo.import_tsv(dir), media_type="text/event-stream")


@app.get('/kdrs_xml')
def export_kdrs_xml(base: str, version: str, descr: str):
    engine = get_engine(cfg, base)
    dbo = Database(engine, base, cfg.uid)
    xml = dbo.export_as_kdrs_xml(version, descr)
    response = StreamingResponse(io.StringIO(xml), media_type="application/xml")
    response.headers['Content-Disposition'] = \
        f'attachment; filename={dbo.identifier}.xml'

    return response


@app.get('/file')
def get_file(base: str, table: str, pkey: str, column: str = None):
    pkey = json.loads(urllib.parse.unquote(pkey))
    engine = get_engine(cfg, base)
    dbo = Database(engine, base, cfg.uid)
    tbl = Table(dbo, table)
    rec = Record(dbo, tbl, pkey)
    path = rec.get_file_path(column)
    path = os.path.join(cfg.host, path)

    return FileResponse(path)


@app.post('/convert')
def convert(base: str, table: str, from_format: str, to_format: str,
            fields: str):
    fields = json.loads(fields)
    engine = get_engine(cfg, base)
    dbo = Database(engine, base, cfg.uid)
    tbl = Table(dbo, table)
    for field_name in fields:
        result = tbl.convert(field_name, from_format, to_format)

    return {'result': result}


@app.get('/query')
def query(base: str, sql: str, limit: str):
    print('sql', sql)
    engine = get_engine(cfg, base)
    dbo = Database(engine, base, cfg.uid)
    if not hasattr(app.state, 'cnxn') or app.state.cnxn is None:
        app.state.cnxn = engine.connect()
    limit = 0 if not limit else int(limit)
    result = dbo.query_result(sql, limit, app.state.cnxn)

    return {'result': result}


if __name__ == '__main__':
    uvicorn.run(
        app,
        host='localhost',
        port=8000,
    )
