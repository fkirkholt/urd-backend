import uvicorn
from fastapi import FastAPI, Request, Response, HTTPException, Body
from fastapi.templating import Jinja2Templates
from fastapi.responses import (HTMLResponse, JSONResponse, StreamingResponse,
                               FileResponse)
from fastapi.staticfiles import StaticFiles
from starlette import status
import io
import urllib.parse
import re
import tempfile
from subprocess import run
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
import magic
from user import User
from odbc_engine import ODBC_Engine
from starlette.background import BackgroundTask
import typer


cfg = Settings()
cfg_default = Settings()

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="static/html")
mod = os.path.getmtime("static/js/dist/index.js")


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
        path = os.path.join(cfg.host, db_name)
        url = f"duckdb:///{path}"
    elif cfg.system == 'sqlite':
        path = os.path.join(cfg.host, db_name)
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

    if cfg.system == 'mariadb':
        url += '?charset=utf8mb4&collation=utf8mb4_unicode_ci'

    try:
        engine = create_engine(url)
    except Exception as ex:
        if str(ex).startswith('No module named'):
            msg = 'Please install driver ' + driver
        else:
            msg = str(ex)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=msg
        )

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
                        "database": db_name
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
    session = None
    path_parts = request.url.path.split('/')
    if 'cnxn' in request.query_params:
        session: str = request.cookies.get(request.query_params['cnxn'])
    elif len(path_parts) > 1:
        session: str = request.cookies.get(path_parts[1])

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
def login(response: Response, cnxn: str, system: str, server: str, username: str,
          password: str, database: str):
    cfg.cnxn = cnxn 
    cfg.system = system or cfg.system
    cfg.uid = username
    cfg.pwd = password
    cfg.database = database or cfg.database
    cfg.host = server or cfg.host

    # cfg.timeout = None if cfg.system == 'sqlite' else cfg.timeout
    if cfg.system == 'sqlite' and cfg.database != 'urdr':
        cfg.timeout = None
    response.set_cookie(key=cnxn, value=token(), expires=cfg.timeout)

    return {"success": True}


@app.get("/logout")
def logout(response: Response):
    response.delete_cookie("session")
    cfg = cfg_default
    cnxn = {
        'system': cfg_default.system,
        'host': cfg_default.host,
        'database': cfg_default.database
    }

    return {'success': True, 'cnxn': cnxn}


@app.get("/file")
def get_file(path: str):
    filepath = os.path.join(cfg.host, path)
    if cfg.system not in ['sqlite', 'duckdb']:
        return {'path': path, 'type': 'server'}
    if os.path.isdir(filepath):
        return {'path': path, 'type': 'dir'}
    if not os.path.isfile(filepath) and not os.path.isdir(filepath):
        return {'path': path, 'type': None}
    size = os.path.getsize(filepath)
    content = None
    msg = None
    type = magic.from_file(filepath, mime=True)
    text_types = ['application/javascript']
    with open(filepath, 'rb') as reader:
        if reader.read(6) == b'SQLite':
            type = 'sqlite'
        elif reader.read(4) == b'DUCK':
            type = 'duckdb'
    if type.startswith('text/') or type in text_types:
        if size < 100000000:
            with open(filepath, 'r') as file:
                content = file.read()
        else:
            msg = 'File too large to open'
    name = os.path.basename(filepath)
    lsp = False
    for ext in cfg.lsp_filetypes.split('|'):
        if path.endswith(ext):
            lsp = True

    return {'path': path, 'name': name, 'content': content, 'type': type,
            'msg': msg, 'abspath': filepath if lsp else None,
            'websocket': cfg.websocket if lsp else None}


@app.get('/backlinks')
def get_backlinks(path: str):
    backlinks = []
    filepath = os.path.join(cfg.host, path)
    for path, folders, files in os.walk(cfg.host):
        for filename in files:
            if not filename.endswith('.md'):
                continue
            relpath = os.path.relpath(filepath, path)
            with open(os.path.join(path, filename), 'r') as file:
                content = file.read()
                if '(' + relpath + ')' in content:
                    abspath = os.path.join(path, filename)
                    backlinks.append(os.path.relpath(abspath, cfg.host))

    return backlinks


@app.post("/file")
def update_file(path: str, content: str = Body(...)):
    filepath = os.path.join(cfg.host, path)
    with open(filepath, 'w') as file:
        file.write(content)

    return {'result': 'success'}


@app.put("/file_rename")
def rename_file(src: str, dst: str):
    src = os.path.join(cfg.host, src)
    dst = os.path.join(cfg.host, dst)
    os.rename(src, dst)
    filepath = os.path.join(cfg.host, src)
    for path, folders, files in os.walk(cfg.host):
        for filename in files:
            if not filename.endswith('.md'):
                continue
            relpath = os.path.relpath(filepath, path)
            new_content = ''
            with open(os.path.join(path, filename), 'r') as file:
                content = file.read()
                if '(' + relpath + ')' in content:
                    new_path = relpath.replace(os.path.basename(relpath),
                                               os.path.basename(dst))
                    new_content = content.replace('(' + relpath + ')',
                                                  '(' + new_path + ')')
            if new_content:
                with open(os.path.join(path, filename), 'w') as file:
                    file.write(new_content)
    return {'success': True}

@app.put("/file_delete")
def delete_file(filename: str):
  filepath = os.path.join(cfg.host, filename)
  os.remove(filepath)

  return {'success': True}


@app.get("/dblist")
def dblist(response: Response, role: str = None, path: str = None):
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
                base.columns.type = 'database'
                result.append(base)

        else:
            filepath = os.path.join(cfg.host, path) if path else cfg.host
            if os.path.isfile(filepath):
                dirpath = os.path.dirname(filepath)
            else:
                dirpath = filepath
            file_list = os.listdir(dirpath)
            file_list.sort()
            for filename in file_list:
                filepath = os.path.join(dirpath, filename)
                if os.path.islink(filepath):
                    continue
                attrs = xattr.xattr(filepath)
                comment = None
                if 'user.comment' in attrs:
                    comment = attrs.get('user.comment')
                base = Dict()
                base.columns.name = os.path.join(path, filename) if path else filename
                base.columns.label = filename
                base.columns.description = comment
                base.columns.type = 'file'
                base.columns.size = os.path.getsize(filepath)
                if os.path.isdir(filepath):
                    base.columns.type = 'dir'
                else:
                    with open(filepath, 'rb') as reader:
                        if reader.read(6) == b'SQLite':
                            base.columns.type = 'database'
                        elif reader.read(4) == b'DUCK':
                            base.columns.type = 'database'

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
            base.columns.type = 'database'
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
        'path': path,
        'roles': [] if cfg.system in ('sqlite', 'duckdb') else user.roles,
        'role': role,
        'useradmin': useradmin,
        'system': cfg.system
    }}


@app.get("/ripgrep")
def ripgrep(path: str, pattern: str):
    dir = os.path.join(cfg.host, path) if path else cfg.host
    cwd = os.getcwd()
    os.chdir(dir)
    cmd = 'rg ' + pattern + ' --line-number --color=always --colors=path:none'
    cmd += ' --max-columns=255 --max-columns-preview'
    cmd += '' if any(char.isupper() for char in pattern) else ' -i'
    result = run(cmd, shell=True, capture_output=True, text=True)
    os.chdir(cwd)
    lines = result.stdout.split('\n')
    files = []
    result = []
    for line in lines:
        if line == '':
            break
        parts = line.split(':', 1)
        file = parts[0]
        if file not in files:
            files.append(file)
            base = Dict()
            base.columns.name = os.path.join(path, file)
            base.columns.label = file
            if len(parts) > 1:
                desc = parts[1]
                base.columns.description = desc
            base.columns.type = 'file'
            result.append(base)
        elif len(parts) > 1:
            desc = parts[1]
            base.columns.description += '<br>' + desc

    return {'data': {
        'records': result,
        'path': path,
        'grep': pattern,
        'roles': [],
        'role': None,
        'useradmin': None,
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


@app.post("/record")
def create_record(base: str, table: str, pkey: str, values: str = Body(...)):
    engine = get_engine(cfg, base)
    dbo = Database(engine, base, cfg.uid)
    tbl = Table(dbo, table)
    pkey = json.loads(pkey)
    record = Record(dbo, tbl, pkey)
    vals = json.loads(values)
    pkey = record.insert(vals)

    return {'values': pkey}


@app.put("/record")
def update_record(base: str, table: str, pkey: str, values: str = Body(...)):
    engine = get_engine(cfg, base)
    dbo = Database(engine, base, cfg.uid)
    tbl = Table(dbo, table)
    pkey = json.loads(pkey)
    record = Record(dbo, tbl, pkey)
    vals = json.loads(values)

    return {'result': record.update(vals)}


@app.delete("/record")
def delete_record(base: str, table: str, pkey: str):
    engine = get_engine(cfg, base)
    dbo = Database(engine, base, cfg.uid)
    tbl = Table(dbo, table)
    pkey = json.loads(pkey)
    record = Record(dbo, tbl, pkey)

    return {'result', record.delete()}


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
        conds.append(f"lower({view}) like '%{search}%'")
    cond = " and ".join(conds)
    data = fld.get_options(cond, {}, get_parent=False)

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
               select_recs: bool, view_as_table: bool, 
               table: str = None, filter: str = None):
    """Create sql for exporting a database

    Parameters:
    dialect: The sql dialect used (mysql, postgresql, sqlite)
    list_recs: If records from lookup tables should be included
    data_recs: If records from data tables should be included
    select_recs: If included records should be selected from
                 existing database
    """

    engine = get_engine(cfg, base)
    dbo = Database(engine, base, cfg.uid)

    if cfg.system in ['sqlite', 'duckdb'] and dest != 'download':
        dest = os.path.join(cfg.host, dest)

    return StreamingResponse(dbo.export_sql(dest, dialect, table_defs, no_fkeys,
                                            list_recs, data_recs, select_recs,
                                            view_as_table, table, filter),
                             media_type="text/event-stream")


@app.get('/download')
def download_file(path: str, media_type: str):
    filename = os.path.basename(path)
    return FileResponse(path, media_type=media_type, filename=filename,
                        background=BackgroundTask(cleanup, path))


@app.get('/export_tsv')
def export_tsv(base: str, tables: str, dest: str, clobs_as_files: bool,
               limit: int = None, columns: str = None, folder: str = None,
               filter: str = None):
    engine = get_engine(cfg, base)
    dbo = Database(engine, base, cfg.uid)
    download = True if dest == 'download' else False
    tbls = json.loads(urllib.parse.unquote(tables))
    if columns:
        cols = json.loads(urllib.parse.unquote(columns))
    else:
        cols = None

    if download:
        tempdir = tempfile.TemporaryDirectory()
        dest = tempdir.name
    else:
        if not os.path.exists(dest):
            os.makedirs(dest)

    return StreamingResponse(dbo.export_tsv(tbls, dest, limit, clobs_as_files,
                                            cols, download, filter),
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


@app.get('/db_file')
def get_db_file(base: str, table: str, pkey: str, column: str = None):
    pkey = json.loads(urllib.parse.unquote(pkey))
    engine = get_engine(cfg, base)
    dbo = Database(engine, base, cfg.uid)
    tbl = Table(dbo, table)
    rec = Record(dbo, tbl, pkey)
    path = rec.get_file_path(column)
    path = os.path.join(cfg.host, os.path.dirname(base), path)

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
    engine = get_engine(cfg, base)
    dbo = Database(engine, base, cfg.uid)
    if not hasattr(app.state, 'cnxn'):
        app.state.cnxn = dict()
    if base not in app.state.cnxn:
        app.state.cnxn[base] = engine.connect()
    limit = 0 if not limit else int(limit)
    result = dbo.query_result(sql, limit, app.state.cnxn[base])

    return {'result': result}


@app.get("/{full_path:path}")
async def capture_routes(request: Request, full_path: str):
    path_parts = full_path.split('/')
    filepath = os.path.join(cfg.host, '/'.join(path_parts[1:]))
    type = ''
    if os.path.isfile(filepath):
        type = magic.from_file(filepath, mime=True)
    name = os.path.basename(filepath)
    if type.startswith('image/') or type == 'application/pdf':
        headers = {
            "Content-Type": type,
            "Content-Disposition": "inline"
        }
        return FileResponse(filepath, media_type=type, filename=name,
                            headers=headers)

    return templates.TemplateResponse("urd.html", {
        "request": request, "v": mod, "base": cfg.database
    })


def main(host: str = 'localhost', port: int = 8000):
    uvicorn.run(
        app,
        host=host,
        port=port,
    )


if __name__ == "__main__":
    typer.run(main)
