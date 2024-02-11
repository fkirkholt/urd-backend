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
from sqlalchemy import create_engine, text
from settings import Settings
from database import Database
from table import Table, Grid
from record import Record
from field import Field
import json
import os
from addict import Dict
from jose import jwt
import time
import xattr
from user import User


cfg = Settings()

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="static/html")
mod = os.path.getmtime("static/js/dist/bundle.js")


def get_engine(cfg, db_name=None):
    # driver = cfg.driver[cfg.db_system]
    if cfg.system != 'duckdb':
        driver = getattr(cfg, f'{cfg.system}_driver')

    if cfg.system == 'duckdb':
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
            "message": "login"
        }, status_code=401)

    response = await call_next(request)
    if cfg.uid is not None and request.url.path != "/logout":
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

    cfg.timeout = None if cfg.system == 'sqlite' else cfg.timeout
    response.set_cookie(key="session", value=token(), expires=cfg.timeout)

    return {"success": True}


@app.get("/logout")
def logout(response: Response):
    response.delete_cookie("session")
    cfg.system = None
    cfg.host = None
    cfg.database = None
    cfg.uid = None
    cfg.pwd = None
    return {"success": True}


@app.get("/dblist")
def dblist(role: str = None):
    result = []
    useradmin = False
    if cfg.system in ('sqlite', 'duckdb'):
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
            with engine.connect() as conn:
                conn.execute(text('set default role ' + role))
        elif cfg.system in ['mysql', 'mariadb']:
            sql = 'select current_role()'
            with engine.connect() as conn:
                rows = conn.execute(text(sql)).fetchall()
                role = (None if len(rows) == 0 else rows[0][0]
                        if len(rows) == 1 else 'ALL')

                if role:
                    conn.execute(text('set role ' + role))

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
                rows = cnxn.execute(text('show grants')).fetchall()
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
            rows = cnxn.execute(text(sql)).fetchall()

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
            rows = cnxn.execute(text(sql)).fetchall()

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
        cnxn.execute(text(sql))
        cnxn.commit()


@app.put("/change_password")
def change_password(old_pwd: str, new_pwd: str):
    if old_pwd != cfg.pwd:
        return {'data': 'Feil passord'}
    elif cfg.system in ['mysql', 'mariadb']:
        cfg2 = Settings()
        if None in [cfg2.system, cfg2.host, cfg2.uid, cfg2.pwd]:
            return {'data': 'Påloggingsdata mangler. Kontakt administrator.'}
        engine = get_engine(cfg2)
        with engine.connect() as cnxn:
            sql = f"alter user {cfg.uid}@{cfg.host} identified by '{new_pwd}'"
            cnxn.execute(text(sql))
            cnxn.commit()

        return {'data': 'Passord endret'}
    else:
        return {'data': 'Ikke implementert for denne databaseplattformen'}


@app.put("/create_user")
def create_user(name: str, pwd: str):
    engine = get_engine(cfg)
    if cfg.system in ['mysql', 'mariadb']:
        sql = f"create user '{name}'@'{cfg.host}' identified by '{pwd}'"
        with engine.connect() as cnxn:
            cnxn.execute(text(sql))
            cnxn.commit()

        return userlist()


@app.get("/database")
def db_info(base: str):
    engine = get_engine(cfg, base)
    dbo = Database(engine, base)
    info = dbo.get_info()

    return {'data': info}


@app.get("/table")
async def get_table(request: Request):
    req = Dict({item[0]: item[1]
                for item in request.query_params.multi_items()})
    engine = get_engine(cfg, req.base)
    schema = req.get('schema', None)
    if cfg.system == 'postgresql' and schema:
        base_path = req.base + '.' + req.schema
    else:
        base_path = req.base or schema
    dbo = Database(engine, base_path)
    table = Table(dbo, req.table)
    grid = Grid(table)
    table.limit = int(req.get('limit', 30))
    table.offset = int(req.get('offset', 0))

    if req.get('filter', None):
        req['filter'] = urllib.parse.unquote(req['filter'])
        if req['filter'].startswith('where '):
            where = req['filter'][6:].split(';')[0]
            grid.cond.prep_stmnts.append(where)
        else:
            grid.set_search_cond(req['filter'])

    if req.get('sort', None):
        grid.sort_columns = json.loads(req.sort)

    # todo: handle sort
    pkey_vals = None
    if ('prim_key' in req and req.prim_key):
        pkey_vals = json.loads(req.prim_key)
    return {'data': grid.get(pkey_vals)}


@app.get("/record")
def get_record(base: str, table: str, pkey: str, schema: str = None):
    engine = get_engine(cfg, base)
    if cfg.system == 'postgresql' and schema:
        base_path = base + '.' + schema
    else:
        base_path = base or schema
    dbo = Database(engine, base_path)
    tbl = Table(dbo, table)
    pk = json.loads(pkey)
    record = Record(dbo, tbl, pk)
    return {'data': record.get()}


@app.get("/children")
def get_children(base: str, table: str, pkey: str):
    engine = get_engine(cfg, base)
    dbo = Database(engine, base)
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
    dbo = Database(engine, base)
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
    dbo = Database(engine, base)
    tbl = Table(dbo, req['table_name'])
    return {'data': tbl.save(req['records'])}


@app.get("/options")
async def get_options(request: Request):
    req = Dict({item[0]: item[1]
                for item in request.query_params.multi_items()})
    engine = get_engine(cfg, req.base)
    dbo = Database(engine, req.base)
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
    # Get condition defining classification relations
    params = {}
    if fkey:
        cond2, params = fld.get_condition()
        if cond2:
            cond = cond + ' and ' + cond2

    data = fld.get_options(cond, params)
    return data


@app.get('/urd/dialog_cache', response_class=HTMLResponse)
def dialog_cache(request: Request):
    return templates.TemplateResponse("update_cache.htm", {
        "request": request
    })


@app.put('/urd/update_cache')
async def update_cache(base: str, config: str):
    engine = get_engine(cfg, base)
    dbo = Database(engine, base)
    dbo.config = Dict(json.loads(config))
    dbo.config.update_cache = True
    dbo.get_tables()
    dbo.get_contents()

    return {'sucess': True, 'msg': "Cache oppdatert"}


@app.get('/table_sql')
def export_sql(base: str, dialect: str, include_recs: bool, select_recs: bool,
               table: str = None):
    # Fiks alle slike connections
    engine = get_engine(cfg, base)
    dbo = Database(engine, base)
    if table:
        table = Table(dbo, table)
        ddl = table.export_ddl(dialect)
        if include_recs:
            ddl += table.export_records(select_recs)
        filename = table.name
    else:
        ddl = dbo.export_as_sql(dialect, include_recs, select_recs)
        filename = base + '.' + dialect
    response = StreamingResponse(io.StringIO(ddl), media_type="txt/plain")
    response.headers["Content-Disposition"] = \
        f"attachment; filename={filename}.sql"

    return response


@app.get('/table_csv')
def export_csv(base: str, table: str, fields: str):
    engine = get_engine(cfg, base)
    dbo = Database(engine, base)
    table = Table(dbo, table)
    table.offset = 0
    table.limit = None
    columns = json.loads(urllib.parse.unquote(fields))
    csv = table.get_csv(columns)
    response = StreamingResponse(io.StringIO(csv), media_type="txt/csv")
    response.headers['Content-Disposition'] = \
        f'attachment; filename={table.name}.csv'

    return response


@app.get('/file')
def get_file(base: str, table: str, pkey: str):
    pkey = json.loads(urllib.parse.unquote(pkey))
    engine = get_engine(cfg, base)
    dbo = Database(engine, base)
    tbl = Table(dbo, table)
    rec = Record(dbo, tbl, pkey)
    path = rec.get_file_path()
    path = os.path.join(cfg.host, path)

    return FileResponse(path)


@app.post('/convert')
def convert(base: str, table: str, from_format: str, to_format: str,
            fields: str):
    fields = json.loads(fields)
    engine = get_engine(cfg, base)
    dbo = Database(engine, base)
    tbl = Table(dbo, table)
    for field_name in fields:
        result = tbl.convert(field_name, from_format, to_format)

    return {'result': result}


@app.get('/query')
def query(base: str, sql: str, limit: str):
    print('sql', sql)
    engine = get_engine(cfg, base)
    dbo = Database(engine, base)
    limit = 0 if not limit else int(limit)
    result = dbo.query_result(sql, limit)

    return {'result': result}


if __name__ == '__main__':
    uvicorn.run(
        app,
        host='localhost',
        port=8000,
    )
