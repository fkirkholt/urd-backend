from fastapi import FastAPI, Request, Response, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseSettings
import io
import urllib.parse
from schema import Schema
from database import Database, Connection
from table import Table, Grid
from record import Record
from column import Column
import json
import os
from addict import Dict
from jose import jwt
import time

class Settings(BaseSettings):
    secret_key: str = "some_secret_key"
    timeout   : int = 30 * 60 # 30 minutes
    db_system : str = None
    db_server : str = None
    db_name   : str = None
    db_uid    : str = None
    db_pwd    : str = None

cfg = Settings()

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="static/html")
mod = os.path.getmtime("static/js/bundle.js")

@app.middleware("http")
async def check_login(request: Request, call_next):
    session: str = request.cookies.get("session")
    now = time.time()
    if session:
        payload = jwt.decode(session, cfg.secret_key)
        cfg.db_system = payload["system"]
        cfg.db_server = payload["server"]
        cfg.db_uid = payload["uid"]
        cfg.db_pwd = payload["pwd"]
        cfg.db_name = payload["database"]
    elif (request.url.path not in ("/login", "/") and not request.url.path.startswith('/static')):
        return JSONResponse(content={
            "message": "login"
        }, status_code=401)

    response = await call_next(request)
    if cfg.db_uid is not None and request.url.path != "/logout":
        token = jwt.encode({
            "system": cfg.db_system,
            "server": cfg.db_server,
            "uid": cfg.db_uid,
            "pwd": cfg.db_pwd,
            "database": cfg.db_name,
            "timestamp": now
        }, cfg.secret_key)
        response.set_cookie(key="session", value=token, expires=cfg.timeout)
    return response

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("urd.html", {
        "request": request, "v": mod, "base": cfg.db_name
    })

@app.post("/login")
def login(response: Response, system: str, server: str, username: str, password: str, database: str):
    cfg.db_system = system
    cfg.db_uid = username
    cfg.db_pwd = password
    cfg.db_name = database
    cfg.db_server = server or 'localhost'

    timestamp = time.time()
    token = jwt.encode({
        "system": system,
        "server": server,
        "database": database,
        "uid": username,
        "pwd": password,
        "timestamp": timestamp
    }, cfg.secret_key)
    cfg.timeout = None if cfg.db_system == 'sqlite3' else cfg.timeout
    response.set_cookie(key="session", value=token, expires=cfg.timeout)
    return {"success": True}

@app.get("/logout")
def logout(response: Response):
    response.delete_cookie("session")
    cfg.db_system = None
    cfg.db_server = None
    cfg.db_name = None
    cfg.db_uid = None
    cfg.db_pwd = None
    return {"success": True}

@app.get("/dblist")
def dblist():
    result = []
    if cfg.db_system == 'sqlite3':
        file_list = os.listdir(cfg.db_server)
        for filename in file_list:
            if os.path.splitext(filename)[1] not in ('.db', '.sqlite3'):
                continue
            base = Dict()
            base.columns.name = filename
            base.columns.label = filename.capitalize()
            base.columns.description = None
            result.append(base)
    else:
        cnxn = Connection(cfg)
        dbnames = cnxn.get_databases()
        for dbname in dbnames:
            cnxn = Connection(cfg, dbname)
            dbo = Database(cnxn, dbname)
            base = Dict()
            base.columns.name = dbname
            base.columns.label = dbo.metadata.label or dbname.capitalize()
            base.columns.description = dbo.metadata.description or None
            result.append(base)
    return {'data': {'records': result}}

@app.get("/database")
def db_info(base: str):
    cnxn = Connection(cfg, base)
    dbo = Database(cnxn, base)
    info = dbo.get_info()

    return {'data': info}

@app.get("/table")
async def get_table(request: Request):
    req = Dict({item[0]: item[1]
                for item in request.query_params.multi_items()})
    cnxn = Connection(cfg, req.base)
    schema = req.get('schema', None)
    if cnxn.system == 'postgres' and schema:
        base_path = req.base + '.' + req.schema
    else:
        base_path = req.base or schema
    dbo = Database(cnxn, base_path)
    table = Table(dbo, req.table)
    grid = Grid(table)
    table.limit  = int(req.get('limit', 30))
    table.offset = int(req.get('offset', 0))
    if req.get('filter', None):
        grid.set_search_cond(req['filter'])
    if req.get('sort', None):
        grid.sort_columns = json.loads(req.sort)

    # todo: handle sort
    pkey_vals = None
    if ('prim_key' in req and req.prim_key):
        pkey_vals = json.loads(req.prim_key)
    return {'data': grid.get(pkey_vals)}

@app.get("/record")
def get_record(base: str, table: str, primary_key: str, schema: str = None):
    cnxn = Connection(cfg, base)
    if cnxn.system == 'postgres' and schema:
        base_path = base + '.' + schema
    else:
        base_path = base or schema
    dbo = Database(cnxn, base_path)
    tbl = Table(dbo, table)
    pk = json.loads(primary_key)
    record = Record(dbo, tbl, pk)
    return {'data': record.get()}

@app.get("/children")
def get_children(base: str, table: str, primary_key: str):
    cnxn = Connection(cfg, base)
    base_path = base or schema
    dbo = Database(cnxn, base_path)
    tbl = Table(dbo, table)
    tbl.offset = 0
    tbl.limit = 30
    pk = json.loads(primary_key)
    record = Record(dbo, tbl, pk)
    return {'data': record.get_children()}

@app.get("/relations")
def get_relations(base: str, table: str, primary_key: str, count: bool, alias: str = None, types: str = None):
    cnxn = Connection(cfg, base)
    dbo = Database(cnxn, base)
    tbl = Table(dbo, table)
    pk = json.loads(primary_key)
    if types:
        types = json.loads(types)
    record = Record(dbo, tbl, pk)
    if count:
        return {'data': record.get_relation_count(types)}
    else:
        relation = record.get_relation(alias)
        return {'data': {alias: relation}}

@app.put("/table")
async def save_table(request: Request):
    req = await request.json()
    base = req['base_name']
    cnxn = Connection(cfg, base)
    dbo = Database(cnxn, base)
    tbl = Table(dbo, req['table_name'])
    return {'data': tbl.save(req['records'])}

@app.get("/select")
async def get_select(request: Request):
    # todo: skal ikke behøve alias
    req = Dict({item[0]: item[1]
                for item in request.query_params.multi_items()})
    cnxn = Connection(cfg, req.base)
    dbo = Database(cnxn, req.base)
    tbl = Table(dbo, req.table)
    if 'key' in req:
        key = json.loads(req.key)
        colname = key[-1]
    else:
        colname = self.get_primary_key()[-1]
    col = Column(tbl, colname)
    data = col.get_select(req)
    return data

@app.get('/urd/dialog_schema', response_class=HTMLResponse)
def dialog_schema(request: Request):
    return templates.TemplateResponse("update_schema.htm", {
        "request": request
    })

@app.put('/urd/update_schema')
async def update_schema(request: Request):
    req = await request.json()
    base = req['base']
    config = Dict(json.loads(req['config']))
    cnxn = Connection(cfg, base)
    dbo = Database(cnxn, base)
    schema_name = dbo.schema
    schema = Schema(schema_name)
    schema.update(dbo, config)

    return {'sucess': True, 'msg': "Cache oppdatert"}

@app.get('/table_sql')
def export_sql(base: str, table: str, dialect: str):
    # Fiks alle slike connections
    cnxn = Connection(cfg, base)
    dbo = Database(cnxn, base)
    table = Table(dbo, table)
    ddl = table.export_ddl(dialect)
    response = StreamingResponse(io.StringIO(ddl), media_type="txt/plain")
    response.headers["Content-Disposition"] = f"attachment; filename={table.name}.sql"

    return response

@app.get('/file')
def get_file(base: str, table: str, primary_key: str):
    pkey = json.loads(urllib.parse.unquote(primary_key))
    cnxn = Connection(cfg, base)
    dbo = Database(cnxn, base)
    tbl = Table(dbo, table)
    rec = Record(dbo, tbl, pkey)
    path = rec.get_file_path()
    os.chdir(cfg.db_server)

    return FileResponse(path)
