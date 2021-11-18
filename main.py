from fastapi import FastAPI, Request, Response, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseSettings
import io
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
    timeout   : int = 15 * 60 # 15 minutes
    db_system : str = "postgres"
    db_server : str = "localhost"
    db_name   : str = "postgres"
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
    if session:
        payload = jwt.decode(session, cfg.secret_key)
        now = time.time()
        if 'uid' in payload and (payload["timestamp"] + cfg.timeout) > now :
            cfg.db_uid = payload["uid"]
            cfg.db_pwd = payload["pwd"]
    else:
        cfg.db_uid = None
        cfg.db_pwd = None

    if (cfg.db_uid is None and request.url.path != "/" and request.url.path != "/login" and not request.url.path.startswith('/static')):
        return JSONResponse(content={
            "message": "login"
        }, status_code=401)

    response = await call_next(request)
    if cfg.db_uid is not None and request.url.path != "/logout":
        token = jwt.encode({"uid": cfg.db_uid, "pwd": cfg.db_pwd, "timestamp": now}, cfg.secret_key)
        response.set_cookie(key="session", value=token, expires=cfg.timeout)
    return response

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("urd.html", {
        "request": request, "v": mod, "base": cfg.db_name
    })

@app.post("/login")
def login(response: Response, brukernavn: str, passord: str):
    Connection(cfg.db_system, cfg.db_server, brukernavn, passord, cfg.db_name)
    timestamp = time.time()
    token = jwt.encode({"uid": brukernavn, "pwd": passord, "timestamp": timestamp}, cfg.secret_key)
    response.set_cookie(key="session", value=token, expires=cfg.timeout)
    return {"success": True}

@app.get("/logout")
def logout(response: Response):
    response.delete_cookie("session")
    return {"success": True}

@app.get("/dblist")
def dblist():
    cnxn = Connection(cfg.db_system, cfg.db_server, cfg.db_uid, cfg.db_pwd, cfg.db_name)
    return {'data': {'records': cnxn.get_databases()}}

@app.get("/database")
def db_info(base: str):
    if base == cfg.db_name:
        #TODO Fix user
        return {'data': {
            'base': {
                'name': cfg.db_name
            },
            'user': {
                'name': 'Admin',
                'id': 'admin',
                "admin": 0
            }
        }}
    cnxn = Connection(cfg.db_system, cfg.db_server, cfg.db_uid, cfg.db_pwd, base)
    dbo = Database(cnxn, base)
    info = dbo.get_info()

    return {'data': info}

@app.get("/table")
async def get_table(request: Request):
    req = Dict({item[0]: item[1]
                for item in request.query_params.multi_items()})
    cnxn = Connection(cfg.db_system, cfg.db_server, cfg.db_uid, cfg.db_pwd, req.base)
    schema = req.get('schema', None)
    if cnxn.system == 'postgres' and schema:
        base_path = req.base + '.' + req.schema
    else:
        base_path = req.base or schema
    dbo = Database(cnxn, base_path)
    table = Table(dbo, req.table)
    grid = Grid(table)
    table.limit  = req.get('limit', 30)
    table.offset = req.get('offset', 0)
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
    cnxn = Connection(cfg.db_system, cfg.db_server, cfg.db_uid, cfg.db_pwd, base) #TODO
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
    cnxn = Connection(cfg.db_system, cfg.db_server, cfg.db_uid, cfg.db_pwd, base) #TODO
    base_path = base or schema
    dbo = Database(cnxn, base_path)
    tbl = Table(dbo, table)
    pk = json.loads(primary_key)
    record = Record(dbo, tbl, pk)
    return {'data': record.get_children()}

@app.get("/relations")
def get_relations(base: str, table: str, primary_key: str, count: bool, alias: str = None, types: str = None):
    cnxn = Connection(cfg.db_system, cfg.db_server, cfg.db_uid, cfg.db_pwd, base) #TODO
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
    cnxn = Connection(cfg.db_system, cfg.db_server, cfg.db_uid, cfg.db_pwd, base) #TODO
    dbo = Database(cnxn, base)
    tbl = Table(dbo, req['table_name'])
    return {'data': tbl.save(req['records'])}

@app.get("/select")
async def get_select(request: Request):
    # todo: skal ikke behøve alias
    req = Dict({item[0]: item[1]
                for item in request.query_params.multi_items()})
    cnxn = Connection(cfg.db_system, cfg.db_server, cfg.db_uid, cfg.db_pwd, req.base) #TODO
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
    config = json.loads(req['config'])
    cnxn = Connection(cfg.db_system, cfg.db_server, cfg.db_uid, cfg.db_pwd, base) #TODO
    dbo = Database(cnxn, base)
    schema_name = dbo.schema
    schema = Schema(schema_name)
    schema.update(dbo, config)

    # return {'sucess': False}

@app.get('/table_sql')
def export_sql(base: str, table: str, dialect: str):
    cnxn = Connection(cfg.db_system, cfg.db_server, cfg.db_uid, cfg.db_pwd, base) #TODO
    dbo = Database(cnxn, base)
    table = Table(dbo, table)
    ddl = table.export_ddl(dialect)
    response = StreamingResponse(io.StringIO(ddl), media_type="txt/plain")
    response.headers["Content-Disposition"] = f"attachment; filename={table.name}.sql"

    return response
