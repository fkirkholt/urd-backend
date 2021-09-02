from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, StreamingResponse
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

class Settings(BaseSettings):
    db_system: str = "postgres"
    db_server: str = "localhost"
    db_name  : str = "postgres"
    db_uid   : str = "urd"
    db_pwd   : str = "urd"

cfg = Settings()

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="static/html")
mod = os.path.getmtime("static/js/bundle.js")

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("urd.html", {
        "request": request, "v": mod, "base": cfg.db_name
    })

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
    if (req.base == cfg.db_name and req.table == 'database_'):
        cnxn = Connection(cfg.db_system, cfg.db_server, cfg.db_uid, cfg.db_pwd, cfg.db_name)
        return {'data': {'records': cnxn.get_databases()}}
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
