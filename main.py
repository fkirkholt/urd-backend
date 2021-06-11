from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from schema import Schema
from database import Database
from table import Table
from record import Record
import json
import os
from config import config
from addict import Dict

app = FastAPI()

# todo: Legg dette en annen plass
# cnxnstr = 'Driver={PostgreSQL Unicdaode};Server=localhost;Port=5432;Database=urd;Uid=urd;Pwd=urd;'

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="static/html")
mod = os.path.getmtime("static/js/bundle.js")

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("urd.html", {
        "request": request, "v": mod, "base": config['db']['name']
    })

@app.get("/database")
def db_info(base: str):
    db = Database(base)
    info = db.get_info()
    json.dumps(info)
    return {'data': info}

@app.get("/table")
def get_table(base: str, table: str, schema: str = None, sort: str = None, limit: int = 30, offset: int = 0, filter: str = None):
    db = Database(base or schema)
    table = Table(db, table)
    table.limit  = limit
    table.offset = offset
    if filter:
        table.set_search_cond(filter)
    # todo: handle sort
    return {'data': table.get_grid()}

@app.get("/record")
def get_record(base: str, table: str, primary_key: str, schema: str = None):
    db = Database(base or schema)
    tbl = Table(db, table)
    pk = json.loads(primary_key)
    record = Record(db, tbl, pk)
    return {'data': record.get()}

@app.get("/relations")
def get_relations(base: str, table: str, primary_key: str, count: bool, alias: str = None, types: str = None):
    db = Database(base)
    tbl = Table(db, table)
    pk = json.loads(primary_key)
    if types:
        types = json.loads(types)
    record = Record(db, tbl, pk)
    if count:
        return {'data': record.get_relation_count(types)}
    else:
        relation = record.get_relation(alias)
        return {'data': {alias: relation}}

@app.put("/table")
async def save_table(request: Request):
    req = await request.json()
    base = req['base_name']
    db = Database(base)
    tbl = Table(db, req['table_name'])
    return {'data': tbl.save(req['records'])}

@app.get("/select")
async def get_select(request: Request):
    # todo: skal ikke behøve alias
    req = Dict({item[0]: item[1]
                for item in request.query_params.multi_items()})
    # print(request_query_params)
    db = Database(req.base)
    tbl = Table(db, req.table)
    data = tbl.get_select(req)
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
    db = Database(base)
    schema_name = db.schema
    schema = Schema(schema_name)
    schema.update(db, config)

    # return {'sucess': False}
