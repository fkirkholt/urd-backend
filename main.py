from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from database import Database
from table import Table
from record import Record
import json
import os
from config import config

app = FastAPI()

# todo: Legg dette en annen plass
# cnxnstr = 'Driver={PostgreSQL Unicode};Server=localhost;Port=5432;Database=urd;Uid=urd;Pwd=urd;'

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
    return {'data': db.get_info()}

@app.get("/table")
def get_table(base: str, table: str, sort: str = None):
    db = Database(base)
    table = Table(db, table)
    if sort:
        table.grid['sort_columns'] = json.loads(sort)
    return {'data': table.get_grid()}

@app.get("/record")
def get_record(base: str, table: str, primary_key: str):
    db = Database(base)
    pk = json.loads(primary_key)
    record = Record(db, table, pk)
    return {'data': record.get()}

@app.get("/relations")
def get_relations(base: str, table: str, primary_key: str, count: bool):
    db = Database(base)
    pk = json.loads(primary_key)
    record = Record(db, table, pk)
    return {'data': record.get_relations(count, None)}
