import json
import os
import urllib.parse
import time
import re
import io
import tempfile
from litestar import Controller, get, post, put, delete, Request
from litestar.response import File, Stream
from litestar.exceptions import HTTPException
from starlette import status
from addict import Dict
from models.field import Field
from models.record import Record
from models.engine import get_engine, Connection
from models.database import Database
from models.table import Table, Grid
from models.user import User


class Database_Controller(Controller):
    @get("/dblist", sync_to_thread=True)
    def dblist(self, request: Request, role: str = '') -> dict:
        print('----henter dblist----')
        cfg = request.app.state.cfg
        result = []
        useradmin = False
        if cfg.database == 'urdr':
            engine = get_engine(cfg, 'urdr')
            db_manager = request.app.state.db_manager
            pool = db_manager.get_pool(engine)
            with pool.connection() as db_cnxn:
                user = User(engine, db_cnxn, name=cfg.uid)
                rows = user.databases()

                for row in rows:
                    base = Dict()
                    base.columns.name = row.name
                    base.columns.label = row.name.capitalize()
                    base.columns.description = row.description
                    base.columns.type = 'database'
                    result.append(base)
        else:
            engine = get_engine(cfg)
            db_cnxn = engine.connect()
            if role:
                with db_cnxn.cursor() as crsr:
                    sql = 'set default role ' + role
                    crsr.execute(sql)
            elif cfg.system in ['mysql', 'mariadb']:
                with db_cnxn.cursor() as crsr:
                    sql = 'select current_role()'
                    crsr.execute(sql)
                    rows = crsr.fetchall()
                    role = (None if len(rows) == 0 else rows[0][0]
                            if len(rows) == 1 else 'ALL')

                    if role:
                        sql = 'set role ' + role
                        crsr.execute(sql)
                        db_cnxn.commit()

            user = User(engine, db_cnxn)
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
                with db_cnxn.cursor() as crsr:
                    sql = 'show grants'
                    crsr.execute(sql)
                    rows = crsr.fetchall()
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
            'system': cfg.system,
        }}


    @get("/database", sync_to_thread=True)
    def db_info(self, base: str, request: Request, db_cnxn: Connection) -> dict:
        cfg = request.app.state.cfg
        engine = get_engine(cfg, base)
        dbo = Database(engine, base, cfg.uid, db_cnxn)
        info = dbo.get_info()
        return {'data': info}


    @get("/table")
    async def get_table(
        self, base: str, table: str, request: Request, db_cnxn: Connection,
        limit: int = 30, offset: int = 0, schema: str = '', sort: str = '',
        compressed: bool = False, prim_key: str = '', filter: str = ''
    ) -> dict:
        cfg = request.app.state.cfg
        engine = get_engine(cfg, base)
        if cfg.system == 'postgresql' and schema:
            base_path = base + '.' + schema
        else:
            base_path = base or schema
        dbo = Database(engine, base_path, cfg.uid, db_cnxn)
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


    @post("/record", sync_to_thread=True)
    def create_record(self, base: str, table: str, pkey: str, request: Request,
                      data: str, db_cnxn: Connection) -> dict:
        cfg = request.app.state.cfg
        engine = get_engine(cfg, base)
        dbo = Database(engine, base, cfg.uid, db_cnxn)
        tbl = Table(dbo, table)
        pkey = json.loads(pkey)
        record = Record(dbo, tbl, pkey)
        vals = json.loads(data)
        time.sleep(0.5)
        pkey = record.insert(vals)
        return {'values': pkey}


    @put("/record", sync_to_thread=True)
    def update_record(self, base: str, table: str, pkey: str, request: Request,
                      data: str, db_cnxn: Connection) -> dict:
        cfg = request.app.state.cfg
        engine = get_engine(cfg, base)
        dbo = Database(engine, base, cfg.uid, db_cnxn)
        tbl = Table(dbo, table)
        pkey = json.loads(pkey)
        record = Record(dbo, tbl, pkey)
        vals = json.loads(data)
        time.sleep(0.5)
        return {'result': record.update(vals)}


    @delete("/record", sync_to_thread=True, status_code=200)
    def delete_record(self, base: str, table: str, pkey: str, request: Request,
                      db_cnxn: Connection) -> None:
        cfg = request.app.state.cfg
        engine = get_engine(cfg, base)
        dbo = Database(engine, base, cfg.uid, db_cnxn)
        tbl = Table(dbo, table)
        pkey = json.loads(pkey)
        record = Record(dbo, tbl, pkey)
        return {'result': record.delete()}


    @get("/record", sync_to_thread=True)
    def get_record(self, base: str, table: str, pkey: str,
                   request: Request, db_cnxn: Connection, schema: str = '') -> dict:
        cfg = request.app.state.cfg
        engine = get_engine(cfg, base)
        if cfg.system == 'postgresql' and schema:
            base_path = base + '.' + schema
        else:
            base_path = base or schema
        dbo = Database(engine, base_path, cfg.uid, db_cnxn)
        tbl = Table(dbo, table)
        pk = json.loads(pkey)
        record = Record(dbo, tbl, pk)
        return {'data': record.get()}


    @get("/children", sync_to_thread=True)
    def get_children(self, base: str, table: str, pkey: str, request: Request,
                     db_cnxn: Connection) -> dict:
        cfg = request.app.state.cfg
        engine = get_engine(cfg, base)
        dbo = Database(engine, base, cfg.uid, db_cnxn)
        tbl = Table(dbo, table)
        tbl.offset = 0
        tbl.limit = 30
        pk = json.loads(pkey)
        record = Record(dbo, tbl, pk)
        return {'data': record.get_children()}


    @get("/relations", sync_to_thread=True)
    def get_relations(self, base: str, table: str, pkey: str, count: bool,
                      request: Request, db_cnxn: Connection, alias: str = '') -> dict:
        cfg = request.app.state.cfg
        engine = get_engine(cfg, base)
        dbo = Database(engine, base, cfg.uid, db_cnxn)
        tbl = Table(dbo, table)
        pk = json.loads(pkey)
        record = Record(dbo, tbl, pk)
        if count:
            return {'data': record.get_relation_count()}
        else:
            relation = record.get_relation(alias)
            return {'data': {alias: relation}}


    @put("/table")
    async def save_table(self, request: Request, db_cnxn: Connection) -> dict:
        cfg = request.app.state.cfg
        req = await request.json()
        base = req['base_name']
        engine = get_engine(cfg, base)
        dbo = Database(engine, base, cfg.uid, db_cnxn)
        tbl = Table(dbo, req['table_name'])
        return {'data': tbl.save(req['records'])}


    @get("/options")
    async def get_options(self, request: Request, db_cnxn: Connection) -> list:
        cfg = request.app.state.cfg
        req = Dict({item[0]: item[1]
                    for item in request.query_params.multi_items()})
        engine = get_engine(cfg, req.base)
        dbo = Database(engine, req.base, cfg.uid, db_cnxn)
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


    @get('/db_file', sync_to_thread=True)
    def get_db_file(self, base: str, table: str, pkey: str,
                    request: Request, db_cnxn: Connection, column: str = None) -> File:
        """Download file from file reference in database"""
        cfg = request.app.state.cfg
        pkey = json.loads(urllib.parse.unquote(pkey))
        engine = get_engine(cfg, base)
        dbo = Database(engine, base, cfg.uid, db_cnxn)
        tbl = Table(dbo, table)
        rec = Record(dbo, tbl, pkey)
        path = rec.get_file_path(column)
        path = os.path.join(cfg.host, os.path.dirname(base), path)
        return File(path)


    @post('/convert', sync_to_thread=True)
    def convert(self, base: str, table: str, from_format: str, to_format: str,
                fields: str, request: Request, db_cnxn: Connection) -> dict:
        cfg = request.app.state.cfg
        fields = json.loads(fields)
        engine = get_engine(cfg, base)
        dbo = Database(engine, base, cfg.uid, db_cnxn)
        tbl = Table(dbo, table)
        for field_name in fields:
            result = tbl.convert(field_name, from_format, to_format)
        return {'result': result}


    @get('/export_sql', sync_to_thread=True)
    def export_sql(self, dest: str, base: str, dialect: str, table_defs: bool,
                   no_fkeys: bool, list_recs: bool, data_recs: bool,
                   select_recs: bool, view_as_table: bool, no_empty: bool,
                   view_defs: bool, request: Request, db_cnxn: Connection,
                   table: str | None = None, filter: str | None = None) -> Stream:
        """Create sql for exporting a database

        Parameters:
        dialect: The sql dialect used (mysql, postgresql, sqlite)
        list_recs: If records from lookup tables should be included
        data_recs: If records from data tables should be included
        select_recs: If included records should be selected from
                     existing database
        """

        cfg = request.app.state.cfg
        engine = get_engine(cfg, base)
        dbo = Database(engine, base, cfg.uid, db_cnxn)

        if cfg.system in ['sqlite', 'duckdb'] and dest != 'download':
            dest = os.path.join(cfg.host, dest)

        return Stream(dbo.export_sql(dest, dialect, table_defs, no_fkeys,
                                     list_recs, data_recs, select_recs,
                                     view_as_table, no_empty, view_defs,
                                     table, filter),
                      media_type="text/event-stream")


    @get('/export_tsv', sync_to_thread=True)
    def export_tsv(self, request: Request, db_cnxn: Connection, base: str, tables: str,
                   clobs_as_files: bool, dest: str, limit: int | None = None,
                   columns: str | None = None, folder: str | None = None,
                   filter: str | None = None) -> Stream:
        cfg = request.app.state.cfg
        engine = get_engine(cfg, base)
        dbo = Database(engine, base, cfg.uid, db_cnxn)
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
            if cfg.system in ['sqlite', 'duckdb']:
                dest = os.path.join(cfg.host, dest)
            if not os.path.exists(dest):
                os.makedirs(dest)

        return Stream(dbo.export_tsv(tbls, dest, limit, clobs_as_files,
                                     cols, download, filter),
                      media_type="text/event-stream")


    @get('/import_tsv', sync_to_thread=True)
    def import_tsv(self, base: str, dir: str, request: Request,
                   db_cnxn: Connection) -> Stream:
        cfg = request.app.state.cfg
        engine = get_engine(cfg, base)
        dbo = Database(engine, base, cfg.uid, db_cnxn)
        return Stream(dbo.import_tsv(dir), media_type="text/event-stream")


    @get('/kdrs_xml', sync_to_thread=True)
    def export_kdrs_xml(self, base: str, version: str, descr: str,
                        request: Request, db_cnxn: Connection) -> Stream:
        cfg = request.app.state.cfg
        engine = get_engine(cfg, base)
        dbo = Database(engine, base, cfg.uid, db_cnxn)
        xml = dbo.export_as_kdrs_xml(version, descr)
        response = Stream(io.StringIO(xml), media_type="application/xml")
        response.headers['Content-Disposition'] = \
            f'attachment; filename={dbo.identifier}.xml'
        return response



    @get('/query', sync_to_thread=True)
    def query(self, base: str, sql: str, limit: str, request: Request,
              db_cnxn: Connection) -> dict:
        cfg = request.app.state.cfg
        engine = get_engine(cfg, base)
        dbo = Database(engine, base, cfg.uid, db_cnxn)
        limit = 0 if not limit else int(limit)
        result = dbo.query_result(sql, limit)
        return {'result': result}


    @get('/urd/update_cache')
    async def update_cache(self, base: str, config: str,
                           request: Request, db_cnxn: Connection) -> Stream:
        cfg = request.app.state.cfg
        engine = get_engine(cfg, base)
        dbo = Database(engine, base, cfg.uid, db_cnxn)
        dbo.config = Dict(json.loads(config))
        dbo.config.update_cache = True
        dbo.cache = None
        dbo.tables = Dict()

        def event_stream():
            if ('html_attributes' not in dbo.tablenames):
                dbo.create_html_attributes()
            tbl_count = len(dbo.tablenames)
            i = 0
            for tbl in dbo.refl.tables(dbo.schema).values():
                i += 1
                progress = round(i/tbl_count * 100)
                data = json.dumps({'msg': tbl.name, 'progress': progress})
                yield f"data: {data}\n\n"

                if tbl.name[-5:] == '_view' and tbl.name[:-5] in dbo.tablenames:
                    continue
                if '_fts' in tbl.name:
                    continue

                table = Table(dbo, tbl.name, type=tbl.type, comment=tbl.comment)
                dbo.tables[tbl.name] = table.get()

            dbo.get_contents()
            data = json.dumps({'msg': 'done'})
            yield f"data: {data}\n\n"

        return Stream(event_stream(), media_type="text/event-stream")

