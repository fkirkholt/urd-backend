import uvicorn
from litestar import Litestar, get, post, Request, Response
from litestar.response import Template, File
from litestar.contrib.jinja import JinjaTemplateEngine
from litestar.template.config import TemplateConfig
from litestar.types import Scope, Receive, Send
from litestar.static_files import create_static_files_router
from litestar.logging import LoggingConfig
from litestar.datastructures import Cookie, State
from litestar.status_codes import HTTP_401_UNAUTHORIZED
from settings import drivers, Settings
import os
from jose import jwt
import time
import magic
from starlette.background import BackgroundTask
from contextlib import asynccontextmanager
import typer
from controllers.file import File_Controller
from controllers.user import User_Controller
from controllers.database import Database_Controller


cfg = Settings()
cfg_default = Settings()

mod = os.path.getmtime("static/js/dist/index.js")


@asynccontextmanager
async def db_lifespan(app: Litestar):
    app.state.cnxn_registry = {}
    try:
        yield
    finally:
        # Cleanup: Close all active connections on shutdown
        for cnxn in app.state.cnxn_registry.values():
            cnxn.close()


# Log errors to console
logging_config = LoggingConfig(
    log_exceptions="always",
)


def cleanup(temp_file):
    os.remove(temp_file)


def token():
    return jwt.encode({
        "system": cfg.system,
        "server": cfg.host,
        "uid": cfg.uid,
        "pwd": cfg.pwd,
        "database": cfg.database,
        "driver": cfg.driver,
        "timestamp": time.time()
    }, cfg.secret_key)


def login_middleware(app):

    async def check_login(scope: Scope, receive: Receive, send: Send):
        request = Request(scope)
        response = Response(content={"message": "OK"})
        session = None
        path_parts = scope['path'].split('/')
        print('receive', receive)
        if 'cnxn' in request.query_params:
            session: str = request.cookies.get(request.query_params.get('cnxn'))
        elif len(path_parts) > 1:
            session: str = request.cookies.get(path_parts[1])

        if session and scope['path'] != '/login':
            payload = jwt.decode(session, cfg.secret_key)
            cfg.system = payload["system"]
            cfg.host = payload["server"]
            cfg.uid = payload["uid"]
            cfg.pwd = payload["pwd"]
            cfg.database = payload["database"]
            cfg.driver = payload["driver"]
        elif (
            cfg.system not in ('sqlite', 'duckdb') and
            scope['path'] not in ("/login", "/", "/drivers") and
            not scope['path'].startswith('/static')
        ):
            response = Response(
                content={
                    "message": "login",
                    "detail": {
                        'system': cfg.system,
                        'host': cfg.host,
                        'database': cfg.database
                    }
                },
                status_code=HTTP_401_UNAUTHORIZED,
                media_type="application/json"
            )
            asgi_response = response.to_asgi_response(app, request)
            await asgi_response(scope, receive, send)
            return

        await app(scope, receive, send)

    return check_login


@get("/", sync_to_thread=True)
def home(request: Request) -> Template:
    return Template(template_name="urd.html", context={
        "request": request, "v": mod, "base": cfg.database
    })


@get("/drivers", sync_to_thread=True)
def get_drivers(system: str) -> dict:
    return drivers[system]


@post("/login", sync_to_thread=True)
def login(cnxn: str, system: str, server: str, driver: str,
          database: str | None = None, username: str | None = None,
          password: str | None = None) -> Response[dict]:
    cfg.cnxn = cnxn
    cfg.system = system or cfg.system
    cfg.uid = username
    cfg.pwd = password
    cfg.database = database or cfg.database
    cfg.host = server or cfg.host
    cfg.driver = driver

    if cfg.system == 'sqlite' and cfg.database != 'urdr':
        cfg.timeout = None

    return Response(
        content={"success": True, },
        cookies=[Cookie(key=cnxn, value=token(), expires=cfg.timeout)]
    )


@get("/logout", sync_to_thread=True)
def logout(send: Send) -> dict:
    cnxn = {
        'system': cfg_default.system,
        'host': cfg_default.host,
        'database': cfg_default.database
    }
    return {'success': True, 'cnxn': cnxn}

@get('/urd/dialog_cache', sync_to_thread=True)
def dialog_cache(request: Request) -> Template:
    return Template(template_name="update_cache.htm", context={
        "request": request
    })



@get('/download', sync_to_thread=True)
def download_file(path: str, media_type: str) -> File:
    filename = os.path.basename(path)
    return File(path, media_type=media_type, filename=filename,
                background=BackgroundTask(cleanup, path))


@get("/{full_path:path}")
async def capture_routes(request: Request, full_path: str) -> Template:
    cfg = request.app.state.cfg
    path_parts = full_path.split('/')
    filepath = os.path.join(cfg.host, '/'.join(path_parts[1:]))
    type = ''

    if os.path.isfile(filepath):
        if '.wasm' in filepath:
            type = 'application/wasm'
        else:
            type = magic.from_file(filepath, mime=True)
    name = os.path.basename(filepath)
    if type.startswith('image/') or type == 'application/pdf':
        return File(path=filepath, media_type=type, filename=name,
                    content_disposition_type="inline")

    return Template(template_name="urd.html", context={
        "request": request, "v": mod, "base": cfg.database
    })

def main(host: str = 'localhost', port: int = 8000):
    uvicorn.run(
        app,
        host=host,
        port=port,
    )

app = Litestar(
    route_handlers=[
        home, get_drivers, login, logout, dialog_cache, download_file, capture_routes,
        File_Controller, Database_Controller, User_Controller,
        create_static_files_router(path="/static", directories=["static"]),
    ],
    template_config=TemplateConfig(
        directory="static/html",
        engine=JinjaTemplateEngine,
    ),
    logging_config=logging_config,
    state=State({'cfg': cfg, 'drivers': drivers, 'cnxn_registry': {}}),
    middleware=[login_middleware],
    lifespan=[db_lifespan]
)


if __name__ == "__main__":
    typer.run(main)
