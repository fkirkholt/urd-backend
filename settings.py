from pydantic_settings import BaseSettings
from ruamel.yaml import YAML
from pathlib import Path
from addict import Dict


yaml = YAML()

with open(Path("drivers.yml"), "r") as content:
    _drivers = yaml.load(content)

_local_drivers = {}
if Path('drivers.local.yml').exists():
    with open(Path('drivers.local.yml'), 'r') as content:
        _local_drivers = yaml.load(content)

    _drivers.update(_local_drivers)

drivers = Dict(_drivers)


class Settings(BaseSettings):
    secret_key: str = "some_secret_key"
    timeout: int = 30 * 60  # 30 minutes
    cnxn: str | None = None
    system: str | None = None
    host: str | None = None
    subfolders: list = []
    database: str | None = None
    uid: str | None = None
    pwd: str | None = None
    driver: str | None = None
    max_connections: int = 10
    norwegian_chars: bool = False
    exportdir: str | None = None
    websocket: str | None = None
    # Filetypes that should be checked with LSP over websocket
    lsp_filetypes: str = ''  # bar delimited: .py|.js

    class Config:
        env_prefix = 'urdr_'
