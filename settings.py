from decouple import config
from ruamel.yaml import YAML
from pathlib import Path
from addict import Dict
from dataclasses import dataclass


yaml = YAML()

with open(Path("drivers.yml"), "r") as content:
    _drivers = yaml.load(content)

_local_drivers = {}
if Path('drivers.local.yml').exists():
    with open(Path('drivers.local.yml'), 'r') as content:
        _local_drivers = yaml.load(content)

    _drivers.update(_local_drivers)

drivers = Dict(_drivers)


@dataclass
class Settings:
    secret_key: str = config("URDR_SECRET_KEY", default="some_secret_key")
    timeout: int = config("URDR_TIMEOUT", 30 * 60, cast=int)  # 30 minutes
    cnxn: str | None = config("URDR_CNXN", default=None)
    system: str | None = config("URDR_SYSTEM", default=None)
    host: str | None = config("URDR_HOST", default=None)
    database: str | None = config("URDR_DATABASE", default=None)
    uid: str | None = config("URDR_UID", default=None)
    pwd: str | None = config("URDR_PWD", default=None)
    driver: str | None = config("URDR_DRIVER", default=None)
    max_connections: int = config("URDR_MAX_CONNECTIONS", default=10, cast=int)
    norwegian_chars: bool = config("URDR_NORWEGIAN_CHARS", default=False, cast=bool)
    exportdir: str | None = config("URDR_EXPORTDIR", default=None)
    websocket: str | None = config("URDR_WEBSOCKET", default=None)
    # Filetypes that should be checked with LSP over websocket
    lsp_filetypes: str = config("URDR_LSP_FILETYPES", default='')  # bar delimited: .py|.js
    gguf_model_dir: str = config("GGUF_MODEL_DIR", default="~/.local/share/ai-models/")
    gguf_model: str = config("URDR_GGUF_MODEL", default="paraphrase-multilingual-MiniLM-L12-118M-v2-Q4_K_M.gguf")
    # gguf_model: str = 'bge-m3-Q4_K_M.gguf'
    sqlite_ext_dir: str = config("SQLITE_EXT_DIR", default="~/.local/share/sqlite-extensions")
