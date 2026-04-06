import os
import magic
import re
import xattr
from addict import Dict
from settings import yaml
from subprocess import run
from litestar import Controller, get, post, put, delete, Request

class File_Controller(Controller):

    def ripgrep(self, host: str, path: str, pattern: str):
        dir = os.path.join(host, path) if path else host
        cmd = 'rg ' + pattern +  ' --line-number --color=always --colors=path:none'
        cmd += ' --max-columns=255 --max-columns-preview'
        cmd += '' if any(char.isupper() for char in pattern) else ' -i'
        result = run(cmd, cwd=dir, shell=True, capture_output=True, text=True)
        lines = result.stdout.split('\n')
        files = []
        result = []
        for line in lines:
            if line == '':
                break
            parts = line.split(':', 1)
            file = parts[0]
            if file not in files:
                files.append(file)
                base = Dict()
                base.columns.name = os.path.join(path, file)
                base.columns.label = file
                if len(parts) > 1:
                    desc = parts[1]
                    base.columns.description = desc
                base.columns.type = 'file'
                result.append(base)
            elif len(parts) > 1:
                desc = parts[1]
                base.columns.description += '<br>' + desc

        return result



    @get("/file_list", sync_to_thread=True)
    def file_list(self, request: Request, path: str = '', pattern: str = '') -> dict:
        print('----henter filliste----')
        cfg = request.app.state.cfg
        result = []
        useradmin = False
        if not pattern:
            filepath = os.path.join(cfg.host, path) if path else cfg.host
            title_regex = re.compile(r'^(?:#\s+(?P<h1>.*)|__(?P<bold>.*?)__)')
            if os.path.isfile(filepath):
                dirpath = os.path.dirname(filepath)
            else:
                dirpath = filepath
            for entry in sorted(os.scandir(dirpath), key=lambda e: e.name):
                filename = entry.name
                filepath = entry.path
                if entry.is_symlink():
                    continue
                title = None
                comment = None
                if filename.endswith('.md'):
                    with open(filepath, 'r', encoding='utf-8') as f:
                        chunk = f.readline(50)
                        matches = title_regex.match(chunk)
                        if matches:
                            title = matches.group('h1') or matches.group('bold')
                else:
                    attrs = xattr.xattr(filepath)
                    if 'user.comment' in attrs:
                        comment = attrs.get('user.comment')
                base = Dict()
                base.columns.name = os.path.join(path, filename) if path else filename
                base.columns.label = filename
                base.columns.title = title
                base.columns.description = comment
                base.columns.type = 'file'
                base.columns.size = entry.stat().st_size
                if entry.is_dir():
                    base.columns.type = 'dir'
                elif filename.endswith('.db'):
                    with open(filepath, 'rb') as reader:
                        string = reader.read(12)
                        if b'SQLite' in string:
                            base.columns.type = 'database'
                        elif b'DUCK' in string:
                            base.columns.type = 'database'

                result.append(base)
        else:
            result = self.ripgrep(cfg.host, path, pattern)

        autocomplete = {}
        for filename in os.listdir("autocomplete"):
            if filename[0] == '_':
                continue
            with open("autocomplete/" + filename, "r") as content:
                autocomplete[filename] = yaml.load(content)

        return {'data': {
            'records': result,
            'path': path,
            'useradmin': useradmin,
            'system': cfg.system,
            'autocomplete': autocomplete
        }}

    @get("/file")
    async def get_file(self, request: Request, path: str = '') -> dict:
        cfg = request.app.state.cfg
        print('cfg', cfg)
        print('path', path)
        filepath = os.path.join(cfg.host, path)
        if cfg.system not in ['sqlite', 'duckdb']:
            return {'path': path, 'type': 'server'}
        if os.path.isdir(filepath):
            return {'path': path, 'type': 'dir'}
        if not os.path.isfile(filepath) and not os.path.isdir(filepath):
            return {'path': path, 'type': None}
        size = os.path.getsize(filepath)
        content = None
        msg = None
        type = magic.from_file(filepath, mime=True)
        text_types = ['application/javascript']
        with open(filepath, 'rb') as reader:
            string = reader.read(12)
            if b'SQLite' in string:
                type = 'sqlite'
            elif b'DUCK' in string:
                type = 'duckdb'
        if type.startswith('text/') or type in text_types:
            if size < 100000000:
                with open(filepath, 'r') as file:
                    content = file.read()
            else:
                msg = 'File too large to open'
        name = os.path.basename(filepath)
        lsp = False
        for ext in cfg.lsp_filetypes.split('|'):
            if path.endswith(ext):
                lsp = True

        return {'path': path, 'name': name, 'content': content, 'type': type,
                'msg': msg, 'abspath': filepath if lsp else None,
                'websocket': cfg.websocket if lsp else None}


    @get('/backlinks', sync_to_thread=True)
    def get_backlinks(self, path: str, request: Request) -> list:
        cfg = request.app.state.cfg
        backlinks = []
        filepath = os.path.join(cfg.host, path)
        for path, folders, files in os.walk(cfg.host):
            for filename in files:
                if not filename.endswith('.md'):
                    continue
                relpath = os.path.relpath(filepath, path)
                with open(os.path.join(path, filename), 'r') as file:
                    content = file.read()
                    if '(' + relpath + ')' in content:
                        abspath = os.path.join(path, filename)
                        backlinks.append(os.path.relpath(abspath, os.path.dirname(filepath)))

        return backlinks


    @put("/file_rename", sync_to_thread=True)
    def rename_file(self, src: str, dst: str, request: Request) -> dict:
        cfg = request.app.state.cfg
        src = os.path.join(cfg.host, src)
        dst = os.path.join(cfg.host, dst)
        os.rename(src, dst)
        filepath = os.path.join(cfg.host, src)
        for path, folders, files in os.walk(cfg.host):
            for filename in files:
                if not filename.endswith('.md'):
                    continue
                relpath = os.path.relpath(filepath, path)
                new_content = ''
                with open(os.path.join(path, filename), 'r') as file:
                    content = file.read()
                    if '(' + relpath + ')' in content:
                        new_path = relpath.replace(os.path.basename(relpath),
                                                   os.path.basename(dst))
                        new_content = content.replace('(' + relpath + ')',
                                                      '(' + new_path + ')')
                if new_content:
                    with open(os.path.join(path, filename), 'w') as file:
                        file.write(new_content)
        return {'success': True}


    @put("/file_delete", sync_to_thread=True)
    def delete_file(self, filename: str, request: Request) -> dict:
        cfg = request.app.state.cfg
        filepath = os.path.join(cfg.host, filename)
        os.remove(filepath)
        return {'success': True}


    @post("/file", sync_to_thread=True)
    def update_file(self, path: str, data: str, request: Request) -> dict:
        cfg = request.app.state.cfg
        filepath = os.path.join(cfg.host, path)
        with open(filepath, 'w') as file:
            file.write(data)
        return {'result': 'success'}



