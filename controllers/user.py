import hashlib
from litestar import Controller, get, put, Request
from addict import Dict
from util import to_rec
from settings import Settings
from models.engine import get_engine
from models.expression import Expression
from models.user import User


class User_Controller(Controller):

    @get("/userlist", sync_to_thread=True)
    def userlist(self, request: Request) -> dict:
        cfg = request.app.state.cfg
        users = []
        roles = []
        engine = get_engine(cfg)
        cnxn = engine.connect()
        with cnxn.cursor() as crsr:
            if engine.name in ['mysql', 'mariadb']:
                sql = """
                select cast(user as char) as name, cast(Host as char) as host
                from mysql.user
                where host not in ('%', '')
                  and user not in ('PUBLIC', 'root', 'mariadb.sys', '')
                order by user
                """
                crsr.execute(sql)
                rows = crsr.fetchall()
                print('rows', rows)

                for row in rows:
                    rec = to_rec(row, crsr)
                    user = Dict()
                    user.name = rec.name
                    user.host = rec.host
                    users.append(user)
                print('users', users)

                sql = """
                select user as name
                from mysql.user
                where host in ('%', '')
                  and not length(authentication_string)
                  and user != 'PUBLIC'
                """
                crsr.execute(sql)
                rows = crsr.fetchall()

                for row in rows:
                    rec = to_rec(row, crsr)
                    roles.append(rec.name)

        cnxn.close()
        return {'data': {'users': users, 'roles': roles}}


    @get("/user_roles", sync_to_thread=True)
    def user_roles(self, request: Request, user: str, host: str) -> dict:
        cfg = request.app.state.cfg
        engine = get_engine(cfg)
        user = User(engine, user)
        return {'data': user.roles}


    @put("/change_user_role", sync_to_thread=True)
    def change_role(self, request: Request, user: str, host: str, role: str,
                    grant: bool) -> None:
        cfg = request.app.state.cfg
        engine = get_engine(cfg)
        cnxn = engine.connect()
        if grant:
            sql = f'grant {role} to {user}@{host}'
        else:
            sql = f'revoke {role} from {user}@{host}'
        with cnxn.cursor() as crsr:
            crsr.execute(sql)
        cnxn.commit()
        cnxn.close()


    @put("/change_password", sync_to_thread=True)
    def change_password(self, request: Request, base: str, old_pwd: str,
                        new_pwd: str) -> dict:
        cfg = request.app.state.cfg
        if old_pwd != cfg.pwd:
            return {'data': 'Feil passord'}
        elif cfg.system in ['mysql', 'mariadb']:
            cfg2 = Settings()
            if None in [cfg2.system, cfg2.host, cfg2.uid, cfg2.pwd]:
                return {'data': 'Påloggingsdata mangler. Kontakt administrator.'}
            engine = get_engine(cfg2)
            cnxn = engine.connect()
            with cnxn.cursor() as crsr:
                sql = f"alter user {cfg.uid}@{cfg.host} identified by '{new_pwd}'"
                crsr.execute(sql)
            cnxn.commit()
            cnxn.close()

            return {'data': 'Passord endret'}
        elif cfg.system == 'sqlite' and cfg.database == 'urdr':
            engine = get_engine(cfg, base)
            cnxn = engine.connect()
            db_path = engine.url.database
            urdr = 'main' if db_path.endswith('/urdr.db') else 'urdr'
            sql = f"update {urdr}.user set password = :pwd where id = :uid"
            pwd = hashlib.sha256(new_pwd.encode('utf-8')).hexdigest()
            with cnxn.cursor() as crsr:
                expr = Expression(engine)
                sql, params = expr.prepare(sql, {'uid': cfg.uid, 'pwd': pwd})
                crsr.execute(sql, params)
            cnxn.commit()
            cnxn.close()
            return {'data': 'Passord endret'}
        else:
            return {'data': 'Ikke implementert for denne databaseplattformen'}


    @put("/create_user", sync_to_thread=True)
    def create_user(self, request: Request, name: str, pwd: str) -> dict:
        cfg = request.app.state.cfg
        engine = get_engine(cfg)
        if cfg.system in ['mysql', 'mariadb']:
            cnxn = engine.connect()
            sql = f"create user '{name}'@'{cfg.host}' identified by '{pwd}'"
            with cnxn.cursor() as crsr:
                crsr.execute(sql)
            cnxn.commit()
            cnxn.close()
            return self.userlist()


