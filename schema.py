import pyodbc
from addict import Dict

class Schema:
    def __init__(self, name):
        self.name = name

    def update(self, dbo, config):
        config = Dict(config)

        self.tables = dbo.get_tables(config)
        dbo.get_contents(config)
