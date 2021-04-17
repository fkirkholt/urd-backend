import json

class Schema:
    def __init__(self, name):
        with open('schemas/' + name + '/schema.json') as myfile:
            data = myfile.read()

        schema = json.loads(data)

        self.tables = schema["tables"]
        self.reports = schema.get("reports", [])
        self.contents = schema.get("contents", [])
    
