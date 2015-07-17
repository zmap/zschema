__schemas = {}

def register_schema(name, schema):
    __schemas[name] = schema

def get_schema(name):
    return __schemas[name]
