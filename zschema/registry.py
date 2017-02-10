try:
    __zschema_schemas
except NameError:
    __zschema_schemas = {}

def register_schema(name, schema):
    global __zschema_schemas
    __zschema_schemas[name] = schema

def get_schema(name):
    global __zschema_schemas
    return __zschema_schemas[name]

def all_schemas():
    global __zschema_schemas
    return __zschema_schemas.copy()
