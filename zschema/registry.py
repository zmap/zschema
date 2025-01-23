from __future__ import print_function
from builtins import int, str

from .compounds import Record

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


def __register(self, name):
    register_schema(name, self)
    return self


Record.register = __register
