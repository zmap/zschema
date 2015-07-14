from zschema.keys import Port, DataValidationException
from zschema.leaves import (
    DateTime,
    AnalyzedString,
    String,
    Binary,
    IndexedBinary,
    Boolean,
    Double,
    Float,
    Long,
    Short,
    Byte,
    Integer,
    IPv4Address
)
from zschema.compounds import ListOf, SubRecord, Record

__version__ = "0.0.9"

#__all__ = ["keys", "leaves", "compounds", "tests"]

__schemas = {}

def register_schema(name, schema):
    if name in __schemas:
        raise Exception("Name conflict. %s already registered in global namespace." % name)
    else:
        __schemas[name] = schema

def get_schema(name):
    return __schemas[name]
