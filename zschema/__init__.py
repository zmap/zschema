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

__version__ = "0.0.14"

__schemas = {}

def register_schema(name, schema):
    __schemas[name] = schema

def get_schema(name):
    return __schemas[name]
