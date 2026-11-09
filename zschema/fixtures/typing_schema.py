"""Representative consumer schema; checked without diagnostic suppressions."""

from zschema.compounds import ListOf, NestedListOf, Record, SubRecord, SubRecordType
from zschema.keys import Port
from zschema.leaves import Boolean, Enum, String, Unsigned32BitInteger


class NamedString(String):
    DOC = "Inherited documentation"
    REQUIRED = True


empty = SubRecord({})
address = SubRecordType(
    {"street": String()}, type_name="Address", required=True, doc="An address"
)
tag = String.with_args(doc="A tag", required=True)
schema: Record = Record(
    {
        "name": NamedString(required=False, doc=None),
        "enabled": Boolean(required=True, examples=[True, False]),
        "count": Unsigned32BitInteger(min_value=0, max_value=100, units="items"),
        "state": Enum(values=["on", "off"]),
        "address": address(doc="Home address"),
        "tags": ListOf(tag(required=False), min_items=0, max_items=10),
        "nested": NestedListOf(String(), "value"),
        Port(443): SubRecord(
            {"tls": Boolean()}, extends=empty, allow_unknown=True, es_nested=True
        ),
    },
    doc="Example schema",
    validation_policy="error",
    es_dynamic_policy="strict",
)
schema.validate({}, policy="error", path=["root", 0])
