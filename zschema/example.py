from zschema.keys import Port
from zschema.compounds import ListOf, Record, SubRecord
from zschema.leaves import Boolean, DateTime, IPv4Address, String, Unsigned32BitInteger


heartbleed = SubRecord(
    {
        "heartbeat_support": Boolean(),
        "heartbleed_vulnerable": Boolean(),
        "timestamp": DateTime(),
    }
)

host = Record(
    {
        "ipstr": IPv4Address(required=True),
        "ip": Unsigned32BitInteger(),
        Port(443): SubRecord({"tls": String(), "heartbleed": heartbleed}),
        "tags": ListOf(String()),
    }
)
