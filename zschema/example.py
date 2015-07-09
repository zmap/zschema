from leaves import *
from compounds import *


heartbleed = SubRecord({
    "heartbeat_support":Boolean(),
    "heartbleed_vulnerable":Boolean(),
    "timestamp":DateTime()
})

host = Record({
        "ipstr":IPv4Address(required=True),
        "ip":Long(),
        Port(443):SubRecord({
            "tls":String(),
            "heartbleed":heartbleed
        }),
        "tags":ListOf(String())
})
