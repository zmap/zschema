import json
import pprint

from zschema.leaves import *
from zschema.compounds import *

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

#print String().to_bigquery("fuck")

#host.print_indent_string()

#print "\n=====\n"

#print heartbleed.to_bigquery("noname")
print "== Google Big Query == "
print host.to_bigquery()
print "\n\n"
print "== Elastic Search == "
print host.to_es("ipv4")



print "== checking validation of good record == "
test = {
    "ipstr":"141.212.120.1",
    "ip":2379511809,
    "443":{
        "tls":"test"
    }
}
host.validate(test)


print "== checking invalid key == "
test = {
    "keydne":"141.212.120.1asdf",
    "ip":2379511809,
    "443":{
        "tls":"test"
    }
}
host.validate(test)


print "== checking wrong type of value == "
test = {
    "ipstr":"141.212.120.1asdf",
    "ip":2379511809,
    "443":{
        "tls":"test"
    }
}
host.validate(test)
