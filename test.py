import json
import pprint

from leaves import *
from compounds import *

heartbleed = SubRecord({
	"heartbeat_support":Boolean(),
	"heartbleed_vulnerable":Boolean(),
	"timestamp":DateTime()
})

host = Record({
		"ipstr":IPv4Address(required=True),
		"ip":Integer(),
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
print host.to_bigquery()