import json
import pprint

from zschema.leaves import *
from zschema.compounds import *

class LeafUnitTests(unittest.TestCase):

    def test_valid(self):
        for leaf in VALID_LEAVES:
            leaf().validate(leaf.__name__, leaf.VALID)

    def test_invalid(self):
        for leaf in VALID_LEAVES:
            try:
                leaf().validate(leaf.__name__, leaf.INVALID)
                raise Exception("invalid value did not fail for %s",
                                leaf.__name__)
            except DataValidationException:
                continue


VALID_ELASTIC_SEARCH = {
    "host": {
        "properties": {
            "443": {
                "properties": {
                    "tls": {
                        "index": "not_analyzed", 
                        "type": "string"
                    }, 
                    "heartbleed": {
                        "properties": {
                            "heartbeat_support": {
                                "type": "boolean"
                            }, 
                            "heartbleed_vulnerable": {
                                "type": "boolean"
                            }, 
                            "timestamp": {
                                "type": "datetime"
                            }
                        }
                    }
                }
            }, 
            "ipstr": {
                "type": "ip"
            }, 
            "ip": {
                "type": "long"
            }, 
            "tags": {
                "index": "not_analyzed", 
                "type": "string"
            }
        }
    }
}

VALID_BIG_QUERY = [
    {
        "fields": [
            {
                "type": "STRING", 
                "name": "tls", 
                "mode": "NULLABLE"
            }, 
            {
                "fields": [
                    {
                        "type": "BOOLEAN", 
                        "name": "heartbeat_support", 
                        "mode": "NULLABLE"
                    }, 
                    {
                        "type": "BOOLEAN", 
                        "name": "heartbleed_vulnerable", 
                        "mode": "NULLABLE"
                    }, 
                    {
                        "type": "TIMESTAMP", 
                        "name": "timestamp", 
                        "mode": "NULLABLE"
                    }
                ], 
                "type": "RECORD", 
                "name": "heartbleed", 
                "mode": "NULLABLE"
            }
        ], 
        "type": "RECORD", 
        "name": "p443", 
        "mode": "NULLABLE"
    }, 
    {
        "type": "STRING", 
        "name": "ipstr", 
        "mode": "REQUIRED"
    }, 
    {
        "type": "STRING", 
        "name": "tags", 
        "mode": "REPEATED"
    }, 
    {
        "type": "DOUBLE", 
        "name": "ip", 
        "mode": "NULLABLE"
    }
]


class CompileAndValidationTests(unittest.TestCase):

    def setUp(self):
        heartbleed = SubRecord({
        	"heartbeat_support":Boolean(),
        	"heartbleed_vulnerable":Boolean(),
        	"timestamp":DateTime()
        })
        
        self.host = Record({
        		"ipstr":IPv4Address(required=True),
        		"ip":Long(),
        		Port(443):SubRecord({
        			"tls":String(),
        			"heartbleed":heartbleed
        		}),
        		"tags":ListOf(String())
        })

    def test_bigquery(self):
        global VALID_BIG_QUERY
        r = self.host.to_bigquery()
        self.assertEqual(json.loads(r), VALID_BIG_QUERY)

    def test_elasticsearch(self):
        global VALID_ELASTIC_SEARCH
        r = self.host.to_es("host")
        self.assertEqual(json.loads(r), VALID_ELASTIC_SEARCH)

    def test_validation_known_good(self):
        test = {
            "ipstr":"141.212.120.1",
            "ip":2379511809,
            "443":{
                "tls":"test"
            }
        }
        self.host.validate(test)

    def test_validation_bad_key(self):
        test = {
            "keydne":"141.212.120.1asdf",
            "ip":2379511809,
            "443":{
                "tls":"test"
            }
        }
        try:
            self.host.validate(test)
            raise Exception("validation did not fail")
        except DataValidationException:
            pass

    def test_validation_bad_value(self):
        test = {
            "ipstr":10,
            "ip":2379511809,
            "443":{
                "tls":"test"
            }
        }
        try:
            self.host.validate(test)
            raise Exception("validation did not fail")
        except DataValidationException:
            pass

