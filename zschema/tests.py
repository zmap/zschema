import json
import pprint

from zschema import registry
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
                                "type": "date"
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
        "type": "INTEGER",
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
        self.maxDiff=10000

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
        self.assertEqual(r, VALID_BIG_QUERY)

    def test_elasticsearch(self):
        global VALID_ELASTIC_SEARCH
        r = self.host.to_es("host")
        self.assertEqual(r, VALID_ELASTIC_SEARCH)

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

    def test_merge_no_conflict(self):
        a = SubRecord({
                "a":String(),
                "b":SubRecord({
                    "c":String()
                    })
            })
        b = SubRecord({
                "d":String(),
            })
        valid = SubRecord({
                "a":String(),
                "b":SubRecord({
                    "c":String()
                    }),
                "d":String(),

        })
        self.assertEqual(a.merge(b).to_dict(), valid.to_dict())

    def test_merge_different_types(self):
        a = SubRecord({
                "a":String(),
            })
        b = SubRecord({
                "a":SubRecord({})
            })
        try:
            a.merge(b)
            raise Exception("validation did not fail")
        except MergeConflictException:
            pass

    def test_merge_unmergable_types(self):
        a = SubRecord({
                "a":String(),
            })
        b = SubRecord({
                "a":String(),
            })
        try:
            a.merge(b)
            raise Exception("validation did not fail")
        except MergeConflictException:
            pass

    def test_merge_recursive(self):
        a = SubRecord({
                "m":SubRecord({
                       "a":String()
                })
            })
        b = SubRecord({
                "a":String(),
                "m":SubRecord({
                       "b":String()
                 })

            })
        c = SubRecord({
                "a":String(),
                "m":SubRecord({
                       "a":String(),
                       "b":String()
                    })
            })
        self.assertEquals(a.merge(b).to_dict(), c.to_dict())

    def test_extends(self):
        host = Record({
                 "host":IPv4Address(required=True),
                 "time":DateTime(required=True),
                 "data":SubRecord({}),
                 "error":String()
               })
        banner_grab = Record({
                        "data":SubRecord({
                                   "banner":String()
                               })
                      }, extends=host)
        tls_banner_grab = Record({
                            "data":SubRecord({
                                       "tls":SubRecord({})
                                   })
                          }, extends=banner_grab)
        smtp_starttls = Record({
                            "data":SubRecord({
                                       "ehlo":String()
                                   })
                        }, extends=tls_banner_grab)

        valid = Record({
                  "host":IPv4Address(required=True),
                  "time":DateTime(required=True),
                  "data":SubRecord({
                            "banner":String(),
                            "tls":SubRecord({}),
                            "ehlo":String()
                         }),
                  "error":String()
                })
        self.assertEqual(smtp_starttls.to_dict(), valid.to_dict())

    def test_null_required(self):
        test = {
            "ipstr":None,
        }
        try:
            self.host.validate(test)
            raise Exception("validation did not fail")
        except DataValidationException:
            pass

    def test_null_notrequired(self):
        test = {
            "ip":None,
            "443":{
                "tls":"None"
            }
        }
        self.host.validate(test)


class RegistryTests(unittest.TestCase):

    def setUp(self):
        self.host = Record({
                "ipstr":IPv4Address(required=True),
                "ip":Unsigned32BitInteger(),
        })
        self.domain = Record({
                "domain":String(required=True),
        })

    def test_get_registered(self):
        try:
            registry.get_schema("host")
            self.fail("missing schema should throw")
        except KeyError:
            pass
        all_schemas = registry.all_schemas()
        self.assertEqual(0, len(all_schemas))

        registry.register_schema("host", self.host)
        try:
            host_schema = registry.get_schema("host")
        except KeyError:
            self.fail("registered schema should not throw")
        all_schemas = registry.all_schemas()
        self.assertEqual(1, len(all_schemas))
        all_schemas['domain'] = self.domain

        all_schemas = registry.all_schemas()
        self.assertEqual(1, len(all_schemas))

        registry.register_schema("domain", self.domain)
        self.assertEqual(1, len(all_schemas))
        try:
            domain_schema = registry.get_schema("domain")
        except KeyError:
            self.fail("registered schema should not throw")
        all_schemas = registry.all_schemas()
        self.assertEqual(2, len(all_schemas))


class NestedListOfTests(unittest.TestCase):
    pass
