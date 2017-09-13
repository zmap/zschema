import json
import os
import pprint

from zschema import registry
from zschema.leaves import *
from zschema.compounds import *

def json_fixture(name):
    filename = name + ".json"
    fixture_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', filename)
    with open(fixture_path) as fixture_file:
        fixture = json.load(fixture_file)
    return fixture

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
                        "type": "keyword"
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
                "type": "keyword"
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
                        "type": "DATETIME",
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
        self.maxDiff=10000

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

    #def test_missing_required(self):
    #     # ipstr is not set
    #     test = {
    #         "443":{
    #             "tls":"string",
    #         }
    #     }
    #     try:
    #         self.host.validate(test)
    #         self.fail("ipstr is missing")
    #     except DataValidationException:
    #         pass

    def test_null_subkey(self):
        test = {
            "ipstr": "1.2.3.4",
            "443": {
                "heartbleed": None,
            }
        }
        try:
            self.host.validate(test)
            self.fail("heartbleed is null")
        except DataValidationException:
            pass

    def test_null_port(self):
        test = {
            "ipstr": "1.2.3.4",
            "443": None,
        }
        try:
            self.host.validate(test)
            self.fail("443 is null")
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

    def test_parses_ipv4_records(self):
        ipv4_host_ssh = Record({
            Port(22):SubRecord({
                "ssh":SubRecord({
                    "banner": SubRecord({
                        "comment":String(),
                        "timestamp":DateTime()
                    })
                })
            })
        })
        ipv4_host_ssh.validate(json_fixture('ipv4-ssh-record'))


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
