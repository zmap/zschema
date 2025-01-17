from collections.abc import Sized
import datetime
import json
import os
import unittest

from zschema import registry
from zschema.compounds import ListOf, NestedListOf, Record, SubRecord, SubRecordType
from zschema.keys import Keyable, Port, MergeConflictException
from zschema.leaves import Boolean, DateTime, Enum, IPv4Address, String, Unsigned8BitInteger, Unsigned32BitInteger, VALID_LEAVES
from zschema.leaves import DataValidationException


def json_fixture(name):
    filename = name + ".json"
    fixture_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', filename)
    with open(fixture_path) as fixture_file:
        fixture = json.load(fixture_file)
    return fixture


class LeafUnitTests(unittest.TestCase):

    def test_valid(self):
        for leaf in VALID_LEAVES:
            leaf(validation_policy="error").validate(leaf.__name__, leaf.VALID)

    def test_invalid(self):
        for leaf in VALID_LEAVES:
            try:
                leaf(validation_policy="error").validate(leaf.__name__, leaf.INVALID)
                raise Exception("invalid value did not fail for %s",
                                leaf.__name__)
            except DataValidationException:
                continue

    def test_to_dict(self):
        for leaf in VALID_LEAVES:
            leaf().to_dict()

    def test_es(self):
        for leaf in VALID_LEAVES:
            leaf().to_es()

    def test_bq(self):
        for leaf in VALID_LEAVES:
            leaf().to_bigquery("myname")

    def test_docs_es(self):
        for leaf in VALID_LEAVES:
            leaf().docs_es()

    def test_docs_bq(self):
        for leaf in VALID_LEAVES:
            leaf().docs_bq()


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

VALID_DOCS_OUTPUT_FOR_ES_FIELDS = {
    "host": {
        "category": None,
        "doc": None,
        "fields": {
            "443": {
                "category": "heartbleed",
                "doc": None,
                "fields": {
                    "heartbleed": {
                        "category": "heartbleed",
                        "doc": None,
                        "fields": {
                            "heartbeat_support": {
                                "category": "heartbleed",
                                "detail_type": "Boolean",
                                "doc": None,
                                "examples": [],
                                "required": False,
                                "type": "boolean"
                            },
                            "heartbleed_vulnerable": {
                                "category": "Vulnerabilities",
                                "detail_type": "Boolean",
                                "doc": None,
                                "examples": [],
                                "required": False,
                                "type": "boolean"
                            },
                            "timestamp": {
                                "category": "heartbleed",
                                "detail_type": "DateTime",
                                "doc": None,
                                "examples": [],
                                "required": False,
                                "type": "date"
                            }
                        },
                        "required": False,
                        "type": "SubRecord"
                    },
                    "tls": {
                        "category": "heartbleed",
                        "detail_type": "String",
                        "doc": None,
                        "examples": [],
                        "required": False,
                        "type": "keyword"
                    }
                },
                "required": False,
                "type": "SubRecord"
            },
            "ip": {
                "category": None,
                "detail_type": "Unsigned32BitInteger",
                "doc": "The IP Address of the host",
                "examples": [],
                "required": False,
                "type": "long"
            },
            "ipstr": {
                "category": None,
                "detail_type": "IPv4Address",
                "doc": None,
                "examples": [
                    "8.8.8.8"
                ],
                "required": True,
                "type": "ip"
            },
            "tags": {
                "category": None,
                "detail_type": "String",
                "doc": None,
                "examples": [],
                "repeated": True,
                "required": False,
                "type": "keyword"
            }
        },
        "required": False,
        "type": "Record"
    }
}

VALID_DOCS_OUTPUT_FOR_BIG_QUERY_FIELDS = {
    "host": {
        "category": None,
        "doc": None,
        "fields": {
            "ip": {
                "category": None,
                "detail_type": "Unsigned32BitInteger",
                "doc": "The IP Address of the host",
                "examples": [],
                "required": False,
                "type": "INTEGER"
            },
            "ipstr": {
                "category": None,
                "detail_type": "IPv4Address",
                "doc": None,
                "examples": [
                    "8.8.8.8"
                ],
                "required": True,
                "type": "STRING"
            },
            "p443": {
                "category": "heartbleed",
                "doc": None,
                "fields": {
                    "heartbleed": {
                        "category": "heartbleed",
                        "doc": None,
                        "fields": {
                            "heartbeat_support": {
                                "category": "heartbleed",
                                "detail_type": "Boolean",
                                "doc": None,
                                "examples": [],
                                "required": False,
                                "type": "BOOLEAN"
                            },
                            "heartbleed_vulnerable": {
                                "category": "Vulnerabilities",
                                "detail_type": "Boolean",
                                "doc": None,
                                "examples": [],
                                "required": False,
                                "type": "BOOLEAN"
                            },
                            "timestamp": {
                                "category": "heartbleed",
                                "detail_type": "DateTime",
                                "doc": None,
                                "examples": [],
                                "required": False,
                                "type": "DATETIME"
                            }
                        },
                        "required": False,
                        "type": "SubRecord"
                    },
                    "tls": {
                        "category": "heartbleed",
                        "detail_type": "String",
                        "doc": None,
                        "examples": [],
                        "required": False,
                        "type": "STRING"
                    }
                },
                "required": False,
                "type": "SubRecord"
            },
            "tags": {
                "category": None,
                "detail_type": "String",
                "doc": None,
                "examples": [],
                "repeated": True,
                "required": False,
                "type": "STRING"
            }
        },
        "required": False,
        "type": "Record"
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
        "doc": "The IP Address of the host",
        "mode": "NULLABLE"
    },
]

VALID_PROTO = """syntax = "proto3";
package schema;

import "google/protobuf/timestamp.proto";

message Host {
    string ipstr = 1;
    uint32 ip = 2;
    message P443Struct {
        string tls = 1;
        message HeartbleedStruct {
            Timestamp timestamp = 10;
            bool heartbeat_support = 11;
        }
        HeartbleedStruct heartbleed = 77;
    }
    P443Struct p443 = 3;
    repeated string tags = 47;
}"""


class CompileAndValidationTests(unittest.TestCase):

    def assertBigQuerySchemaEqual(self, a, b):
        """
        Assert that the given BigQuery schemas are equivalent.

        BigQuery schemas consist of lists whose order doesn't matter, dicts,
        and privimites.
        """
        # allow python to have the first pass at deciding whether two objects
        # are equal. If they aren't, apply less strict logic (e.g., allow lists
        # of differing orders to be equal).
        if a == b:
            return
        else:
            self.assertEqual(type(a), type(b))
            if isinstance(a, Sized) \
                    and isinstance(b, Sized):
                self.assertEqual(len(a), len(b))
            if isinstance(a, list) and isinstance(b, list):
                name_ordered_a = sorted(a, key=lambda x: x['name'])
                name_ordered_b = sorted(b, key=lambda x: x['name'])
                for x, y in zip(name_ordered_a, name_ordered_b):
                    self.assertBigQuerySchemaEqual(x, y)
            elif isinstance(a, dict):
                for k in a:
                    self.assertIn(k, b)
                    self.assertBigQuerySchemaEqual(a[k], b[k])
            else:
                self.assertEqual(a, b)

    def setUp(self):
        self.maxDiff=10000

        heartbleed = SubRecord({ # with explicit proto field indices
            "heartbeat_support":Boolean(pr_index=11),
            "heartbleed_vulnerable":Boolean(category="Vulnerabilities", pr_ignore=True),
            "timestamp":DateTime(pr_index=10)
        }, pr_index=77)
        self.host = Record({
                "ipstr":IPv4Address(required=True, examples=["8.8.8.8"], pr_index=1),
                "ip":Unsigned32BitInteger(doc="The IP Address of the host", pr_index=2),
                Port(443):SubRecord({
                    "tls":String(pr_index=1),
                    "heartbleed":heartbleed
                }, category="heartbleed", pr_index=3),
                "tags":ListOf(String(), pr_index=47)
        })

    def test_bigquery(self):
        global VALID_BIG_QUERY
        r = self.host.to_bigquery()
        self.assertBigQuerySchemaEqual(r, VALID_BIG_QUERY)

    def test_proto(self):
        global VALID_PROTO
        r = self.host.to_proto("host")
        self.assertEqual(r, VALID_PROTO)

    def test_elasticsearch(self):
        global VALID_ELASTIC_SEARCH
        r = self.host.to_es("host")
        self.assertEqual(r, VALID_ELASTIC_SEARCH)

    def test_docs_output(self):
        global VALID_DOCS_OUTPUT_FOR_ES_FIELDS
        r = self.host.docs_es("host")
        self.assertEqual(r, VALID_DOCS_OUTPUT_FOR_ES_FIELDS)

        global VALID_DOCS_OUTPUT_FOR_BIG_QUERY_FIELDS
        r = self.host.docs_bq("host")
        self.assertEqual(r, VALID_DOCS_OUTPUT_FOR_BIG_QUERY_FIELDS)

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
        self.assertEqual(a.merge(b).to_dict(), c.to_dict())

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

    def test_es_dynamic_record(self):
        ipv4_host_with_dynamic_strict = Record()
        es = ipv4_host_with_dynamic_strict.to_es("strict-record")
        self.assertFalse("dynamic" in es["strict-record"])

        ipv4_host_with_dynamic_strict = Record(
            es_dynamic_policy="strict"
        )
        es = ipv4_host_with_dynamic_strict.to_es("strict-record")
        self.assertEqual("strict", es["strict-record"]["dynamic"])

    def test_subrecord_types(self):
        SSH = SubRecordType({
            "banner":SubRecord({
                "comment":String(),
                "timestamp":DateTime()
                })
            },
            doc="class doc",
            required=False)
        self.assertEqual(SSH.DOC, "class doc")
        self.assertEqual(SSH.REQUIRED, False)
        ssh = SSH(doc="instance doc")
        ipv4_host_ssh = Record({
            Port(22):SubRecord({
                "ssh":ssh
            })
        })
        self.assertEqual(ssh.doc, "instance doc")
        self.assertEqual(ssh.required, False)
        ipv4_host_ssh.validate(json_fixture('ipv4-ssh-record'))
        # class unchanged
        self.assertEqual(SSH.DOC, "class doc")
        self.assertEqual(SSH.REQUIRED, False)

    def test_subrecord_type_override(self):
        SSH = SubRecordType({
            "banner": SubRecord({
                "comment": String(),
                "timestamp": DateTime()
                })
            },
            doc="class doc",
            required=False)
        self.assertEqual(SSH.DOC, "class doc")
        self.assertEqual(SSH.REQUIRED, False)
        ssh = SSH(doc="instance doc", required=True)
        ipv4_host_ssh = Record({
            Port(22):SubRecord({
                "ssh":ssh
            })
        })
        self.assertEqual(ssh.doc, "instance doc")
        self.assertEqual(ssh.required, True)
        ipv4_host_ssh.validate(json_fixture('ipv4-ssh-record'))
        # class unchanged
        self.assertEqual(SSH.DOC, "class doc")
        self.assertEqual(SSH.REQUIRED, False)


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


class SubRecordTests(unittest.TestCase):

    def test_subrecord_child_types_can_override_parent_attributes(self):
        Certificate = SubRecordType({}, doc="A parsed certificate.")
        c = Certificate(doc="The CA certificate.")
        OtherType = SubRecord({
            "ca": c,
            "host": Certificate(doc="The host certificate."),
        }, doc="hello")
        self.assertEqual("A parsed certificate." , Certificate().doc)
        self.assertEqual("The CA certificate.", OtherType.definition["ca"].doc)
        self.assertEqual("The host certificate.", OtherType.definition["host"].doc)


class NestedListOfTests(unittest.TestCase):
    pass


class WithArgsTests(unittest.TestCase):
    def test_with_args(self):
        Certificate = SubRecord.with_args({}, doc="A parsed certificate.")
        CertificateChain = ListOf.with_args(Certificate())
        AlgorithmType = String.with_args(doc="An algorithm identifier", examples=["a", "b", "c"])
        OtherType = SubRecord({
            "ca": Certificate(doc="The CA certificate."),
            "host": Certificate(doc="The host certificate."),
            "chain": CertificateChain(doc="The certificate chain."),
            "host_alg": AlgorithmType(doc="The host algorithm", examples=["x", "y"]),
            "client_alg": AlgorithmType(doc="The client algorithm"),
            "sig_alg": AlgorithmType(examples=["p", "q"]),
        })
        # Check default
        self.assertEqual("A parsed certificate.", Certificate().doc)

        # Check overridden
        self.assertEqual("The CA certificate.", OtherType.definition["ca"].doc)
        self.assertEqual("The host certificate.", OtherType.definition["host"].doc)

        # Check ListOf
        self.assertEqual("The certificate chain.", OtherType.definition["chain"].doc)

        # Check that instance default is used in child
        self.assertEqual("A parsed certificate.", OtherType.definition["chain"].object_.doc)

        # Check Leaf type doc overrides
        self.assertEqual("The host algorithm", OtherType.definition["host_alg"].doc)
        self.assertEqual("The client algorithm", OtherType.definition["client_alg"].doc)
        self.assertEqual("An algorithm identifier", OtherType.definition["sig_alg"].doc)

        # Check that examples are inherited
        self.assertEqual(["a", "b", "c"], OtherType.definition["client_alg"].examples)

        # Check that examples are overridden
        self.assertEqual(["x", "y"], OtherType.definition["host_alg"].examples)
        self.assertEqual(["p", "q"], OtherType.definition["sig_alg"].examples)

    def test_with_args_positional_override(self):
        class Positional(Keyable):
            def __init__(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs
                self.doc = kwargs.get("doc")

        # Leave the positional argument (list type) blank, but specify a category.
        GenericCategorizedList = ListOf.with_args(category="my category")
        # Get lists with different types (but the same category).
        categorized_string_list = GenericCategorizedList(String())
        categorized_cert_list = GenericCategorizedList(Positional())
        self.assertEqual("my category", categorized_string_list.category)
        self.assertEqual("my category", categorized_cert_list.category)

        # ListOf(...) takes only one positional arg, so StringList(Positional()) should raise.
        StringList = ListOf.with_args(String())
        self.assertRaises(Exception, lambda: StringList(Positional()))

        # ListOf(...) needs exactly one positional arg, so GenericCategorizedList() should also raise.
        self.assertRaises(Exception, lambda: CategorizedCertificateList())

        # Confirm that positional args are pulled from the proper location
        MyPositional = Positional.with_args("a", doc="default docs")
        p0 = MyPositional()
        p0doc = MyPositional(doc="some docs")
        # Passing positional args to the factory constructor and the contructor is not allowed
        self.assertRaises(Exception, lambda: MyPositional("b"))
        self.assertRaises(Exception, lambda: MyPositional("x, y"))
        self.assertEqual(("a",), p0.args)
        self.assertEqual("default docs", p0.doc)
        self.assertEqual(("a",), p0doc.args)
        self.assertEqual("some docs", p0doc.doc)

class DatetimeTest(unittest.TestCase):

    def test_datetime_DateTime(self):
        DateTimeRecord = DateTime(validation_policy="error")
        DateTimeRecord.validate("fake", datetime.datetime.now())
        DateTimeRecord.validate("fake", "Wed Dec  5 01:23:45 CST 1956")
        DateTimeRecord.validate("fake", 116048701)


class ValidationPolicies(unittest.TestCase):

    def setUp(self):
        self.maxDiff=10000

        Child = SubRecordType({
            "foo":Boolean(),
            "bar":Boolean(validation_policy="error"),
        }, validation_policy="error")
        self.record = Record({
            "a":Child(validation_policy="error"),
            "b":Child(validation_policy="warn"),
            "c":Child(validation_policy="ignore"),
            "d":Child(validation_policy="inherit"),
        })

    def test_policy_setting_warn(self):
        self.record.validate({"b":{"foo":"string value"}})

    def test_policy_setting_ignore(self):
        self.record.validate({"c":{"foo":"string value"}})

    def test_policy_setting_error(self):
        self.assertRaises(DataValidationException, lambda: self.record.validate({"c":{"bar":"string value"}}))

    def test_policy_setting_inherit(self):
        self.assertRaises(DataValidationException, lambda: self.record.validate({"a":{"foo":"string value"}}))

    def test_policy_setting_multi_level_inherit(self):
        self.assertRaises(DataValidationException, lambda: self.record.validate({"a":{"bar":"string value"}}))

    def test_explicit_policy(self):
        self.record.validate({"a":{"foo":"string value"}},
                policy="ignore")

    def test_child_subtree_overrides_and_inherits(self):
        schema = Record({
            Port(445): SubRecord({
                "smb": SubRecord({
                    "banner": SubRecord({
                        "smb_v1": Boolean()
                    })
                }, validation_policy="error")
            })
        }, validation_policy="warn")

        doc = {
            "445": {
                "smb": {
                    "banner": {
                        "smb_v1": True,
                        "metadata": {},
                    }
                }
            }
        }
        self.assertRaises(DataValidationException, lambda: schema.validate(doc))

class ExcludeTests(unittest.TestCase):
    def test_ListOf_exclude(self):
        a = ListOf(String())
        self.assertFalse(a.exclude_bigquery)
        self.assertFalse(a.exclude_elasticsearch)

        b = ListOf(String(exclude=["bigquery"]))
        self.assertTrue(b.exclude_bigquery)
        self.assertFalse(b.exclude_elasticsearch)

        c = ListOf(String(), exclude=["elasticsearch"])
        self.assertFalse(c.exclude_bigquery)
        self.assertTrue(c.exclude_elasticsearch)

        d = ListOf(String(exclude=["bigquery"]), exclude=["elasticsearch"])
        self.assertTrue(d.exclude_bigquery)
        self.assertTrue(d.exclude_elasticsearch)

    # TODO: test the rest of the types


class PathLogUnitTests(unittest.TestCase):
    sub_type = SubRecord({
        "sub1": String(),
        "sub2": SubRecord({
            "sub2sub1": Unsigned8BitInteger(),
            "sub2sub2": NestedListOf(String(), "sub2sub2.subrecord_name"),
        }),
        "sub3": Enum(values=["a", "b", "c"])
    }, validation_policy="error")
    SCHEMA = Record({
        "a": SubRecord({
            "a1": String(),
            "a2": ListOf(sub_type),
            "a3": Unsigned8BitInteger(),
        }),
        "b": String(),
    }, validation_policy="error")

    def test_good(self):
        good = {
            "a": {
                "a1": "{a.a1}:good",
                "a2": [
                    {
                        "sub1": "{a.a2[0].sub1}:good",
                        "sub2": {
                            "sub2sub1": 1,
                            "sub2sub2": [
                                "{a.a2[0].sub2.sub2sub2[0]}:good",
                                "{a.a2[0].sub2.sub2sub2[1]}:good",
                            ],
                        },
                    },
                    {
                        "sub1": "{a.a2[1].sub1}:good",
                        "sub2": {
                            "sub2sub1": 1,
                            "sub2sub2": [],
                        },
                    },
                ],
                "a3": 1,
            },
            "b": "{b}:good",
        }
        self.SCHEMA.validate(good, policy="error")

    def test_bad_root(self):
        bad1 = {
            "does_not_exist": "{does_not_exist}:bad1",
            "a": {
                "a1": "{a.a1}:bad1",
                "a2": [
                    {
                        "sub1": "{a.a2[0].sub1}:bad1",
                        "sub2": {
                            "sub2sub1": 1,
                            "sub2sub2": [
                                "{a.a2[0].sub2.sub2sub2[0]}:bad1",
                                "{a.a2[0].sub2.sub2sub2[1]}:bad1",
                            ],
                        },
                    },
                    {
                        "sub1": "{a.a2[1].sub1}:bad1",
                        "sub2": {
                            "sub2sub1": 1,
                            "sub2sub2": [],
                        },
                    },
                ],
                "a3": 1,
            },
            "b": "{b}:bad1",
        }
        try:
            self.SCHEMA.validate(bad1, policy="error")
            self.assertTrue(False, "bad1 failed to fail")
        except DataValidationException as e:
            self.assertTrue(not e.path)

        del(bad1["does_not_exist"])
        bad1["b"] = 23

        try:
            self.SCHEMA.validate(bad1, policy="error")
            self.assertTrue(False, "bad1 failed to fail")
        except DataValidationException as e:
            self.assertEqual(e.path, ["b"])


    def test_bad_a_key(self):
        bad = {
            "a": {
                "does_not_exist": 23,
                "a1": "{a.a1}:bad1",
                "a2": [
                    {
                        "sub1": "{a.a2[0].sub1}:bad1",
                        "sub2": {
                            "sub2sub1": 1,
                            "sub2sub2": [
                                "{a.a2[0].sub2.sub2sub2[0]}:bad1",
                                "{a.a2[0].sub2.sub2sub2[1]}:bad1",
                            ],
                        },
                    },
                    {
                        "sub1": "{a.a2[1].sub1}:bad1",
                        "sub2": {
                            "sub2sub1": 1,
                            "sub2sub2": [],
                        },
                    },
                ],
                "a3": 1,
            },
            "b": "{b}:bad1",
        }
        try:
            self.SCHEMA.validate(bad, policy="error")
            self.assertTrue(False, "bad failed to fail")
        except DataValidationException as e:
            self.assertEqual(e.path, ["a"])
        del(bad["a"]["does_not_exist"])
        bad["a"]["a3"] = "not an int"
        try:
            ret = self.SCHEMA.validate(bad, policy="error")
            self.assertTrue(False, "bad failed to fail")
        except DataValidationException as e:
            self.assertEqual(e.path, ["a", "a3"])

    def test_bad_deep_key(self):
        bad = {
            "a": {
                "a1": "{a.a1}:bad",
                "a2": [
                    {
                        "sub1": "{a.a2[0].sub1}:bad",
                        "sub2": {
                            "sub2sub1": 1,
                            "sub2sub2": [
                                "{a.a2[0].sub2.sub2sub2[0]}:bad",
                                "{a.a2[0].sub2.sub2sub2[1]}:bad",
                            ],
                            "does_not_exist": "fake",
                        },
                    },
                    {
                        "sub1": "{a.a2[1].sub1}:bad1",
                        "sub2": {
                            "sub2sub1": 1,
                            "sub2sub2": [],
                        },
                    },
                ],
                "a3": 1,
            },
            "b": "{b}:bad1",
        }
        try:
            self.SCHEMA.validate(bad, policy="error")
            self.assertTrue(False, "failed to fail")
        except DataValidationException as e:
            self.assertEqual(e.path, ["a", "a2", 0, "sub2", ])
        del(bad["a"]["a2"][0]["sub2"]["does_not_exist"])
        bad["a"]["a2"][0]["sub2"]["sub2sub2"][1] = {
            "wrong type": "bad type"
        }
        try:
            self.SCHEMA.validate(bad, policy="error")
            self.assertTrue(False, "bad failed to fail")
        except DataValidationException as e:
            self.assertEqual(e.path, ["a", "a2", 0, "sub2", "sub2sub2", 1])


class TestSubRecordType(unittest.TestCase):

    def test_subrecord_type(self):
        A = SubRecordType({
            "string": String(),
            "boolean": Boolean(),
        })

        first = A()
        second = A()

        # The class returned by SubRecordType() should be constructable into
        # unique objects
        self.assertIsNot(first, second)
        self.assertTrue(issubclass(A, SubRecord))
        self.assertIsInstance(first, A)
        self.assertIsInstance(second, A)

        # Check the properties aren't shared
        self.assertIsNone(first.definition['string'].doc)
        self.assertIsNone(second.definition['string'].doc)
        first.definition['string'].doc = "hello"
        self.assertIsNone(second.definition['string'].doc)

    def test_subrecord_type_extends(self):
        S = SubRecordType({
            "provided": Boolean(),
        })

        extended_type = SubRecord({
            "property": String(),
            "record": SubRecord({
                "another": String(),
            }),
        }, extends=S())

        base = S()
        extends = extended_type
        self.assertNotIsInstance(extends, S)
        self.assertFalse(base.definition['provided'].exclude)
        self.assertFalse(extended_type.definition['provided'].exclude)
        base.definition['provided'].exclude = ['bigquery']
        self.assertEqual(['bigquery'], base.definition['provided'].exclude)
        self.assertFalse(extended_type.definition['provided'].exclude)

    def test_indexing_works(self):
        definition = {
            "id": Unsigned32BitInteger(doc="int doc"),
            "name": Enum(values=["a", "b", "c"], doc="enum doc"),
        }
        T = SubRecordType(definition)
        t = T(exclude={"bigquery"})
        self.assertEqual({"bigquery"}, t.exclude)
        self.assertEqual("int doc", t["id"].doc)
        self.assertEqual("enum doc", t["name"].doc)
        self.assertFalse(t["id"].exclude)
        t["id"].set("exclude", t["id"].exclude | {"elasticsearch"})
        self.assertEqual({"bigquery"}, t.exclude)
        self.assertEqual({"elasticsearch"}, t["id"].exclude)
        second = T()
        self.assertFalse(second.exclude)
        self.assertFalse(second["id"].exclude)

        CertType = SubRecordType({
            "id": Unsigned32BitInteger(doc="The numerical certificate type value. 1 identifies user certificates, 2 identifies host certificates."),
            "name": Enum(values=["USER", "HOST", "unknown"], doc="The human-readable name for the certificate type."),
        })
        ssh_certkey_public_key_type = CertType(exclude={"bigquery"})
        ssh_certkey_public_key_type["id"].set("exclude",
                                      ssh_certkey_public_key_type["id"].exclude |
                                      {"elasticsearch"})

    def test_multiple_subrecord_types(self):
        A = SubRecordType({
            "first": String(),
        }, type_name="A")
        B = SubRecordType({
            "second": Boolean(),
        }, type_name="B")

        a = A()
        self.assertIn("first", a.definition)
        b = B()
        self.assertIn("second", b.definition)
        a = A()
        self.assertIn("first", a.definition)


