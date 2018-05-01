import collections
import itertools
import json
import os
import pprint
import datetime

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
            self.assertEquals(type(a), type(b))
            if isinstance(a, collections.Sized) \
                    and isinstance(a, collections.Sized):
                self.assertEquals(len(a), len(b))
            if isinstance(a, list):
                for x, y in itertools.izip(sorted(a), sorted(b)):
                    self.assertBigQuerySchemaEqual(x, y)
            elif isinstance(a, dict):
                for k in a:
                    self.assertIn(k, b)
                    self.assertBigQuerySchemaEqual(a[k], b[k])
            else:
                self.assertEquals(a, b)

    def setUp(self):
        self.maxDiff=10000

        heartbleed = SubRecord({
            "heartbeat_support":Boolean(),
            "heartbleed_vulnerable":Boolean(category="Vulnerabilities"),
            "timestamp":DateTime()
        })
        self.host = Record({
                "ipstr":IPv4Address(required=True, examples=["8.8.8.8"]),
                "ip":Unsigned32BitInteger(doc="The IP Address of the host"),
                Port(443):SubRecord({
                    "tls":String(),
                    "heartbleed":heartbleed
                }, category="heartbleed"),
                "tags":ListOf(String())
        })

    def test_bigquery(self):
        global VALID_BIG_QUERY
        r = self.host.to_bigquery()
        self.assertBigQuerySchemaEqual(r, VALID_BIG_QUERY)

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
            "banner":SubRecord({
                "comment":String(),
                "timestamp":DateTime()
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
        self.assertRaises(lambda: StringList(Positional()))

        # ListOf(...) needs exactly one positional arg, so GenericCategorizedList() should also raise.
        self.assertRaises(lambda: CategorizedCertificateList())

        # Confirm that positional args are pulled from the proper location
        MyPositional = Positional.with_args("a", doc="default docs")
        p0 = MyPositional()
        p0doc = MyPositional(doc="some docs")
        # Passing positional args to the factory constructor and the contructor is not allowed
        self.assertRaises(lambda: MyPositional("b"))
        self.assertRaises(lambda: MyPositional("x, y"))
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
        self.assertRaises(lambda: self.record.validate({"c":{"foo":"string value"}}))

    def test_policy_setting_inherit(self):
        self.assertRaises(lambda: self.record.validate({"c":{"foo":"string value"}}))

    def test_policy_setting_multi_level_inherit(self):
        self.assertRaises(lambda: self.record.validate({"a":{"bar":"string value"}}))

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
