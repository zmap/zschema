from __future__ import print_function
from builtins import int, str, dict

import sys
import copy
import json
from collections import OrderedDict

from zschema.keys import Keyable, DataValidationException, MergeConflictException
from zschema.keys import _NO_ARG


def _is_valid_object(name, object_):
    if not isinstance(object_, Keyable):
        raise Exception("Invalid schema. %s is not a Keyable." % name)

def _proto_message_name(string):
    if string != string.lower():
        return string
    string = "".join(w.capitalize() for w in string.split("_"))
    return string

def _proto_indent(string, n):
    return "\n".join(n*"    " + s for s in string.split("\n"))

# Track protobuf message definitions that have been emitted.
_proto_messages = OrderedDict()


class ListOf(Keyable):

    MAX_ITEMS = 0
    MIN_ITEMS = 0

    def __init__(self, object_, max_items=_NO_ARG, min_items=_NO_ARG, *args, **kwargs):
        _is_valid_object("Anonymous ListOf", object_)
        super(ListOf, self).__init__(*args, **kwargs)
        self.set("object_", object_)
        self.set("max_items", max_items)
        self.set("min_items", min_items)

    @property
    def exclude_bigquery(self):
        # If the child type is excluded, that is the same as excluding this --
        # it's not clear what it would mean otherwise, from a schema perspective
        return super(ListOf, self).exclude_bigquery \
               or self.object_.exclude_bigquery

    @property
    def exclude_elasticsearch(self):
        return super(ListOf, self).exclude_elasticsearch \
               or self.object_.exclude_elasticsearch

    def print_indent_string(self, name, indent):
        tabs = "\t" * indent if indent else ""
        print('{}{}"{:s}"'.format(tabs, name, self.__class__.__name__))
        self.object_.print_indent_string(self.key_to_string(name), indent+1)

    def to_bigquery(self, name):
        retv = self.object_.to_bigquery(name)
        retv["mode"] = "REPEATED"
        return retv

    def to_proto(self, name, indent):
        retv = self.object_.to_proto(name, indent)
        retv["field"] = "repeated " + retv["field"]
        return retv

    def docs_bq(self, parent_category=None):
        category = self.category or parent_category
        retv = self.object_.docs_bq(parent_category=category)
        retv["category"] = category
        retv["repeated"] = True
        if self.doc:
            retv["doc"] = self.doc
        return retv

    def to_es(self):
        # all elasticsearch fields are automatically repeatable
        return self.object_.to_es()

    def docs_es(self, parent_category=None):
        category = self.category or parent_category
        retv = self.object_.docs_es(parent_category=category)
        retv["category"] = category
        retv["repeated"] = True
        if self.doc:
            retv["doc"] = self.doc
        return retv

    def validate(self, name, value, policy=_NO_ARG, parent_policy=_NO_ARG, path=_NO_ARG):
        calculated_policy = self._calculate_policy(name, policy, parent_policy)
        if not path:
            path = []
        try:
            if not isinstance(value, list):
                m = "%s: %s is not a list" % (name, str(value))
                raise DataValidationException(m, path=path)
            if self.max_items > 0 and len(value) > self.max_items:
                m = "%s: %s has too many values (max: %i)" % (name, str(value),
                        self.max_items)
                raise DataValidationException(m, path=path)
            if self.min_items > 0 and len(value) < self.min_items:
                m = "%s: %s has too few values (min: %i)" % (name, str(value),
                        self.min_items)
                raise DataValidationException(m, path=path)
        except DataValidationException as e:
            self._handle_validation_exception(calculated_policy, e)
            # we won't be able to iterate
            return
        for i, item in enumerate(value):
            try:
                self.object_.validate(name, item, policy, calculated_policy, path=path + [i])
            except DataValidationException as e:
                self._handle_validation_exception(calculated_policy, e)

    def to_dict(self):
        return {"type":"list", "list_of":self.object_.to_json()}

    def to_flat(self, parent, name):
        for rec in self.object_.to_flat(parent, name, repeated=True):
            yield rec


def ListOfType(object_,
        required=_NO_ARG,
        max_items=_NO_ARG,
        doc=_NO_ARG,
        desc=_NO_ARG,
        examples=_NO_ARG,
        category=_NO_ARG,
        validation_policy=_NO_ARG,
        pr_ignore=_NO_ARG):
    _is_valid_object("Anonymous ListOf", object_)
    t = type("ListOf", (ListOf,), {})
    t.set_default("object_", object_)
    t.set_default("max_items", max_items)
    t.set_default("required", required)
    t.set_default("doc", doc)
    t.set_default("desc", desc)
    t.set_default("category", category)
    t.set_default("examples", examples)
    t.set_default("validation_policy", validation_policy)
    t.set_default("pr_ignore", pr_ignore)


class SubRecord(Keyable):

    DEFINITION = {}
    ALLOW_UNKNOWN = False
    TYPE_NAME = None
    ES_NESTED = False

    def __init__(self, definition=_NO_ARG, extends=_NO_ARG,
            allow_unknown=_NO_ARG, type_name=_NO_ARG, es_nested=_NO_ARG,
            *args, **kwargs):
        super(SubRecord, self).__init__(*args, **kwargs)
        self.set("definition", definition)
        self.set("allow_unknown", allow_unknown)
        self.set("type_name", type_name)
        if extends is not _NO_ARG:
            extends = copy.deepcopy(extends)
            self.set("definition", self.merge(extends).definition)
        self.set("es_nested", es_nested)
        # safety check
        if self.definition:
            for k, v in sorted(self.definition.items()):
                _is_valid_object(k, v)

    def __getitem__(self, key):
        return self.definition[key]

    def __setitem__(self, key, value):
        self.definition[key] = value

    def __delitem__(self, key):
        del self.definition[key]

    def new(self, **kwargs):
        # Get a new "instance" of the type represented by the SubRecord, e.g.:
        # Certificate = SubRecord({...}, doc="A parsed certificate.")
        # OtherType = SubRecord({
        #   "ca": Certificate.new(doc="The CA certificate."),
        #   "host": Certificate.new(doc="The host certificate.", required=True)
        # })
        e = "WARN: .new() is deprecated and will be removed in a "\
                "future release. Schemas should use SubRecordTypes.\n"
        sys.stderr.write(e)
        return SubRecord({}, extends=self, **kwargs)

    def to_flat(self, parent, name, repeated=False):
        if repeated:
            mode = "repeated"
        elif self.required:
            mode = "required"
        else:
            mode = "nullable"
        this_name = ".".join([parent, self.key_to_es(name)]) if parent else self.key_to_es(name)
        yield {"type":self.__class__.__name__, "name":this_name, "mode":mode}
        for subname, doc in sorted(self.definition.items()):
            for item in doc.to_flat(this_name, self.key_to_es(subname)):
                yield item

    def merge(self, other):
        assert isinstance(other, SubRecord)
        newdef = {}
        l_keys = set(self.definition.keys())
        r_keys = set(other.definition.keys())
        for key in (l_keys | r_keys):
            l_value = self.definition.get(key, None)
            r_value = other.definition.get(key, None)
            if not l_value:
                newdef[key] = r_value
            elif not r_value:
                newdef[key] = l_value
            elif type(l_value) != type(r_value):
                msg = "Unable to merge definitions. Differing types: {} vs {}"
                msg = msg.format(type(l_value), type(r_value))
                raise MergeConflictException(msg)
            elif l_value.__class__ == SubRecord:
                newdef[key] = l_value.merge(r_value)
            else:
                raise MergeConflictException("Only subrecords can be merged. (%s)", key)
        self.set("definition", newdef)
        self.set("required", self.required or other.required)
        self.set("doc", self.doc or other.doc)
        return self

    def to_bigquery(self, name):
        fields = [v.to_bigquery(k) \
                for (k,v) in sorted(self.definition.items()) \
                if not v.exclude_bigquery
                ]
        retv = {
            "name":self.key_to_bq(name),
            "type":"RECORD",
            "fields":fields,
            "mode":"REQUIRED" if self.required else "NULLABLE"
        }
        return retv

    def to_proto(self, name, indent):
        if self.type_name is not None: # named message type -- produced at top level, once
            message_type = _proto_message_name(self.type_name)
            anon = False
        else: # anonymous message type -- nests within containing message
            message_type = _proto_message_name(self.key_to_proto(name)) + "Struct"
            anon = True

        proto_def = ""
        global _proto_messages
        if anon or message_type not in _proto_messages:
            # Explicitly indexed values go first, then implicitly indexed values:
            expected = sum([1 for k, v in self.definition.items() if not v.pr_ignore])
            explicits = [
                (v.to_proto(k, indent), v.explicit_index)
                for k, v in self.definition.items()
                if v.explicit_index is not None and not v.pr_ignore
            ]
            explicits = list(sorted(explicits, key=lambda t: t[1]))
            if len(explicits) != expected:
                msg = "Explicit field numbers required ({}).".format(name)
                raise Exception(msg)
            n = 0
            proto = []
            retvs = explicits
            for (v, i) in retvs:
                if v["message"]:
                    proto += [v["message"]]
                if i is not None:
                    n = i
                else:
                    n += 1
                proto += ["%s = %d;" % (v["field"], n)]
            proto_def = "message %s {\n%s\n}" % \
                        (message_type, _proto_indent("\n".join(proto), indent+1))
            if not anon:
                _proto_messages[message_type] = proto_def
                proto_def = ""
        return {
            "message": proto_def,
            "field": "%s %s" % (message_type, self.key_to_proto(name))
        }

    def docs_bq(self, parent_category=None):
        category = self.category or parent_category
        retv = self._docs_common(category)
        fields = { self.key_to_bq(k): v.docs_bq(parent_category=category) \
                   for (k,v) in sorted(self.definition.items()) \
                   if not v.exclude_bigquery }
        retv["fields"] = fields
        return retv

    def print_indent_string(self, name, indent):
        tabs = "\t" * indent if indent else ""
        print("{}{:s}:subrecord:".format(tabs, self.key_to_string(name)))
        for name, value in sorted(self.definition.items()):
            value.print_indent_string(name, indent+1)

    def to_es(self):
        p = {self.key_to_es(k): v.to_es() \
                for k, v in sorted(self.definition.items()) \
                if not v.exclude_elasticsearch}
        retv = {"properties": p}
        if self.es_nested:
            retv["type"] = "nested"
        return retv

    def _docs_common(self, category):
        retv = {
            "category": category,
            "doc": self.doc,
            "type": self.__class__.__name__,
            "required": self.required,
        }
        return retv

    def docs_es(self, parent_category=None):
        category = self.category or parent_category
        retv = self._docs_common(category)
        retv["fields"] = { self.key_to_es(k): v.docs_es(parent_category=category) \
                           for k, v in sorted(self.definition.items()) \
                           if not v.exclude_elasticsearch }
        return retv

    def to_dict(self):
        source = sorted(self.definition.items())
        p = {self.key_to_es(k): v.to_dict() for k, v in source}
        return {"type":"subrecord", "subfields": p, "doc":self.doc, "required":self.required}


    def validate(self, name, value, policy=_NO_ARG, parent_policy=_NO_ARG, path=_NO_ARG):
        calculated_policy = self._calculate_policy(name, policy, parent_policy)
        if not path:
            path = []

        try:
            if not isinstance(value, dict):
                m = "%s: %s is not a dict" % (name, str(value))
                raise DataValidationException(m, path=path)
        except DataValidationException as e:
            self._handle_validation_exception(calculated_policy, e)
            # cannot iterate over members if this isn't a dictionary
            return
        for subkey, subvalue in sorted(value.items()):
            try:
                if not self.allow_unknown and subkey not in self.definition:
                    raise DataValidationException("%s: %s is not a valid subkey" %
                                                  (name, subkey), path=path)
                if subkey in self.definition:
                    self.definition[subkey].validate(subkey, subvalue,
                            policy, calculated_policy, path=path + [subkey])
            except DataValidationException as e:
                self._handle_validation_exception(calculated_policy, e)


class _SubRecordDefaulted(SubRecord):

    _INIT_DEFAULTS = None

    @classmethod
    def _get_copy_default(cls, k):
        v = cls._INIT_DEFAULTS.get(k, _NO_ARG)
        return copy.deepcopy(v)

    def __init__(self, definition=_NO_ARG, extends=_NO_ARG,
                 allow_unknown=_NO_ARG, type_name=_NO_ARG, *args, **kwargs):
        definition = definition or self._get_copy_default('definition')
        allow_unknown = allow_unknown or self._get_copy_default(
                'allow_unknown')
        type_name = type_name or self._get_copy_default('type_name')
        for k, v in self._INIT_DEFAULTS.items():
            if k in {"definition", "allow_unknown", "type_name"}:
                # These keys are managed by the constructor
                continue
            self.set(k, copy.deepcopy(v))
        super(_SubRecordDefaulted, self).__init__(
            definition=definition,
            extends=extends,
            allow_unknown=allow_unknown,
            type_name=type_name,
            *args,
            **kwargs
        )

    @classmethod
    def _set_default_at_init(cls, k, v):
        cls._INIT_DEFAULTS[k] = v
        cls.set_default(k, v)


def SubRecordType(definition,
        required=_NO_ARG,
        type_name=_NO_ARG,
        doc=_NO_ARG,
        desc=_NO_ARG,
        allow_unknown=_NO_ARG,
        exclude=_NO_ARG,
        category=_NO_ARG,
        validation_policy=_NO_ARG,
        pr_ignore=_NO_ARG):
    #import pdb; pdb.set_trace()
    name = type_name if type_name else "SubRecordType"
    t = type(name, (_SubRecordDefaulted,), {
        "_INIT_DEFAULTS": dict(),
    })
    t._set_default_at_init("definition", definition)
    t._set_default_at_init("type_name", type_name)
    t._set_default_at_init("required", required)
    t._set_default_at_init("doc", doc)
    t._set_default_at_init("desc", desc)
    t._set_default_at_init("allow_unknown", allow_unknown)
    t._set_default_at_init("exclude", exclude)
    t._set_default_at_init("category", category)
    t._set_default_at_init("validation_policy", validation_policy)
    t._set_default_at_init("pr_ignore", pr_ignore)
    return t


class NestedListOf(ListOf):

    def __init__(self, object_, subrecord_name, max_items=10, doc=None, category=None, *args, **kwargs):
        super(NestedListOf, self).__init__(object_, max_items=max_items,
                doc=doc, category=category, *args, **kwargs)
        self.set("subrecord_name", subrecord_name)

    def to_bigquery(self, name):
        subr = SubRecord({
            self.subrecord_name:ListOf(self.object_)
        })
        retv = subr.to_bigquery(self.key_to_bq(name))
        retv["mode"] = "REPEATED"
        if self.doc:
            retv["doc"] = self.doc
        return retv

    def docs_bq(self, parent_category=None):
        subr = SubRecord({
            self.subrecord_name: ListOf(self.object_)
        })
        category = self.category or parent_category
        retv = subr.docs_bq(parent_category=category)
        retv["repeated"] = True
        if self.doc:
            retv["doc"] = self.doc
        return retv


class Record(SubRecord):

    VALIDATION_POLICY = "error"
    ES_DYNAMIC_POLICY = None

    def to_es(self, name):
        subrecord = SubRecord.to_es(self)
        if self.es_dynamic_policy != None:
            subrecord["dynamic"] = self.es_dynamic_policy
        return {name:subrecord}

    def docs_es(self, name, parent_category=None):
        category = self.category or parent_category
        return {name: SubRecord.docs_es(self, parent_category=category)}

    def to_bigquery(self):
        source = sorted(self.definition.items())
        return [s.to_bigquery(name) \
                for (name, s) in source \
                if not s.exclude_bigquery
                ]

    def to_proto(self, name):
        self.type_name = name
        SubRecord.to_proto(self, name, 0)
        return """syntax = "proto3";
package schema;

import "google/protobuf/timestamp.proto";

""" + "\n".join(_proto_messages.values())

    def docs_bq(self, name, parent_category=None):
        category = self.category or parent_category
        return {name: SubRecord.docs_bq(self, parent_category=category)}

    def print_indent_string(self):
        for name, field in sorted(self.definition.items()):
            field.print_indent_string(name, 0)

    def validate(self, value, policy=_NO_ARG, path=_NO_ARG):
        if policy is None:
            policy = _NO_ARG
        if not path:
            path = []
        calculated_policy = self._calculate_policy("root", policy, self.validation_policy)
        # ^ note: record explicitly does not take a parent_policy
        if not isinstance(value, dict):
            raise DataValidationException("record is not a dict:\n{}".format(value), path=path)
        for subkey, subvalue in sorted(value.items()):
            try:
                if subkey not in self.definition:
                    msg = "{} is not a valid subkey of root".format(subkey)
                    raise DataValidationException(msg, path=path)
                self.definition[subkey].validate(
                    subkey,
                    subvalue,
                    policy,
                    self.validation_policy,
                    path=path + [subkey],
                )
            except DataValidationException as e:
                self._handle_validation_exception(calculated_policy, e)

    def to_dict(self):
        source = sorted(self.definition.items())
        return {self.key_to_es(k): v.to_es() for k, v in source}

    def to_json(self):
        return json.dumps(self.to_dict(), indent=4)

    def to_flat(self):
        for subname, doc in sorted(self.definition.items()):
            for item in doc.to_flat(None, self.key_to_es(subname)):
                yield item

    @classmethod
    def from_json(cls, j):
        return cls({(k, __encode(v)) for k, v in sorted(j.items())})

