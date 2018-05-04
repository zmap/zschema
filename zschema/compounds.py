import sys
import copy
import json

from keys import *
from keys import _NO_ARG


def _is_valid_object(name, object_):
    if not isinstance(object_, Keyable):
        raise Exception("Invalid schema. %s is not a Keyable." % name)


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
        return self.object_.exclude_bigquery

    @property
    def exclude_elasticsearch(self):
        return self.object_.exclude_elasticsearch

    def print_indent_string(self, name, indent):
        tabs = "\t" * indent if indent else ""
        print tabs + name + ":%s:" % self.__class__.__name__,
        self.object_.print_indent_string(self.key_to_string(name), indent+1)

    def to_bigquery(self, name):
        retv = self.object_.to_bigquery(name)
        retv["mode"] = "REPEATED"
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

    def validate(self, name, value, policy=_NO_ARG, parent_policy=_NO_ARG):
        calculated_policy = self._calculate_policy(name, policy, parent_policy)
        try:
            if not isinstance(value, list):
                m = "%s: %s is not a list" % (name, str(value))
                raise DataValidationException(m)
            if self.max_items > 0 and len(value) > self.max_items:
                m = "%s: %s has too many values (max: %i)" % (name, str(value),
                        self.max_items)
                raise DataValidationException(m)
            if self.min_items > 0 and len(value) < self.min_items:
                m = "%s: %s has too few values (min: %i)" % (name, str(value),
                        self.min_items)
                raise DataValidationException(m)
        except DataValidationException as e:
            self._handle_validation_exception(calculated_policy, e)
            # we won't be able to iterate
            return
        for item in value:
            try:
                self.object_.validate(name, item, policy, calculated_policy)
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
        validation_policy=_NO_ARG):
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


class SubRecord(Keyable):

    DEFINITION = {}
    ALLOW_UNKNOWN = False

    def __init__(self, definition=_NO_ARG, extends=_NO_ARG,
            allow_unknown=_NO_ARG, *args, **kwargs):
        super(SubRecord, self).__init__(*args, **kwargs)
        self.set("definition", definition)
        self.set("allow_unknown", allow_unknown)
        if extends is not _NO_ARG:
            extends = copy.deepcopy(extends)
            self.set("definition", self.merge(extends).definition)
        # safety check
        if self.definition:
            for k, v in sorted(self.definition.iteritems()):
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
        for subname, doc in sorted(self.definition.iteritems()):
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
                raise MergeConflictException("Unable to merge definitions. "
                                "Differing types: %s vs %s" % (type(l_value),
                                            type(r_value)))
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
                for (k,v) in sorted(self.definition.iteritems()) \
                if not v.exclude_bigquery
                ]
        retv = {
            "name":self.key_to_bq(name),
            "type":"RECORD",
            "fields":fields,
            "mode":"REQUIRED" if self.required else "NULLABLE"
        }
        return retv

    def docs_bq(self, parent_category=None):
        category = self.category or parent_category
        retv = self._docs_common(category)
        fields = { self.key_to_bq(k): v.docs_bq(parent_category=category) \
                   for (k,v) in sorted(self.definition.iteritems()) \
                   if not v.exclude_bigquery }
        retv["fields"] = fields
        return retv

    def print_indent_string(self, name, indent):
        tabs = "\t" * indent if indent else ""
        print tabs + self.key_to_string(name) + ":subrecord:"
        for name, value in sorted(self.definition.iteritems()):
            value.print_indent_string(name, indent+1)

    def to_es(self):
        p = {self.key_to_es(k): v.to_es() \
                for k, v in sorted(self.definition.iteritems()) \
                if not v.exclude_elasticsearch}
        return {"properties": p}

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
                           for k, v in sorted(self.definition.iteritems()) \
                           if not v.exclude_elasticsearch }
        return retv

    def to_dict(self):
        source = sorted(self.definition.iteritems())
        p = {self.key_to_es(k): v.to_dict() for k, v in source}
        return {"type":"subrecord", "subfields": p, "doc":self.doc, "required":self.required}

    def validate(self, name, value, policy=_NO_ARG, parent_policy=_NO_ARG):
        calculated_policy = self._calculate_policy(name, policy, parent_policy)
        try:
            if not isinstance(value, dict):
                m = "%s: %s is not a dict" % (name, str(value))
                raise DataValidationException(m)
        except DataValidationException as e:
            self._handle_validation_exception(calculated_policy, e)
            # cannot iterate over members if this isn't a dictionary
            return
        for subkey, subvalue in sorted(value.iteritems()):
            try:
                if not self.allow_unknown and subkey not in self.definition:
                    raise DataValidationException("%s: %s is not a valid subkey" %
                                                  (name, subkey))
                if subkey in self.definition:
                    self.definition[subkey].validate(subkey, subvalue,
                            policy, calculated_policy)
            except DataValidationException as e:
                self._handle_validation_exception(calculated_policy, e)


def SubRecordType(definition,
        required=_NO_ARG,
        doc=_NO_ARG,
        desc=_NO_ARG,
        allow_unknown=_NO_ARG,
        exclude=_NO_ARG,
        category=_NO_ARG,
        validation_policy=_NO_ARG):
    t = type("SubRecord", (SubRecord,), {})
    t.set_default("definition", definition)
    t.set_default("required", required)
    t.set_default("doc", doc)
    t.set_default("desc", desc)
    t.set_default("allow_unknown", allow_unknown)
    t.set_default("exclude", exclude)
    t.set_default("category", category)
    t.set_default("validation_policy", validation_policy)
    return t


class NestedListOf(ListOf):

    def __init__(self, object_, subrecord_name, max_items=10, doc=None, category=None):
        super(NestedListOf, self).__init__(object_, max_items=max_items,
                doc=doc, category=category)
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

    def to_es(self, name):
        return {name:SubRecord.to_es(self)}

    def docs_es(self, name, parent_category=None):
        category = self.category or parent_category
        return {name: SubRecord.docs_es(self, parent_category=category)}

    def to_bigquery(self):
        source = sorted(self.definition.iteritems())
        return [s.to_bigquery(name) \
                for (name, s) in source \
                if not s.exclude_bigquery
                ]

    def docs_bq(self, name, parent_category=None):
        category = self.category or parent_category
        return {name: SubRecord.docs_bq(self, parent_category=category)}

    def print_indent_string(self):
        for name, field in sorted(self.definition.iteritems()):
            field.print_indent_string(name, 0)

    def validate(self, value, policy=_NO_ARG):
        if policy is None:
            policy = _NO_ARG
        calculated_policy = self._calculate_policy("root", policy, self.validation_policy)
        # ^ note: record explicitly does not take a parent_policy
        if type(value) != dict:
            raise DataValidationException("record is not a dict:\n{}".format(value))
        for subkey, subvalue in sorted(value.iteritems()):
            try:
                if subkey not in self.definition:
                    raise DataValidationException("%s is not a valid subkey of root" % subkey)
                self.definition[subkey].validate(subkey, subvalue, policy,
                        self.validation_policy)
            except DataValidationException as e:
                self._handle_validation_exception(calculated_policy, e)

    def to_dict(self):
        source = sorted(self.definition.iteritems())
        return {self.key_to_es(k): v.to_es() for k, v in source}

    def to_json(self):
        return json.dumps(self.to_dict(), indent=4)

    def to_flat(self):
        for subname, doc in sorted(self.definition.iteritems()):
            for item in doc.to_flat(None, self.key_to_es(subname)):
                yield item

    @classmethod
    def from_json(cls, j):
        return cls({(k, __encode(v)) for k, v in sorted(j.iteritems())})

