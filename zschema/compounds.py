import sys
import copy
import json

from keys import *

def _is_valid_object(name, object_):
    if not isinstance(object_, Keyable):
        raise Exception("Invalid schema. %s is not a Keyable." % name)


class ListOf(Keyable):

    def __init__(self, object_, required=None, max_items=None, doc=None, category=None, validator=None):
        self.replace_set("object_", object_)
        self.replace_set("max_items", max_items)
        self.replace_set("category", category)
        self.replace_set("required", required)
        self.replace_set("doc", doc)
        self.replace_set("validator", validator)
        _is_valid_object("Anonymous ListOf", self.object_)

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
        return self.object_.to_es()

    def docs_es(self, parent_category=None):
        category = self.category or parent_category
        retv = self.object_.docs_es(parent_category=category)
        retv["category"] = category
        retv["repeated"] = True
        if self.doc:
            retv["doc"] = self.doc
        return retv

    def validate(self, name, value):
        if self.validator:
            self.validator.validate(name, value)
            return
        if type(value) != list:
            raise DataValidationException("%s: %s is not a list",
                                          name, str(value))
        for item in value:
            self.object_.validate(name, item)

    def to_dict(self):
        return {"type":"list", "list_of":self.object_.to_json()}

    def to_flat(self, parent, name):
        for rec in self.object_.to_flat(parent, name, repeated=True):
            yield rec


def ListOfType(object_,
        required=None,
        max_items=None,
        doc=None,
        category=None):
    _is_valid_object("Anonymous ListOf", object_)
    attrs = {
        "object_":object_,
        "max_items":max_items,
        "required":required,
        "doc":doc,
        "category":category,
    }
    return type("ListOf", (ListOf,), attrs)


class SubRecord(Keyable):

    def __init__(self,
            definition=None,
            required=False,
            doc=None,
            extends=None,
            allow_unknown=False,
            exclude=None,
            category=None,
            validator=None):
        self.replace_set("definition", definition)
        self.replace_set("required", required)
        self.replace_set("allow_unknown", allow_unknown)
        self.replace_set("doc", doc)
        self.replace_set("category", category)
        self.replace_set("validator", validator)
        self.replace_set("_exclude", set(exclude) if exclude else set([]))
        if extends is not None:
            extends = copy.deepcopy(extends)
            self.definition = self.merge(extends).definition
        # safety check
        if self.definition:
            for k, v in sorted(self.definition.iteritems()):
                _is_valid_object(k, v)

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
        doc = self.doc or other.doc
        doc = self.required or other.required
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
        self.definition = newdef
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

    def validate(self, name, value):
        if self.validator:
            self.validator.validate(name, value)
            return
        if type(value) != dict:
            raise DataValidationException("%s: %s is not a dict",
                                          name, str(value))
        for subkey, subvalue in sorted(value.iteritems()):
            if not self.allow_unknown and subkey not in self.definition:
                raise DataValidationException("%s: %s is not a valid subkey" %
                                              (name, subkey))
            else:
                self.definition[subkey].validate(subkey, subvalue)


def SubRecordType(definition,
        required=False,
        doc=None,
        allow_unknown=False,
        exclude=None,
        category=None):
    attrs = {
        "definition":definition,
        "required":required,
        "doc":doc,
        "allow_unknown":allow_unknown,
        "_exclude":exclude if exclude else set([]),
        "category":category
    }
    return type("SubRecord", (SubRecord,), attrs)



class NestedListOf(ListOf):

    def __init__(self, object_, subrecord_name, max_items=10, doc=None, category=None):
        ListOf.__init__(self, object_, max_items, doc=doc, category=category)
        self.subrecord_name = subrecord_name

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

    def validate(self, value):
        if type(value) != dict:
            raise DataValidationException("record is not a dict", str(value))
        for subkey, subvalue in sorted(value.iteritems()):
            if subkey not in self.definition:
                raise DataValidationException("%s is not a valid subkey of root" % subkey)
            self.definition[subkey].validate(subkey, subvalue)

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

