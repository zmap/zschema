import copy
import json
from keys import *

def _is_valid_object(name, object_):
    if not isinstance(object_, Keyable):
        raise Exception("Invalid schema. %s is not a Keyable." % name)



class ListOf(Keyable):

    def __init__(self, object_, max_items=10):
        self.object_ = object_
        self.max_items = max_items
        _is_valid_object("Anonymous ListOf", object_)

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

    def to_es(self):
        return self.object_.to_es()

    def validate(self, name, value):
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


class SubRecord(Keyable):

    def __init__(self,
            definition,
            required=False,
            doc=None,
            extends=None,
            allow_unknown=False,
            exclude=None):
        self.definition = definition
        self.required = required
        self.allow_unknown = allow_unknown
        self.doc = doc
        self._exclude = set(exclude) if exclude else set([])
        # merge
        if extends:
            extends = copy.deepcopy(extends)
            self.definition = self.merge(extends).definition
        for k, v in self.definition.items():
            _is_valid_object(k, v)

    def new(self):
        return copy.deepcopy(self)

    def to_flat(self, parent, name, repeated=False):
        if repeated:
            mode = "repeated"
        elif self.required:
            mode = "required"
        else:
            mode = "nullable"
        this_name = ".".join([parent, self.key_to_es(name)]) if parent else self.key_to_es(name)
        yield {"type":self.__class__.__name__, "name":this_name, "mode":mode}
        for subname, doc in self.definition.iteritems():
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
        fields = [v.to_bigquery(k) for (k,v) in self.definition.iteritems() if \
                not v.exclude_bigquery]
        return {
            "name":self.key_to_bq(name),
            "type":"RECORD",
            "fields":fields,
            "mode":"REQUIRED" if self.required else "NULLABLE"
        }

    def print_indent_string(self, name, indent):
        tabs = "\t" * indent if indent else ""
        print tabs + self.key_to_string(name) + ":subrecord:"
        for name, value in self.definition.iteritems():
            value.print_indent_string(name, indent+1)

    def to_es(self):
        p = {self.key_to_es(k): v.to_es() for k, v in self.definition.items() \
                if not v.exclude_elasticsearch}
        return {"properties": p}

    def to_dict(self):
        p = {self.key_to_es(k): v.to_dict() for k, v in self.definition.items()}
        return {"type":"subrecord", "subfields": p, "doc":self.doc, "required":self.required}

    def validate(self, name, value):
        if type(value) != dict:
            raise DataValidationException("%s: %s is not a dict",
                                          name, str(value))
        for subkey, subvalue in value.items():
            if not self.allow_unknown and subkey not in self.definition:
                raise DataValidationException("%s: %s is not a valid subkey",
                                              name, subkey)
            else:
                continue
            self.definition[subkey].validate(subkey, subvalue)


class NestedListOf(ListOf):

    def __init__(self, object_, subrecord_name, max_items=10):
        ListOf.__init__(self, object_, max_items)
        self.subrecord_name = subrecord_name

    def to_bigquery(self, name):
        subr = SubRecord({
            self.subrecord_name:ListOf(self.object_)
        })
        retv = subr.to_bigquery(self.key_to_bq(name))
        retv["mode"] = "REPEATED"
        return retv


class Record(SubRecord):

    def to_es(self, name):
        return {name:SubRecord.to_es(self)}

    def to_bigquery(self):
        return [s.to_bigquery(name) for (name, s) in self.definition.items() \
                if not s.exclude_bigquery]

    def to_html(self):
        pass

    def to_documented_html(self):
        pass

    def print_indent_string(self):
        for name, field in self.definition.iteritems():
            field.print_indent_string(name, 0)

    def to_dotted_text(self):
        pass

    def validate(self, value):
        if type(value) != dict:
            raise DataValidationException("record is not a dict", str(value))
        for subkey, subvalue in value.items():
            if subkey not in self.definition:
                raise DataValidationException("%s is not a valid subkey of root",
                                              subkey)
            self.definition[subkey].validate(subkey, subvalue)

    def to_dict(self):
        return {self.key_to_es(k): v.to_es() for k, v in self.definition.items()}

    def to_json(self):
        return json.dumps(self.to_dict(), indent=4)

    def to_flat(self):
        for subname, doc in self.definition.iteritems():
            for item in doc.to_flat(None, self.key_to_es(subname)):
                yield item

    @classmethod
    def from_json(cls, j):
        return cls({(k, __encode(v)) for k, v in j.items()})


