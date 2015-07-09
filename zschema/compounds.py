import json
from keys import *

class ListOf(Keyable):

    def __init__(self, object_, max_items=10, doc=None):
        self.object_ = object_
        self.max_items = max_items
        self.doc = doc

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
        if type(v) != list:
            raise DataValidationException("%s: %s is not a list",
                                          name, str(value))
        for item in value:
            self.object_.validate(name, item)


class SubRecord(Keyable):

    def __init__(self, definition, required=False, doc=None):
        self.definition = definition
        self.required = required
        self.doc = doc
        
    def to_bigquery(self, name):
        return {
            "name":self.key_to_bq(name),
            "type":"RECORD",
            "fields":[v.to_bigquery(k) for (k,v) in self.definition.items()],
            "mode":"REQUIRED" if self.required else "NULLABLE"
        }

    def print_indent_string(self, name, indent):
        tabs = "\t" * indent if indent else ""
        print tabs + self.key_to_string(name) + ":subrecord:"
        for name, value in self.definition.iteritems():
            value.print_indent_string(name, indent+1)

    def to_es(self):
        p = {self.key_to_es(k): v.to_es() for k, v in self.definition.items()}
        return {"properties": p}

    def validate(self, name, value):
        if type(value) != dict:
            raise DataValidationException("%s: %s is not a dict",
                                          name, str(value))
        for subkey, subvalue in value.items():
            if subkey not in self.definition:
                raise DataValidationException("%s: %s is not a valid subkey", 
                                              name, subkey)
            self.definition[subkey].validate(subkey, subvalue)


class Record(SubRecord):
    def __init__(self, definition):
        self.definition = definition

    def to_es(self, name):
        return json.dumps({name:SubRecord.to_es(self)}, indent=4)
        
    def to_bigquery(self):
        retv = [s.to_bigquery(name) for (name, s) in self.definition.items()]
        return json.dumps(retv, indent=4)
    
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


