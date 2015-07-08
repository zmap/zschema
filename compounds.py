import json
from keys import *

class ListOf(Keyable):
	def __init__(self, object_, max_items=10):
		self.object_ = object_
		self.max_items = max_items

	def print_indent_string(self, name, indent):
		tabs = "\t" * indent if indent else ""
		print tabs + name + ":%s:" % self.__class__.__name__,
		self.object_.print_indent_string(self.key_to_string(name), indent+1)
		
	def to_bigquery(self, name):
		retv = self.object_.to_bigquery(name)
		retv["mode"] = "REPEATED"
		return retv


class SubRecord(Keyable):
	def __init__(self, definition, required=False):
		self.definition = definition
		self.required = required
		
	def to_bigquery(self, name):
		fields = []
		for name_, value in self.definition.iteritems():
			fields.append(value.to_bigquery(name_))
		
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


class Record(object):
	def __init__(self, definition):
		self.definition = definition

	def to_elastic_search(self):
		pass
		
	def to_bigquery(self):
		retv = []
		for name, subrecord in self.definition.iteritems():
			retv.append(subrecord.to_bigquery(name))
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