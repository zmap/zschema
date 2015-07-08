class Port(object):
	def __init__(self, port):
		self.port = int(port)
		
	def to_bigquery(self):
		return "p%i" % self.port
		
	def to_es(self):
		return str(self.port)

	to_string = to_es

class Keyable(object):
	@staticmethod
	def key_to_bq(o):
		if type(o) == str:
			return o
		else:
			return o.to_bigquery()

	@staticmethod
	def key_to_es(o):
		if type(o) == str:
			return o
		else:
			return o.to_es()
			
	@staticmethod
	def key_to_string(o):
		if type(o) == str:
			return o
		else:
			return o.to_string()