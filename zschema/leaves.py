import sys
import unittest
import re
import dateutil.parser
import datetime
import socket
import pytz

from keys import *
from keys import _NO_ARG


class Leaf(Keyable):

    # defaults
    REQUIRED = False
    ES_INCLUDE_RAW = False
    ES_INDEX = None
    ES_ANALYZER = None

    def __init__(self,
            required=_NO_ARG,
            es_index=_NO_ARG,
            es_analyzer=_NO_ARG,
            desc=_NO_ARG,
            doc=_NO_ARG,
            examples=_NO_ARG,
            es_include_raw=_NO_ARG,
            deprecated=_NO_ARG,
            ignore=_NO_ARG,
            category=_NO_ARG,
            exclude=_NO_ARG,
            metadata=_NO_ARG,
            units=_NO_ARG,
            min_value=_NO_ARG,
            max_value=_NO_ARG,
            validation_policy=_NO_ARG):
        Keyable.__init__(self,
                required=required,
                desc=desc,
                doc=doc,
                category=category,
                exclude=exclude,
                deprecated=deprecated,
                ignore=ignore,
                examples=examples,
                validation_policy=validation_policy)
        self.set("es_index", es_index)
        self.set("es_analyzer", es_analyzer)
        self.set("units", units)
        self.set("min_value", min_value)
        self.set("max_value", max_value)
        self.set("es_include_raw", es_include_raw)

    def to_dict(self):
        retv = super(Leaf, self).to_dict()
        self.add_not_empty(retv, "es_type", "es_type")
        self.add_not_empty(retv, "bq_type", "bq_type")
        self.add_not_empty(retv, "units", "units")
        self.add_not_empty(retv, "es_analyzer", "es_analyzer")
        self.add_not_empty(retv, "es_index", "es_index")
        self.add_not_empty(retv, "es_search_analyzer", "es_search_analyzer")
        return retv

    def to_es(self):
        retv = {"type":self.ES_TYPE}
        self.add_not_empty(retv, "index", "es_index")
        self.add_not_empty(retv, "analyzer", "es_analyzer")
        self.add_not_empty(retv, "search_analyzer", "es_search_analyzer")
        if self.es_include_raw:
            retv["fields"] = {
                    "raw":{"type":"keyword"}
            }
        return retv

    def _docs_common(self, parent_category):
        retv = {
            "detail_type": self.__class__.__name__,
            "category": self.category or parent_category,
            "doc": self.doc,
            "required": self.required,
            "examples": self.examples,
        }
        self.add_not_empty(retv, "desc", "desc")
        return retv

    def docs_es(self, parent_category=None):
        retv = self._docs_common(parent_category)
        self.add_not_empty(retv, "analyzer", "es_analyzer")
        retv["type"] = self.ES_TYPE
        return retv

    def docs_bq(self, parent_category=None):
        retv = self._docs_common(parent_category)
        retv["type"] = self.BQ_TYPE
        return retv

    def to_bigquery(self, name):
        if not self._check_valid_name(name):
            raise Exception("Invalid field name: %s" % name)
        mode = "REQUIRED" if self.required else "NULLABLE"
        retv = {"name":self.key_to_bq(name), "type":self.BQ_TYPE, "mode":mode}
        if self.doc:
            retv["doc"] = self.doc
        return retv

    def to_string(self, name):
        return "%s: %s" % (self.key_to_string(name),
                           self.__class__.__name__.lower())

    def to_flat(self, parent, name, repeated=False):
        if not self._check_valid_name(name):
            raise Exception("Invalid field name: %s" % name)
        if repeated:
            mode = "repeated"
        elif self.required:
            mode = "required"
        else:
            mode = "nullable"
        full_name = ".".join([parent, name]) if parent else name
        yield {
            "name":full_name,
            "type":self.__class__.__name__,
            "es_type": self.ES_TYPE,
            "documentation":self.doc,
            "mode":mode
        }
        if self.es_include_raw:
            yield {
                "name":full_name + ".raw",
                "type":self.__class__.__name__,
                "documentation":self.doc,
                "es_type": self.ES_TYPE,
                "mode":mode
            }

    def print_indent_string(self, name, indent):
        val = self.key_to_string(name)
        if indent:
            tabs = "\t" * indent
            val = tabs + val
        print val

    def validate(self, name, value, policy=_NO_ARG, parent_policy=_NO_ARG):
        calculated_policy = self._calculate_policy(name, policy, parent_policy)
        try:
            self._raising_validate(name, value)
        except DataValidationException as e:
            self._handle_validation_exception(calculated_policy, e)

    def _raising_validate(self, name, value):
        # ^ take args and kwargs because compounds have additional
        # arguments that get passed in
        if not self._check_valid_name(name):
            raise Exception("Invalid field name: %s" % name)
        if value is None:
            if self.required:
                raise DataValidationException("%s is a required field, but "
                                              "received None" % name)
            else:
                return
        if type(value) not in self.EXPECTED_CLASS:
            m = "class mismatch for %s: expected %s, %s has class %s" % (\
                    self.key_to_string(name), self.EXPECTED_CLASS,
                    str(value), value.__class__.__name__)
            raise DataValidationException(m)
        if hasattr(self, "_validate"):
            self._validate(str(name), value)


class String(Leaf):

    ES_TYPE = "keyword"
    BQ_TYPE = "STRING"
    EXPECTED_CLASS = [str,unicode]

    INVALID = 23
    VALID = "asdf"


class EnglishString(Leaf):

    ES_TYPE = "text"
    BQ_TYPE = "STRING"
    ES_ANALYZER = "standard"
    EXPECTED_CLASS = [str,unicode]

    INVALID = 23
    VALID = "asdf"


class AnalyzedString(Leaf):

    ES_TYPE = "text"
    BQ_TYPE = "STRING"
    ES_ANALYZER = "simple"
    EXPECTED_CLASS = [str,unicode]

    INVALID = 23
    VALID = "asdf"


class WhitespaceAnalyzedString(AnalyzedString):
    """
    curl -XPUT 'localhost:9200/YOUR-INDEX-HERE/_settings' -d '{
      "analysis" : {
        "analyzer":{
          "lower_whitespace":{
            "type":"custom",
            "tokenizer":"whitespace",
            "filter":["lowercase"]
          }
        }
      }
    }'
    """
    ES_ANALYZER="lower_whitespace"


class HexString(Leaf):

    ES_TYPE = "keyword"
    BQ_TYPE = "STRING"
    EXPECTED_CLASS = [str,unicode]

    INVALID = "asdfasdfa"
    VALID = "003a929e3e0bd48a1e7567714a1e0e9d4597fe9087b4ad39deb83ab10c5a0278"

    #ES_SEARCH_ANALYZER = "lower_whitespace"
    HEX_REGEX = re.compile('(?:[A-Fa-f0-9][A-Fa-f0-9])+')

    def _is_hex(self, s):
        return bool(self.HEX_REGEX.match(s))

    def _validate(self, name, value):
        if not self._is_hex(value):
            m = "%s: the value %s is not hex" % (name, value)
            raise DataValidationException(m)


class Enum(Leaf):

    ES_TYPE = "keyword"
    BQ_TYPE = "STRING"

    EXPECTED_CLASS = [str,unicode]

    INVALID = 23
    VALID = None

    def __init__(self, values=None, *args, **kwargs):
        Leaf.__init__(self, *args, **kwargs)
        if values is None:
            values = []
        self.values = values
        self.values_s = set(values)

    def _validate(self, name, value):
        if len(self.values_s) and value not in self.values_s:
            m = "%s: the value %s is not a valid enum option" % (name, value)
            raise DataValidationException(m)

    def _docs_common(self, parent_category):
        retv = super(Enum, self)._docs_common(parent_category)
        if len(self.values_s):
            retv["values"] = list(self.values_s)
            del retv["examples"]
        return retv

class HTML(AnalyzedString):

    """
    curl -XPUT 'localhost:9200/ipv4/_settings' -d '{
      "analysis" : {
        "analyzer":{
          "html":{
            "type":"custom",
            "tokenizer":"standard",
            "char_filter":[ "html_strip"]
          }
        }
      }
    }'
    """
    ES_ANALYZER = "html"


class IPAddress(Leaf):

    ES_TYPE = "ip"
    BQ_TYPE = "STRING"
    EXPECTED_CLASS = [str,unicode]

    INVALID = "my string"
    VALID = "141.212.120.0"

    def _is_ipv4_addr(self, ip):
        try:
            socket.inet_pton(socket.AF_INET, ip)
            return True
        except socket.error:
            return False

    def _is_ipv6_addr(self, ip):
        try:
            socket.inet_pton(socket.AF_INET6, ip)
            return True
        except socket.error:
            return False

    def _validate(self, name, value):
        if not self._is_ipv4_addr(value) and not self._is_ipv6_addr(value):
            m = "%s: the value %s is not a valid IP address" % (name, value)
            raise DataValidationException(m)


class IPv4Address(IPAddress):

    INVALID = "2a04:9740:8:c010:e228:6dff:fefe:6e53"
    VALID = "141.212.120.0"

    def _validate(self, name, value):
        if not self._is_ipv4_addr(value):
            m = "%s: the value %s is not a valid IPv4 address" % (name, value)
            raise DataValidationException(m)


class IPv6Address(IPAddress):

    INVALID = "141.212.120.0"
    VALID = "2a04:9740:8:c010:e228:6dff:fefe:6e53"

    def _validate(self, name, value):
        if not self._is_ipv6_addr(value):
            m = "%s: the value %s is not a valid IPv6 address" % (name, value)
            raise DataValidationException(m)


class _Integer(Leaf):

    ES_TYPE = "integer"
    BQ_TYPE = "INTEGER"

    EXPECTED_CLASS = [int,]

    def _validate(self, name, value):
        max_ = 2**self.BITS - 1
        min_ = -2**self.BITS + 1
        if value > max_:
            raise DataValidationException("%s: %s is larger than max (%s)" % (\
                    name, str(value), str(max_)))
        if value < min_:
            raise DataValidationException("%s: %s is smaller than min (%s)" % (\
                    name, str(value), str(min_)))


class Signed32BitInteger(_Integer):

    INVALID = 8589934592
    VALID = 234234252

    BITS = 32


class Signed8BitInteger(_Integer):

    ES_TYPE = "byte"
    BITS = 8
    INVALID = 2**8+5
    VALID = 34


class Signed16BitInteger(_Integer):

    ES_TYPE = "short"
    BITS = 16
    INVALID = 2**16
    VALID = 0xFFFF
    EXPECTED_CLASS = [int,]


class Unsigned8BitInteger(Signed16BitInteger):
    pass


class Unsigned16BitInteger(Signed32BitInteger):
    pass


class Signed64BitInteger(_Integer):

    ES_TYPE = "long"
    BQ_TYPE = "INTEGER"
    EXPECTED_CLASS = [int,long]
    INVALID = 2l**68
    VALID = 10l
    BITS = 64


class Unsigned32BitInteger(Signed64BitInteger):
    pass


class Float(Leaf):

    ES_TYPE = "float"
    BQ_TYPE = "FLOAT"
    EXPECTED_CLASS = [float,]
    INVALID = "I'm a string!"
    VALID = 10.0


class Double(Float):

    ES_TYPE = "double"
    BQ_TYPE = "FLOAT"
    EXPECTED_CLASS = [float,]


class Boolean(Leaf):

    ES_TYPE = "boolean"
    BQ_TYPE = "BOOLEAN"
    EXPECTED_CLASS = [bool,]
    INVALID = 0
    VALID = True


class Binary(Leaf):

    ES_TYPE = "binary"
    BQ_TYPE = "STRING"
    ES_INDEX = "no"
    EXPECTED_CLASS = [str,unicode]
    B64_REGEX = re.compile('^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$')

    def _is_base64(self, data):
        return bool(self.B64_REGEX.match(data))

    def _validate(self, name, value):
        if not self._is_base64(value):
            m = "%s: the value %s is not valid Base64" % (name, value)
            raise DataValidationException(m)

    VALID = "03F87824"
    INVALID = "normal"


class IndexedBinary(Binary):

    ES_TYPE = "string"
    BQ_TYPE = "STRING"
    ES_INDEX = "not_analyzed"


class DateTime(Leaf):

    ES_TYPE = "date"
    BQ_TYPE = "DATETIME"
    # dateutil.parser.parse(int) throws...? is this intended to be a unix epoch offset?
    EXPECTED_CLASS = [str, int, unicode, datetime.datetime]

    VALID = "Wed Jul  8 08:52:01 EDT 2015"
    INVALID = "Wed DNE  35 08:52:01 EDT 2015"

    MIN_VALUE = "1753-01-01 00:00:00.000000+00:00"
    MAX_VALUE = "9999-12-31 23:59:59.999999+00:00"

    def __init__(self, *args, **kwargs):
        super(DateTime, self).__init__(*args, **kwargs)

        if self.min_value:
            self._min_value_dt = dateutil.parser.parse(self.min_value)
        else:
            self._min_value_dt = dateutil.parser.parse(self.MIN_VALUE)

        if self.max_value:
            self._max_value_dt = dateutil.parser.parse(self.max_value)
        else:
            self._max_value_dt = dateutil.parser.parse(self.MAX_VALUE)

    def _validate(self, name, value):
        if isinstance(value, datetime.datetime):
            dt = value
        elif isinstance(value, int):
            dt = datetime.datetime.utcfromtimestamp(value)
        else:
            try:
                dt = dateutil.parser.parse(value)
            except Exception:
                m = "%s: %s is not valid timestamp" % (name, str(value))
                raise DataValidationException(m)
        dt = DateTime._ensure_tz_aware(dt)
        if dt > self._max_value_dt:
            m = "%s: %s is larger than allowed maximum (%s)" % (name,
                    str(value), str(self._max_value_dt))
            raise DataValidationException(m)
        if dt < self._min_value_dt:
            m = "%s: %s is larger than allowed minimum (%s)" % (name,
                    str(value), str(self._min_value_dt))
            raise DataValidationException(m)

    @staticmethod
    def _ensure_tz_aware(dt):
        """Ensures that the given datetime is timezone-aware. If it is not timezone-aware as
        given, this function localizes it to UTC. Returns a timezone-aware datetime instance."""
        if dt.tzinfo:
            return dt
        return pytz.utc.localize(dt)


class Timestamp(DateTime):

    BQ_TYPE = "TIMESTAMP"


class OID(String):

    VALID = "1.3.6.1.4.868.2.4.1"
    INVALID = "hello"

    OID_REGEX = re.compile("[[0-9]+\\.]*")

    def _is_oid(self, data):
        return bool(self.OID_REGEX.match(data))

    def _validate(self, name, value):
        if not self._is_oid(value):
            m = "%s: the value %s is not a valid oid" % (name, value)
            raise DataValidationException(m)


class EmailAddress(WhitespaceAnalyzedString):

    INCLUDE_RAW = True


class URL(AnalyzedString):

    # This depends on https://github.com/jlinn/elasticsearch-analysis-url being installed

    """
    curl -XPUT 'localhost:9200/YOUR-INDEX-HERE/_settings' -d '{
      "analysis" : {
        "filter":{
          "url":{
            "type":"url",
            "part":null,
            "url_decode":true,
            "allow_malformed":true,
            "tokenize_malformed":true
          }
        }
      }
    }'
    curl -XPUT 'localhost:9200/YOUR-INDEX-HERE/_settings' -d '{
      "analysis" : {
        "analyzer":{
          "url":{
            "type":"custom",
            "tokenizer":"whitespace",
            "filter":["url"]
          }
        }
      }
    }'

    """

    ES_ANALYZER = "URL"
    ES_SEARCH_ANALYZER = "whitespace"
    INCLUDE_RAW = True


class FQDN(URL):
    pass


class URI(URL):
    pass


VALID_LEAVES = [
    DateTime,
    AnalyzedString,
    String,
    Binary,
    IndexedBinary,
    Boolean,
    Double,
    Float,
    IPv4Address,
    IPv6Address,
    IPAddress,
    Enum,
    HexString,
    OID,
    FQDN,
    URL,
    URI,
    EmailAddress,
]

