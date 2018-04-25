class __NO_ARG(object):
    __nonzero__ = lambda _: False
_NO_ARG = __NO_ARG()


class Port(object):

    def __init__(self, port):
        self.port = str(port)

    def to_bigquery(self):
        return "p%s" % self.port

    def to_es(self):
        return self.port

    to_string = to_es

    def __eq__(self, other):
        if type(other) == int:
            return int(self.port).__eq__(other)
        elif type(other) in (str, unicode):
            return self.port.__eq__(str(other))
        else:
            return self.port.__eq__(other.port)

    def __hash__(self):
        return self.port.__hash__()

    def __cmp__(self, other):
        if type(other) == int:
            return cmp(int(self.port), other)
        elif type(other) in (str, unicode):
            return cmp(self.port, str(other))
        else:
            return cmp(int(self.port), int(other.port))


class Keyable(object):

    VALID_ES_INDEXES = [
        "analyzed", # full-text
        "not_analyzed", # searchable, not full-text,
        "no", # field is not searchable
    ]

    VALID_ES_ANALYZERS = [
        "standard",   # The standard analyzer is the default analyzer that Elasticsearch uses.
                      # It is the best general choice for analyzing text that may be in any language.
                      # It splits the text on word boundaries, as defined by the Unicode Consortium,
                      # and removes most punctuation.
        "simple",     # The simple analyzer splits the text on anything that isn't a letter,
                      # and lowercases the terms. It would produce
        "whitespace", # The whitespace analyzer splits the text on whitespace. It doesn't lowercase.
    ]

    # defaults
    REQUIRED = False
    DEPRECATED = False
    DEPRECATED_TYPE = False
    DOC = None
    DESC = None
    CATEGORY = None
    EXAMPLES = []
    EXCLUDE = set([])
    METADATA = {}
    EXCLUDE_BIGQUERY = False
    EXCLUDE_ELASTICSEARCH = False

    # create a map from name of type to class. We can use this
    # in order to create the Python definition from JSON. We need
    # this in the web interface. We define this in keyable because
    _types_by_name = {}

    @staticmethod
    def _check_valid_name(name):
        if "-" in name:
            return False
        return True

    @staticmethod
    def key_to_bq(o):
        if type(o) in (str, unicode):
            return o
        else:
            return o.to_bigquery()

    @staticmethod
    def key_to_es(o):
        if type(o) in (str, unicode):
            if not Keyable._check_valid_name(o):
                raise Exception("invalid key name: %s" % o)
            return o
        else:
            return o.to_es()

    @staticmethod
    def key_to_string(o):
        if type(o) in (str, unicode):
            if not Keyable._check_valid_name(o):
                raise Exception("invalid key name: %s" % o)
            return o
        else:
            return o.to_string()

    @property
    def exclude_bigquery(self):
        return "bigquery" in self.exclude

    @property
    def exclude_elasticsearch(self):
        return "elasticsearch" in self.exclude

    def add_es_var(self, d, name, instance, default=None):
        if default is None:
            default = name.upper()
        if hasattr(self, instance) and getattr(self, instance):
            d[name] = getattr(self, instance)
        elif hasattr(self, default) and getattr(self, default):
            d[name] = getattr(self, default)
        return d

    @classmethod
    def _populate_types_by_name(cls):
        if cls._types_by_name:
            return
        def __iter_classes(kls):
            try:
                for klass in kls.__subclasses__():
                    for klass2 in __iter_classes(klass):
                        yield klass2
            except TypeError:
                try:
                    for klass in kls.__subclasses__(kls):
                        for klass2 in __iter_classes(klass):
                            yield klass2
                except:
                    pass
            yield kls
        for klass in __iter_classes(Keyable):
            Keyable._types_by_name[klass.__name__] = klass


    def __init__(self, required=_NO_ARG, desc=_NO_ARG, doc=_NO_ARG, category=_NO_ARG,
            exclude=_NO_ARG, deprecated=_NO_ARG, ignore=_NO_ARG,
            examples=_NO_ARG, metadata=_NO_ARG):
        self.set("required", required)
        self.set("desc", desc)
        self.set("doc", doc)
        print ">>>> ._value_doc is", self._value_doc
        print "hasattr", hasattr("self", "_value_doc")
        #print ">>>> .doc is ", self.doc
        self.set("examples", examples)
        self.set("category", category)
        self.set("metadata", metadata)
        self.set("exclude", exclude)
        self.set("deprecated", deprecated)
        self.set("ignore", ignore)

        if self.DEPRECATED_TYPE:
            e = "WARN: %s is deprecated and will be removed in a "\
                    "future release\n" % self.__class__.__name__
            sys.stderr.write(e)

    def to_dict(self):
        retv = {
            "required":self.required,
            "doc":self.doc,
            "type":self.__class__.__name__,
            "metadata":self.metadata,
            "examples": self.examples,
        }
        return retv

    def set(self, k, v):
        #print "set called for ", k, "setting to", v
        new_k = "_value_" + k
        setattr(self, new_k, v)

    @classmethod
    def set_default(cls, k, v):
        new_k = k.upper()
        setattr(cls, k, v)

    def __getattr__(self, k):
        # base case so that this doesn't end up in an infinite loop
        print "getattr for", k
        if k[0] == "_":
            raise AttributeError(k)
        if hasattr(self, "_value_" + k):
            print "has _value_" + k
            v = getattr(self, "_value_" + k)
            if v is not _NO_ARG:
                return v
        print "does not have _value_" + k
        if hasattr(self, k.upper()):
            v = getattr(self, k.upper())
            if v is not _NO_ARG:
                print "default"
                return v
        raise AttributeError(k)

    @classmethod
    def set_defaults(cls, required=None, doc=None, category=None):
        cls.set_default("category", category)
        cls.set_default("required", required)
        cls.set_default("doc", doc)


class DataValidationException(TypeError):
    pass


class MergeConflictException(Exception):
    pass

_zschema_types_by_name = {}
