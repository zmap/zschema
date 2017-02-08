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
        elif type(other) == str:
            return self.port.__eq__(other)
        else:
            return self.port.__eq__(other.port)

    def __hash__(self):
        return self.port.__hash__()


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

    # create a map from name of type to class. We can use this
    # in order to create the Python definition from JSON. We need
    # this in the web interface. We define this in keyable because
    _types_by_name = {}

    @staticmethod
    def key_to_bq(o):
        if type(o) in (str, unicode):
            return o
        else:
            return o.to_bigquery()

    @staticmethod
    def key_to_es(o):
        if type(o) in (str, unicode):
            return o
        else:
            return o.to_es()

    @staticmethod
    def key_to_string(o):
        if type(o) in (str, unicode):
            return o
        else:
            return o.to_string()

    @property
    def exclude_bigquery(self):
        return "bigquery" in self._exclude

    @property
    def exclude_elasticsearch(self):
        return "elasticsearch" in self._exclude

    def add_es_var(self, d, name, instance, default):
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


class DataValidationException(TypeError):
    pass


class MergeConflictException(Exception):
    pass

_zschema_types_by_name = {}
