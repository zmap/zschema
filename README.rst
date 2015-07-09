.. image:: https://secure.travis-ci.org/zmap/zschema.svg?branch=master

ZSchema
=======

ZSchema is a generic (meta-)schema language for defining database schemas
that facilitates (1) validating JSON documents against a schema definition and
(2) compilation into multiple database engines. For example, if you wanted to
maintain a single database schema for both MongoDB and ElasticSearch. 
Properties can also be documented inline and documentation compiled to HTML
or a console-friendly text document.

Currently, a schema is defined natively in Python. Example::

    Record({
        "name":String(required=True),
        "addresses":ListOf(SubRecord({
            "street":String(),
            "zipcode":String()
        }),
        "area_code":Integer()
    })

Command Line Interface
======================

zschema [command] [schema] [file]

Commands:

    - elasticsearch (compile to Elastic Search)

    - bigquery (compile to Google BigQuery)

    - json (compile documentation to JSON)

    - text (compile documentation to plain text)

    - html (compile documentation to HTML)

    - validate (validate JSON file (one document per line) against schema)

The schema file can be defined on the command line as module:var.
