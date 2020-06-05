[![Build Status](https://travis-ci.org/zmap/zschema.svg?branch=master)](https://travis-ci.org/zmap/zschema)

ZSchema
=======

ZSchema is a generic (meta-)schema language for defining database schemas. It
facilitates (1) validating JSON documents against a schema definition and (2)
compilating a schema to multiple database engines. For example, if you wanted to
maintain a single database schema for both MongoDB and ElasticSearch.
Properties can also be documented inline and documentation compiled to HTML or a
console-friendly text document.

Schemas are defined in native Python code. Example:

```python
Record({
    "name":String(required=True),
    "addresses":ListOf(SubRecord({
        "street":String(),
        "zipcode":String()
    }),
    "area_code":Integer()
})
```

While this might initially seem strange, Python provides a lot flexibility that
you don't in have JSON when you're defining a schema. For example, you can reuse
components without redefining them or define metaclasses for slighty different
parts of the schema. Overall, ZSchema has a higher learning curve than the
languages that ZSchema can compile. However, it makes defining complex schemas
much easier.

Running ZSchema
===============

Command Line Interface
----------------------

`zschema [command] [schema] [file (optional)]`

Commands:

 * elasticsearch (compile to Elastic Search)

 * bigquery (compile to Google BigQuery)

 * json (compile documentation to JSON)

 * proto (compile documentation to proto3)

 * text (compile documentation to plain text)

 * html (compile documentation to HTML)

 * validate (validate JSON file (one document per line) against schema)

The schema file can be defined on the command line as module:var. File is only
needed when validating whether a data file matches a schema (i.e., using
`validate` command).


Compiling a Schema
------------------


Validating a Schema
-------------------


Developing a Schema
===================




Running Tests
=============

Tests are run with [nose](http://nose.readthedocs.io/en/latest/). Run them via
`python setup.py test`.


License and Copyright
=====================

ZSchema Copyright 2020 ZMap Team

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See LICENSE for the specific
language governing permissions and limitations under the License.
