[![Build Status](https://travis-ci.org/zmap/zschema.svg?branch=master)](https://travis-ci.org/zmap/zschema)

ZSchema
=======

ZSchema is a generic (meta-)schema language for defining database schemas
that facilitates (1) validating JSON documents against a schema definition and
(2) compilation into multiple database engines. For example, if you wanted to
maintain a single database schema for both MongoDB and ElasticSearch.
Properties can also be documented inline and documentation compiled to HTML
or a console-friendly text document.

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


Command Line Interface
----------------------

`zschema [command] [schema] [file]`

Commands:

 * elasticsearch (compile to Elastic Search)

 * bigquery (compile to Google BigQuery)

 * json (compile documentation to JSON)

 * text (compile documentation to plain text)

 * html (compile documentation to HTML)

 * validate (validate JSON file (one document per line) against schema)

The schema file can be defined on the command line as module:var.

Running Tests
-------------

Tests are run with [nose](http://nose.readthedocs.io/en/latest/). Run them via `python setup.py test`.


License and Copyright
---------------------

ZMap Copyright 2017 Regents of the University of Michigan

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See LICENSE for the specific
language governing permissions and limitations under the License.
