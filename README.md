ZSchema
=======

[![Build Status](https://travis-ci.org/zmap/zschema.svg?branch=master)](https://travis-ci.org/zmap/zschema)

ZSchema is a generic (meta-)schema language for defining database schemas. It
facilitates (1) validating JSON documents against a schema definition and (2)
compilating a schema to multiple database engines. For example, if you wanted
to maintain a single database schema for both MongoDB and ElasticSearch.
Properties can also be documented inline and documentation compiled to HTML or
a console-friendly text document.

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
you don't in have JSON when you're defining a schema. For example, you can
reuse components without redefining them or define metaclasses for slighty
different parts of the schema. Overall, ZSchema has a higher learning curve
than the languages that ZSchema can compile. However, it makes defining complex
schemas much easier.

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

ZSchema allows compiling a registered `Record` to a schema file that can be
read by another service. For example, if you have the following schema in a
module named `myschema`:

```python
p = Record({
    "name":String(required=True),
    "addresses":ListOf(SubRecord({
        "street":String(),
        "zipcode":String()
    }),
    "area_code":Integer()
})
zschema.registry.register_schema("person", p)
```

Then you can compile this to Elasticsearch by running the following:
```
zschema elasticsearch myschema:person
```

Validating a Schema
-------------------

If you wanted to validate a JSON file containing data, you can pass this in along with the schema:

```
zschema validate myschema:person people.json
```


Developing a Schema
===================

Schemas are created by defining a `Record` object. Records are a set of named
fields (and their associated types). They can also contain lists of fields and
subrecords. Below is a very simple record:

```python
Record({
    "name":String(required=True),
    "address":SubRecord({
        "street":String(),
        "zipcode":ZipCode()
    }),
    "area_code":Integer(),
	"emails":ListOf(EmailAddress()),
	"enabled":Boolean(),
})
```

You will immediately notice a few things:

 * Fields are instantiated classes and can take initialization options

 * You can have customized fields (e.g., `EmailAddress`). These are useful for both
   maintaining your sanity as well as adding additional validation logic.


These types are known as _leaves_ and you can find the full list here:
https://github.com/zmap/zschema/blob/master/zschema/leaves.py. You'll likely
notice that many are Elasticsearch themed (e.g., `EnglishString`,
`AnalyzedString`), but these will compile down to normal types in other systems
too.

One of the benefits of ZSchema is that you can define and embed subrecords
other places:

```python
address = SubRecord({
    "street":String(),
    "zipcode":ZipCode()
    "country":String()
})

Record({
    "name":String(required=True),
    "business_address":ListOf(address),
    "home_address":ListOf(address),
    "area_code":Integer(),
	"emails":ListOf(EmailAddress()),
	"enabled":Boolean(),
})
```

One thing that needs to be careful of here is that all `address` entries here
point to the exact same Python object, so you cannot customize one without
changing all. To support this use case (which comes up frequently because
different fields will have different documentation), you can create a new `SubRecordType`:

```python

Address = SubRecordType({
    "street":String(),
    "zipcode":ZipCode()
    "country":String()
})

Record({
	home:Address(doc="Home Address"),
	work:Address(doc="Home Address"),
})

```

Similar to `doc`, fields can have a description, examples, units, min/max
values, etc. A full list of attributes can be found here:
https://github.com/zmap/zschema/blob/master/zschema/leaves.py#L25.




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
