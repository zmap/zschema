import sys
import os.path
import json
import zschema.registry
import argparse

from imp import load_source
from importlib import import_module
from site import addsitedir

from leaves import *
from keys import *
from compounds import *

commands = [
    "bigquery",
    "elasticsearch",
    "docs-bq",
    "docs-es",
    "validate",
    "flat",
    "json"
]

cmdList = ", ".join(commands)

parser = argparse.ArgumentParser(
    prog="zschema",
    description="Process a zschema definition. "
                "VERSION: %s" % zschema.__version__)

parser.add_argument("command",
                    metavar="command", choices=commands,
                    help="The command to execute; one of [ %s ]" % cmdList)

parser.add_argument("schema",
                    help="The name of the schema in the zschema.registry. "
                         "For backwards compatibility, a filename can be "
                         "prefixed with a colon, as in 'schema.py:my-type'.")

parser.add_argument("target", nargs="?",
                    help="Only used for the validate command. "
                         "The input JSON file that will be checked against "
                         "the schema.")

parser.add_argument("--module", help="The name of a module to import.")

parser.add_argument("--validation-policy", help="What to do when a validation "
        "error occurs. This only overrides the top-level Record. It does not "
        "override subrecords. Default: error.", choices=["ignore", "warn", "error"],
        default=None)

parser.add_argument("--validation-policy-override", help="Override validation "
        "policy for all levels of the schema.", choices=["ignore", "warn", "error"],
        default=None)

parser.add_argument("--path", nargs="*",
                    help="Additional PYTHONPATH directories to include.")

args = parser.parse_args()


def main():
    if args.path:
        for syspath in args.path:
            addsitedir(syspath)

    schema = args.schema

    # Backwards compatibility: given "file.py:schema", load file.py.
    if ":" in schema:
        path, recname = schema.split(":")
        load_source('module', path)
        schema = recname

    if args.module:
        import_module(args.module)

    record = zschema.registry.get_schema(schema)
    if args.validation_policy:
        record.set("validation_policy", args.validation_policy)
    command = args.command
    if command == "bigquery":
        print json.dumps(record.to_bigquery())
    elif command == "elasticsearch":
        print json.dumps(record.to_es(recname))
    elif command == "docs-es":
        print json.dumps(record.docs_es(recname))
    elif command == "docs-bq":
        print json.dumps(record.docs_bq(recname))
    elif command == "json":
        print record.to_json()
    elif command == "flat":
        for r in record.to_flat():
            print json.dumps(r)
    elif command == "validate":
        if not os.path.exists(args.target):
            sys.stderr.write("Invalid test file. %s does not exist.\n" % args.target)
            sys.exit(1)
        with open(args.target) as fd:
            for line in fd:
                record.validate(json.loads(line.strip()),
                        args.validation_policy_override)
    else:
        usage()


if __name__ == "__main__":
    main()
