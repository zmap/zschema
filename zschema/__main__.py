import sys
import os.path
import json
import zschema.registry

from imp import load_source

from leaves import *
from keys import *
from compounds import *

def usage():
    sys.stderr.write("USAGE: %s command schema [file].\n" % sys.argv[0].split("/")[-1])
    sys.stderr.write("Valid commands: bigquery, elasticsearch, docs-es, docs-bq, json, flat, validate.\n")
    sys.stderr.write("Schema should be passed as file.py:record\n")
    sys.stderr.write("The optional 'file' argument is used only as the test file for the 'validate' command.\n")
    sys.stderr.write("VERSION: %s\n" % zschema.__version__)
    sys.exit(1)

def main():
    if len(sys.argv) < 3:
        usage()
    path, recname = sys.argv[2].split(":")
    old_cwd = os.getcwd()
    dir, file = os.path.dirname(path), os.path.basename(path)
    os.chdir(dir)
    try:
        module = load_source("module", file)
    finally:
        os.chdir(old_cwd)
    record = zschema.registry.get_schema(recname)
    command = sys.argv[1]
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
        if not os.path.exists(sys.argv[3]):
            sys.stderr.write("Invalid test file. %s does not exist.\n" % sys.argv[3])
            sys.exit(1)
        with open(sys.argv[3]) as fd:
            for line in fd:
                record.validate(json.loads(line.strip()))
    else:
        usage()

if __name__ == "__main__":
    main()
