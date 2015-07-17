import sys
import os.path
import json

from imp import load_source

from leaves import *
from keys import *
from compounds import *
from registry import *

def usage():
    sys.stderr.write("USAGE: %s command schema [file].\n" % sys.argv[0].split("/")[-1])
    sys.stderr.write("Valid commands: bigquery, elasticsearch, json, text, html, validate.\n")
    sys.stderr.write("schema should be defined as file.py:record\n")
    sys.exit(1)

def main():
    if len(sys.argv) < 3:
        usage()

    path, recname = sys.argv[2].split(":")
    module = load_source("module", path)
    record = get_schema(recname) 
    command = sys.argv[1]
    if command == "bigquery":
        print json.dumps(record.to_bigquery())
    elif command == "elasticsearch":
        print json.dumps(record.to_es(recname))
    elif command == "json":
        print record.to_json()
    elif command == "html":
        print record.to_html()
    elif command == "text":
        print record.to_text()
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
