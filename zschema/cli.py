import sys
import os.path
import json

from imp import load_source

from leaves import *
from keys import *
from compounds import *

def usage():
    print "USAGE: %s command schema [file]." % sys.argv[0].split("/")[-1]
    print "Valid commands: bigquery, elasticsearch, json, text, html, validate."
    print "schema should be defined as file.py:record"
    sys.exit(1)


def main():
    if len(sys.argv) < 3:
        usage()

    path, recname = sys.argv[2].split(":")
    module = load_source("module", path)
    record = getattr(module, recname)

    command = sys.argv[1]
    if command == "bigquery":
        print record.to_bigquery()
    elif command == "elasticsearch":
        print record.to_es(recname)
    elif command == "json":
        print record.to_json()
    elif command == "html":
        print record.to_html()
    elif command == "text":
        print record.to_html()
    elif command == "validate":
        assert os.path.exists(sys.argv[3])
        with open(sys.argv[3]) :
            for line in fd:
                record.validate(json.loads(line.strip()))
    else:
        usage()


if __name__ == "__main__":
    main()
