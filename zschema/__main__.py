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
    sys.stderr.write("Valid commands: bigquery, elasticsearch, json, text, html, censys-html, flat, validate.\n")
    sys.stderr.write("schema should be defined as file.py:record\n")
    sys.stderr.write("VERSION: %s\n" % zschema.__version__)
    sys.exit(1)

def main():
    if len(sys.argv) < 3:
        usage()
    path, recname = sys.argv[2].split(":")
    module = load_source("module", path)
    record = zschema.registry.get_schema(recname)
    command = sys.argv[1]
    if command == "bigquery":
        print json.dumps(record.to_bigquery())
    elif command == "elasticsearch":
        print json.dumps(record.to_es(recname))
    elif command == "json":
        print record.to_json()
    elif command == "html":
        for r in record.to_flat():
            type_ = r.get("es_type", "")
            print "<tr><td>%s</td><td>%s</td></tr>" % (r["name"], type_)
    elif command == "text":
        print record.to_text()
    elif command == "flat":
        for r in record.to_flat():
            print r
    elif command == "censys-html":
        for r in record.to_flat():
            type_ = r.get("es_type", None)
            len_ = r["name"].count(".")
            style = 'style="padding-left: %ipx"' % (15 * len_ + 5)
            if not type_:
                print '<tr class="record"><td %s>%s</td><td>%s</td></tr>' % (style, r["name"], "")
            else:
                print "<tr><td %s>%s</td><td>%s</td></tr>" % (style, r["name"], type_)
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

