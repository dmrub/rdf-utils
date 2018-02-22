#!/usr/bin/env python
from __future__ import print_function

import sys
import hashlib
import argparse
import logging
import rdflib
import rdflib.util
from rdfutils import calc_hash_value

INPUT_FORMATS = [i.name for i in rdflib.plugin.plugins(kind=rdflib.parser.Parser)]


def hash_file(file_name, format="auto", hash="sha256"):
    if file_name == '-' or file_name == '':
        if format == "auto":
            raise Exception("Cannot guess RDF format from stdin")
        data = sys.stdin.read()
        graph = rdflib.Graph()
        graph.parse(data=data, format=format)
    else:
        if format == 'auto':
            format = rdflib.util.guess_format(file_name)
        graph = rdflib.Graph()
        graph.parse(file_name, format=format)
    rdf_hash = calc_hash_value(graph)
    if hash != 'none':
        hash_func = hashlib.new(hash)
        hash_func.update(rdf_hash.encode('utf-8'))
        rdf_hash = hash_func.hexdigest()
    return rdf_hash


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Compute hash from RDF graph",
        epilog="supported RDF file formats: {}".format(', '.join(['auto'] + INPUT_FORMATS))
    )
    parser.add_argument("-d", "--debug", action="store_true",
                        help="enable debug mode")
    parser.add_argument("-a", "--hash", choices=('none', 'sha256'),
                        default="sha256",
                        help="hash function")
    parser.add_argument("-I", "--input-format", metavar="FORMAT",
                        choices=['auto'] + INPUT_FORMATS,
                        default="auto",
                        help="input RDF format")
    parser.add_argument("--version", action="version",
                        version="%(prog)s 0.1")
    parser.add_argument('files', metavar='FILE', type=str, nargs='+',
                        help='RDF files')
    args = parser.parse_args(sys.argv[1:])

    logging.basicConfig()

    # Configure application
    for fn in args.files:
        hash_value = hash_file(fn, format=args.input_format, hash=args.hash)
        print("{}  {}".format(hash_value, fn))
