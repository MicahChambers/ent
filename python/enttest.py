#!/usr/bin/python3

import ent
import sys

def main(args):
    entobj = ent.Ent()

    if len(args) > 0:
        entobj.parseV1(args[0])
    else:
        entobj.parseV1("ENTFILE")

    entobj.batch()

if __name__ == "__main__":
    main(sys.argv[1:])
