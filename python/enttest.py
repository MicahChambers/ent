#!/usr/bin/python3

import ent
import sys

def main(argv):
    entobj = ent.Ent()
    entobj.parseV1("ENTFILE")

if __name__ == "__main__":
    main(sys.argv)
