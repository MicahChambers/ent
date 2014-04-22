#!/usr/bin/python3

import ent
import sys
import os

def main(args):
    entobj = ent.Ent(os.getcwd())

    if len(args) > 0:
        ret = entobj.parseV1(args[0])
    else:
        ret = entobj.parseV1("ENTFILE")

    if ret != 0:
        return ret 

    entobj.dumpJobs()
    entobj.pushCache()

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
