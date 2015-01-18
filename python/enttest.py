#!/usr/bin/python3

import ent
import sys
import os

def main(args):
    entobj = ent.Ent('localhost', 12345)

    if len(args) > 0:
        entobj.load(args[0])
    else:
        entobj.load("ENTFILE")

##    # open connections
##    entobj.connect()
##
    # submit jobs
#    entobj.simulate()
    entobj.submit()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
