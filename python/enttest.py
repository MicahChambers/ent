#!/usr/bin/python3

import time
import ent
import sys
import os

def main(args):
    entobj = ent.Ent()

    if len(args) == 2:
        entobj.load(args[0], args[1])
    else:
        entobj.load("ENTFILE", 'testmd5.json')

##    # open connections
##    entobj.connect()
##
    # submit jobs
    try:
        entobj.simulate()
        for i in range(10):
            print(i)
            time.sleep(1)
        entobj.run()
#        entobj.genmakefile('test.make')
#        entobj.submit()
    except:
        print("ERROR")
        sys.exit(-1)

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
