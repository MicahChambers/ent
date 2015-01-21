#!/usr/bin/python3

import time
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
    try:
        entobj.simulate()
        for i in range(10):
            print(i)
            time.sleep(1)
        entobj.run('testmd5.json')
#        entobj.genmakefile('test.make')
#        entobj.submit()
    except InputError as e:
        print(e.msg)
        sys.exit(-1)
    except:
        print("ERROR")
        sys.exit(-1)

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
