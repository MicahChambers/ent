#!/usr/bin/python3

if len(sys.argv) < 5:
    print "args missing"
    sys.exit(1)
 
hostname = sys.argv[1]
password = sys.argv[2]
source = sys.argv[3]
dest = sys.argv[4]
 
username = "root"
port = 22
 
