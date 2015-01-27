#!/ifs/students/mchambers/anaconda/envs/py3k/bin/python

import sys

if len(sys.argv) < 3:
	print("Requires at least 3 arguments:\ncatfiles.py out in in in ...")
	sys.exit(-1)

with open(sys.argv[1], "w") as outfile:
	for infn in sys.argv[2:]:
		with open(infn, 'r') as inf:
			outfile.write(inf.read())
