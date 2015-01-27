#!/ifs/students/mchambers/anaconda/envs/py3k/bin/python

import sys

if len(sys.argv) < 3:
	print("Requires at least 3 arguments:\nmakefile.py out txt txt .. ")
	sys.exit(-1)

with open(sys.argv[1], 'w') as outfile:
	for instr in sys.argv[2:]:
		outfile.write(instr + ' ')

