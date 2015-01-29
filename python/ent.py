#!/usr/bin/python3

import argparse
import os, time
import itertools
import re
import copy
import sys
import json
import hashlib
import subprocess
from pathlib import Path

"""
Main Data Structures:
	Ent: Parser and Global Class

	Job: Hold a concrete set of Command Line Arguments, input and output files

	File: Holds a file name, and knowns which job generates it

	Requestor

	TODO Enforce 1 line per rule (or handle multi-line rules)
"""

## TODO handle quotes in variable definitions
md5re = re.compile('([0-9]{32})  (.*)|md5sum: (.*)')
gvarre = re.compile('(.*?)\${\s*([a-zA-Z0-9_.]+)\s*}(.*)')

VERBOSE=10

decodestatus = {}

def initdrmaa():
		import drmaa
		global decodestatus
		decodestatus = {
			drmaa.JobState.UNDETERMINED: 'process status cannot be determined',
			drmaa.JobState.QUEUED_ACTIVE: 'job is queued and active',
			drmaa.JobState.SYSTEM_ON_HOLD: 'job is queued and in system hold',
			drmaa.JobState.USER_ON_HOLD: 'job is queued and in user hold',
			drmaa.JobState.USER_SYSTEM_ON_HOLD: 'job is queued and in user and system hold',
			drmaa.JobState.RUNNING: 'job is running',
			drmaa.JobState.SYSTEM_SUSPENDED: 'job is system suspended',
			drmaa.JobState.USER_SUSPENDED: 'job is user suspended',
			drmaa.JobState.DONE: 'job finished normally',
			drmaa.JobState.FAILED: 'job finished, but failed',
		}

def main():
	parser = argparse.ArgumentParser(description='Run ENT on an ent script')
	parser.add_argument('--script', '-f', type=str,
			help='Input script file', required=True)
	parser.add_argument('--state', '-s', type=str, nargs=1,
			default="", help='State file (to store md5 sums in')
	parser.add_argument('--simulate', '-x', action='store_const', const=True,
			default="", help='Simulate running rather than actually running')

	args = parser.parse_args()

	if args.state:
		statename = args.state[0]
	else:
		statename = None

	entobj = Ent(args.script, statename)

	if args.simulate:
		entobj.simulate()
	else:
		try:
			entobj.run()
			initdrmaa()
		except:
			e = sys.exc_info()[0]
			print("Error: %s" % e )

		try:
			with open(statename, "w") as of:
				sums = {f.path: f.md5sum for f in entobj.files.values() if f.md5sum}
				json.dump(sums, of, indent=1)
		except IOError as e:
			print("I/O error({0}): {1}".format(e.errno, e.strerror))

class InputError(Exception):
	"""Exception raised for errors in the input.
	   Attributes:
	   expr -- input expression in which the error occurred
	   msg  -- explanation of the error
	"""

	def __init__(self, expr, msg):
		self.expr = expr
		self.msg = msg

##############################################
# Helper Functions
##############################################

def md5sum(fname):
	out = dict()
	if type(fname) != type([]):
		fname = [fname]

	for ff in fname:
		try:
			p = Path(ff)
			m = hashlib.md5()
			stat = p.stat()
			m.update(str(stat.st_mtime).encode())
			out[str(p)] = m.hexdigest()
		except FileNotFoundError:
			pass
		except OSError as e:
			pass

	return out

def parseV2(filename):
	"""Reads a file and returns Generators, and variables as a tuple

	This is a JSON format:

	{
		"generators": [
			{
				"inputs" : ["{SUBJECT}/image"],
				"outputs" : ["{SUBJECT}/outimage"]
				"commands" : ["cp $< $>", "touch $<"]
			},
			...etc
		]
		"variables": {
			"subjects" : ["a", "b", "c"],
			...etc
		}
	}

	"""

	varre = re.compile('\s*([a-zA-Z0-9_.]+)(?:\[\s*([a-zA-Z0-9_.]+)\s*\])?\s*')
	variables = dict()
	jobs = []
	with open(filename, "r") as f:
		infile = json.load(f)
		variables = infile['variables']

		# Check Variable
		outvariables = {}
		for kk,vv in variables.items():
			if type(vv) != type([]):
				vv = [vv]

			m = varre.match(kk)
			if m.group(2):
				# there is a dependent variable, indicate in value
				vv = (vv, m.group(2))
				kk = m.group(1)

			# Add variable
			outvariables[kk] = vv

		for g in infile['generators']:
			inputs = g['inputs']
			outputs = g['outputs']
			commands = g['commands']
			if type(inputs) != type([]):
				inputs = [inputs]
			if type(outputs) != type([]):
				outputs = [outputs]
			if type(commands) != type([]):
				commands = [commands]

			jobs.append(Generator(inputs, outputs))
			jobs[-1].cmds = commands

	return (jobs, outvariables)

def parseV1(filename):
	"""Reads a file and returns Generators, and variables as a tuple

	Variable names may be [a-zA-Z0-9_]*
	Variable values may be anything but white space, multiple values may
	be separated by white space

	"""
	iomre = re.compile("\s*([^:]*?)\s*:(?!//)\s*(.*?)\s*")
	varre = re.compile("\s*([a-zA-Z0-9_.]+)(?:\[\s*([a-zA-Z0-9_.]+)\s*\])?\s*=\s*(.*?)\s*")
	cmdre = re.compile("\t\s*(.*?)\s*")
	commentre = re.compile("(.*?)#.*")

	with open(filename, "r") as f:
		lines = f.readlines()

	lineno=0
	fullline = ""
	cgen = None

	# Outputs
	jobs = []
	variables = {'.PWD' : os.getcwd()}

	## Clean Input
	# Merge Lines that end in \ and remove trailing white space:
	buff = []
	tmp = ""
	for line in lines:
		if line[-2:] == "\\\n":
			tmp += line[:-2]
		elif line[-3:] == "\\\r\n":
			tmp += line[:-3]
		else:
			buff.append(tmp+line.rstrip())
			tmp = ""
	lines = buff

	# Remove comments
	for ii in range(len(lines)):

		# remove comments
		match = commentre.search(lines[ii])
		if match:
			lines[ii] = match.group(1)

	for line in lines:
		if VERBOSE > 4: print("line %i:\n%s" % (lineno, line))

		# if there is a current Generator we are building
		# then first try to append commands to it
		if cgen:
			cmdmatch = cmdre.fullmatch(line)
			if cmdmatch:
				# append the extra command to the Generator
				cgen.cmds.append(cmdmatch.group(1))
				continue
			else:
				# done with current Generator, remove current link
				jobs.append(cgen)
				if VERBOSE > 0: print("Adding Generator: %s" % cgen)
				cgen = None

		# if this isn't a command, try the other possibilities
		iomatch = iomre.fullmatch(line)
		varmatch = varre.fullmatch(line)
		if iomatch:
			# expand inputs/outputs
			inputs = re.split("\s+", iomatch.group(2))
			outputs = re.split("\s+", iomatch.group(1))
			inputs = [s for s in inputs if len(s) > 0]
			outputs = [s for s in outputs if len(s) > 0]

			# create a new generator
			cgen = Generator(inputs, outputs)
			if VERBOSE > 3: print("New Generator: %s:%s" % (inputs, outputs))
		elif varmatch:
			# split variables
			name = varmatch.group(1)
			values = re.split("\s+", varmatch.group(3))
			if name in variables:
				print("Error! Redefined variable: %s" % name)
				return (None, None)
			if VERBOSE > 3: print("Defining: %s = %s"%(name, str(values)))

			if varmatch.group(2):
				# dependent variables are tuples with the values, with the dep
				variables[name] = (values, varmatch.group(2))
			else:
				variables[name] = values
		else:
			continue

	if VERBOSE > 3: print("Done With Initial Pass!")

	return (jobs, variables)

def expand_variables(inlist, localvars, gvars):
	# This is more complicated than it might seem because
	# [${SUBJECT} ${SUBJECT}] might exist in the output so a the same
	# variable should not be expanded twice. To handle this, we need to
	# create lists of dictionaries of local variables that represent one
	# expansion
	varre = re.compile('\${\s*([a-zA-Z0-9_.]+)\s*}')

	if not inlist:
		return [], []

	# 1) find all variables in outputs
	# 2) expand each variable into the list of values it takes on
	# 3) create list of every combination of every list variable as a
	# dictionary of local variables
	# 4) repeat on 1) on the expanded outputs
	ii = 0
	valreal = [copy.deepcopy(localvars)]
	outputs = [copy.deepcopy(inlist)]
	while ii < len(outputs):
		# Find All Variables, Across Output Files
		curout = copy.deepcopy(outputs[ii])
		localvars = copy.deepcopy(valreal[ii])
		expvars = []
		depvars = []
		change = False
		# Convert [file, file, file] to [[partial, var, partial], ...]
		for jj in range(len(curout)):
			print(curout[jj])
			parscur = []
			prevend = 0
			for match in varre.finditer(curout[jj]):
				parscur.append(curout[jj][prevend:match.start()])
				parscur.append(match.group(0))
				prevend = match.end()
				change = True
				iv = match.group(1)
				if iv in localvars:
					pass
				elif iv in gvars and type(gvars[iv]) == type(()):
					# if this variable depends on another
					expvars.append(gvars[iv][1]) # expand parent
					depvars.append(iv) # save this as a dependent variable
				elif iv in gvars:
					expvars.append(iv)
				else:
					raise InputError("genJobs", "Error! Unknown global "
							" variable reference: %s" % iv)
			parscur.append(curout[jj][prevend:])
			if parscur:
				curout[jj] = [p for p in parscur if p]
			else:
				curout[jj] = [curout[jj]]

		# make expansion vars unique and check for higher depth dependencies
		# which are note allowed
		expvars = list(set(expvars))
		for var in expvars:
			if type(gvars[var]) == type(()):
				print("Error variable %s is a dependent on variable but "\
						"depends on %s" % (var, gvars[var][1]))

		# expand variable into list which includes the values from
		# localvars
		newlocalvars = []
		for varset in itertools.product(*[gvars[e] for e in expvars]):
			newlvar = copy.deepcopy(localvars)
			for i in range(len(varset)):
				newlvar[expvars[i]] = varset[i]
			newlocalvars.append(newlvar)

		# Add variable definitions for all the dependent variables based
		# on its dependencies, so for EXTRA[SUBJ] = a b c, lookup SUBJ
		# convert 10588 10659 10844 to indexes
		for depvar in depvars:
			depval = gvars[depvar]
			parentvar = depval[1]
			parentval = gvars[depval[1]]
			for varset in newlocalvars:
				varset[depvar] = depval[0][parentval.index(varset[parentvar])]

		# current set of outputs now has a new set of local vars for every
		# combination of variables. Currout has already been split based on
		# variable so just take replace the variables
		newout = [[copy.deepcopy(j) for j in curout] for i in newlocalvars]
		for kk in range(len(newout)):
			for jj in range(len(newout[kk])):
				for ll in range(len(newout[kk][jj])):
					m = varre.match(newout[kk][jj][ll])
					# expand variables
					if m and m.group(1):
						newout[kk][jj][ll] = newlocalvars[kk][m.group(1)]
				# Re-merge the list to a string
				newout[kk][jj] = "".join(newout[kk][jj])
		if change:
			outputs.extend(newout)
			valreal.extend(newlocalvars)
			del(outputs[ii])
			del(valreal[ii])
		else:
			ii += 1
	return outputs, valreal

## Expand String Based on Local (First) then Global Variables
# only expands variables that match ${[a-zA-Z0-9_]}
def resolve_variables(inlist, localvars, gvars):
	# the expanded version (for instance if there are multi-value)
	# arguments
	ii = 0
	print("Expanding", inlist, localvars)
	outlist = copy.deepcopy(inlist)
	while ii < len(outlist):
		tmp = outlist[ii]
		match = gvarre.fullmatch(tmp)
		if match:
			pref = match.group(1)
			name = match.group(2)
			suff = match.group(3)

			# resolve variable name
			if name in localvars:
				# variable in the list of output vars, just sub in
				outlist[ii] = pref + localvars[name] + suff
			elif name in gvars:
				# if it is a global variable, then we don't have a
				# value from output, if there are multiple values
				# then it is a compound (multivalue) input
				realvals = gvars[name]
				if len(realvals) == 1:
					tmp = pref + realvals[0] + suff
					outlist[ii] = tmp
				else:
					raise InputError("resolve_variables", "Error cannot place "
							"variables length variable in a command")
			else:
				raise InputError('expand_string', 'Error, input ' + \
						'"%s" references variable "%s"' % (instr, name) + \
						'which is a unknown!')
		else:
			ii += 1

	return outlist

def split_command(instr):
	quote_re = re.compile(r'((?:(?<!\\)(?:\\\\)*|[^\\]))"')

	##################################
	# Split up Quoted Regions
	##################################
	spl = []
	start = 0
	quote = False
	for m in quote_re.finditer(instr):
		prev = 0
		if quote:
			# if we are in a quote then stop at end of match
			spl.append(instr[start:m.end()])
			start = m.end()
		else:
			# otherwise stop at end-1
			spl.append(instr[start:m.end()-1])
			start = m.end()-1
		quote = not quote

		# if the command has a quote (that is preceed by an even number of
		# back slashes) then start quote

	if quote:
		raise "Error, unmatched quotes in input!"
	else:
		spl.append(instr[start:])

	##################################
	# Split Unquoted Regions on space
	##################################
	newspl = []
	currword = ""
	for word in spl:
		if not word:
			continue

		# Keep Entire quoted string together
		if word[0] == '"':
			if word[-1] != '"':
				raise "ERROR PARSING"
			currword += word[1:-1]
			print('updated currword: "%s"' % currword)
		else:
			tmp = re.split('\s+', word)
			if currword:
				tmp[0] = currword + tmp[0]
				currword = ""
			newspl.extend(tmp)

	if currword:
		newspl.append(currword)

	return [s for s in newspl if s]

##
# @brief Parses a string with ${<} type syntax with the contents of
# defs. If a variable contains more variable those are resolved as well.
# Special definitions:
# ${<} replaced with all inputs
# ${<N} where N is >= 0, replaced the the N'th input
# ${>} replaced with all outputs
# ${>N} where N is >= 0, replaced the the N'th output
#
# @param inputs List of Files
# @param outputs List of List of Files
#
# @return
def expand_args(string, inputs, outputs):
	# $> - 0 - all outputs
	# $< - 1 - all inputs
	# ${>#} - 2 - output number outputs
	# ${<#} - 3 - output number outputs

	allout  = "(\$>|\${>})|"
	allin   = "(\$<|\${<})|"
	singout = "\${>\s*([0-9]*)}|"
	singin  = "\${<\s*([0-9]*)}"
	r = re.compile(allout+allin+singout+singin)

	## Convert Inputs Outputs to Strings
	inputs = [f.path for f in inputs]
	outputs = [f.path for f in outputs]

	ostring = string
	m = r.search(ostring)
	ii = 0
	while m:
		ii += 1
		# perform lookup
		if m.group(1):
			# all out
			insert = " ".join(outputs)
		elif m.group(2):
			# all in
			insert = " ".join(inputs)
		elif m.group(3):
			# singout
			i = int(m.group(3))
			if i < 0 or i >= len(outputs):
				raise InputError("expand", "Error Resolving Output number "
						"%i in\n%s" % (i, string))
			insert = outputs[i]
		elif m.group(4):
			# singin
			i = int(m.group(4))
			if i < 0 or i >= len(inputs):
				raise InputError("expand", "Error Resolving input number "
						"%i in\n%s" % (i, string))
			insert = inputs[i]

		ostring = ostring[:m.start()] + insert + ostring[m.end():]
		m = r.search(ostring)
		if ii == 100:
			raise InputError("Circular Variable References Detected!")

	return ostring

###############################################################################
# Classes
###############################################################################

class Ent:
	""" Main Ent Class, all others are derived from this. This stores global
	variables, all jobs, all files etc.

	This class owns all jobs and files

	Organization:
	Generator: This is a generic rule of how a particular set of inputs generates
	a particular set of outputs.

	"""

	def __init__(self, entfile = None, statefile = None):
		""" Ent Constructor """
		self.error = 0
		self.files = dict()
		self.variables = {'.PWD' : os.getcwd()}
		self.jobs = list()
		self.md5state = statefile

		# load the file
		if entfile:
			self.load(entfile)

	def load(self, entfile):
		print(entfile)
		if entfile[-5:] == '.json':
			print("Parsing json")
			geners, self.variables = parseV2(entfile)
		elif entfile[-4:] == '.ent':
			print("Parsing ent")
			geners, self.variables = parseV1(entfile)
		else:
			raise 'unknown file type'

		# expand all the Generators into Jobs
		for bb in geners:
			# get jobs and files this generates
			jlist = bb.genJobs(self.files, self.variables)
			if jlist == None:
				return -1
			# Resolve Variable Names in Commands
			for job in jlist:
				print("Expanding input/output arguments for \n%s" % str(job))
				print(job.inputs)
				print(job.outputs)
				print(job.cmds)
				for ii in range(len(job.cmds)):
					job.cmds[ii] = expand_args(job.cmds[ii], job.inputs, \
							job.outputs)

			# add jobs to list of jobs
			self.jobs.extend(jlist)

		if VERBOSE > 6:
			print("All Jobs: ")
			for j in self.jobs:
				print(j)

		##################################################################
		# Read MD5 State From File, and compare with current MD5s
		##################################################################

		# Update File database with current md5sums
		flist = [f.path for f in self.files.values()]
		newsums = md5sum(flist)
		print("MD5 Sums in Filesystem: %s" % json.dumps(newsums, indent=1))
		for f in self.files.values():
			if f.path in newsums:
				f.md5sum = newsums[f.path]

		# Read State File
		try:
			with open(self.md5state, "r") as f:
				sums = json.load(f)
			print("MD5 Sums from State File: %s" % json.dumps(sums, indent=1))
		except:
			if self.md5state:
				print("Warning %s does not exist" % self.md5state)
			else:
				print("Warning no state file given, no state will be read "
						"or saved")
			sums = dict()

		# Update Job Status' for jobs that have matching MD5's
		for job in self.jobs:
			ready = True
			print(job)
			for out in job.outputs:
				p = out.path
				if p not in sums or not out.md5sum or sums[p] != out.md5sum:
					ready = False
					break

			if ready:
				job.status = 'SUCCESS'
				print("All Outputs Exist for Job: %s\n" % str(job.cmds))
			else:
				job.status = 'WAITING'

	def run(self):
		# Set up Grid Engine
		self.gridengine = drmaa.Session()
		self.gridengine.initialize()
		template = self.gridengine.createJobTemplate()

		# Queues. Jobs move from waitqueue to startqueue, startqueue to either
		# running or donequeue, and running to startqueue or (in case of error)
		waitqueue = [j for j in self.jobs if j.status == 'WAITING'] # jobs with unmet depenendencies
		running = {}   # jobs that are currently running
		donequeue = [j for j in self.jobs if j.status == 'SUCCESS']  # jobs that are done
		startqueue = [] # Jobs the need to be started
		while waitqueue or running:
			## Move Any Jobs that we can to startqueue
			for job in waitqueue:
				# check if inputs have been produced
				ready = True
				for inp in job.inputs:
					if inp.genr.status == 'FAIL' or inp.genr.status == 'DEPFAIL':
						ready = False
						job.status = 'DEPFAIL'
						donequeue.append(job)
						del(waitqueue[waitqueue.index(job)])
						if VERBOSE > 1:
							print("Dependency Failed for:\n%s"%str(job))
						break
					elif inp.genr.status != 'SUCCESS':
						ready = False
				if ready:
					# Change to Queue State and Fill Command Queue
					if VERBOSE > 4:
						print("Job Ready to Run:\n%s"%str(job))
					job.status = 'RUNNING'
					job.cmdqueue = [cmd for cmd in job.cmds]
					startqueue.append(job)
					del(waitqueue[waitqueue.index(job)])

			## Start Jobs
			for job in startqueue:
				## Get the Next Job (If there is one)

				# Start the next job
				cmd = job.cmdqueue.pop(0)
				if VERBOSE > 1:
					print("Submitting Job:\n%s " % str(cmd))
				cmd = split_command(cmd)
				template.remoteCommand = cmd[0]
				template.args = cmd[1:]
				job.running_cmd = [template.remoteCommand]+template.args
				job.pid = self.gridengine.runJob(template)
				if VERBOSE > 2:
					print("PID: %s" % job.pid)
				job.status = 'RUNNING'
				running[job.pid] = job
				changed = True

			# Clear the start queue
			startqueue = []

			# Wait for a running job to finish
			jobinfo = self.gridengine.wait(
					drmaa.Session.JOB_IDS_SESSION_ANY,
					drmaa.Session.TIMEOUT_WAIT_FOREVER)
			job = running[jobinfo.jobId]
			del(running[jobinfo.jobId])
			print("Job:\n%s\n Finished with status: %s " % (job.running_cmd,
					jobinfo.exitStatus))
			if jobinfo.exitStatus == 0:

				if job.cmdqueue:
					startqueue.append(job)
				else:
					# No More Jobs Left, Check Outputs
					job.status = 'SUCCESS'
					donequeue.append(job)
			else:
				print("Job Failed: %s\n" % job)
				print("For Command: %s " % job.running_cmd)
				job.status = 'FAIL'
				donequeue.append(job)

		# Update MD5 Sums
		sums = md5sum([f.path for f in self.files.values()])
		for kk, vv in sums.items():
			self.files[kk].md5sum = vv

		self.gridengine.deleteJobTemplate(template)
		self.gridengine.exit()

	def genmakefile(self, filename):

		# Identify Files without Generators
		rootfiles = []
		for k,v in self.files.items():
			print(k)
			print(v)
			if v.genr == None:
				print("no generator")
				v.finished = True
				rootfiles.append(k)

		# Inform the user
		print("The Following Files Must Exist in the Filesystem:")
		for f in rootfiles:
			print(f)

		with open(filename, "w") as f:
			for job in self.jobs:
				## Create Dummy Jobs for multiple outputs

				## Write Inputs. Outputs
				f.write(' '.join([o.path for o in job.outputs]))
				f.write(': ')
				f.write(' '.join([i.path for i in job.inputs]))

				for cmd in job.cmds:
					try:
						cmd = " ".join(re.split("\s+", cmd))
						cmd = expand_args(cmd, job.inputs, job.outputs)
					except InputError as e:
						print("While Expanding Command %s" % cmd)
						print(e.msg)
						sys.exit(-1)
					f.write('\n\t%s' % cmd)
				f.write('\n')

	def simulate(self, statefile = None):

		# Identify Files without Generators
		rootfiles = []
		for k,v in self.files.items():
			print("file: ", k, v, v.genr)
			if v.genr == None:
				v.finished = True
				rootfiles.append(k)

		# Inform the user
		print("The Following Files Must Exist in the Filesystem:")
		for f in rootfiles:
			print(f)

		finished = set()
		outqueue = []
		skipqueue = []
		rqueue = [j for j in self.jobs]
		while len(rqueue) > 0:
			done = []
			for i, job in enumerate(rqueue):
				# Check if all the inputs are ready
				ready = True
				for infile in job.inputs:
					if infile.path not in finished:
					   ready = False
					   break

				if ready and job.status != 'SUCCESS':
					for out in job.outputs:
						finished.add(out.path)
					outqueue.append(' && '.join(job.cmds))
					done.append(i)
				if ready and job.status == 'SUCCESS':
					for out in job.outputs:
						finished.add(out.path)
					skipqueue.append(' && '.join(job.cmds))
					done.append(i)

			# In the Real Runner we will also have to check whether there
			# are processes that are waiting
			if len(done) > 0:
				done.reverse()
				for i in done:
					del(rqueue[i])
			else:
				print("Error The Following Job has Unresolved Dependencies!")
				for rr in rqueue:
					print(rr)
				raise InputError("Unresolved Dependencies")

		print("Jobs that are up to date")
		for q in skipqueue:
			print(q)
		print("Jobs to Run")
		for q in outqueue:
			print(q)

###############################################################################
# File Class
###############################################################################
class File:
	"""
	Keeps track of a particular files metdata. All jobs processes are wrapped
	in an md5 check, which returns success if it matches the previous value
	"""

	path = None  # final path to use for input/output
	force = ""      # force update of file even if file exists with the same md5
	md5sum = 0      # md5sum
	genr = None     # pointer to the Job which generates the file
	users = []      # List of Downstream Jobs that need this file
	finished = False

	def __init__(self, path):
		""" Constructor for File class.

		Parameters
		----------
		path : string
			the input/output file path that may be on the local machine or on
			any remote server
		"""

		self.path = str(Path(path))
		self.finished = False

		# Should be Updated By Job
		self.genr = None
		self.users = []
		self.md5sum = None

	# does whatever is necessary to produce this file
	def produce(self):
		if self.finished:
			return True
		elif self.genr:
			return self.genr.run()
		else:
			raise InputError(str(self.path), "No Generator Creates File")

	def __str__(self):
		if self.finished:
			return str(self.path) + " (done) "
		else:
			return str(self.path) + " (incomplete) "


###############################################################################
# Generator Class
###############################################################################
class Generator:
	"""
	A Generator is a job that has not been split up into Jobs. Thus each
	Generator may have any number of jobs associated with it.
	"""

	cmds = list()
	inputs = list()
	outputs = list()

	def __init__(self, inputs, outputs):
		self.cmds = list()
		self.inputs = copy.deepcopy(inputs)
		self.outputs = copy.deepcopy(outputs)

	def genJobs(self, gfiles, gvars):
		""" The "main" function of Generator is genJobs. It produces a list of
		Job classes (with concrete inputs and outputs)
		and updates files with any newly referenced files. Needs global
		gvars to resolve file names.

		Parameters
		----------
		gfiles : (modified) dict, {filename: File}
			global dictionary of files, will be updated with any new files found
		gvars : dict {varname: [value...] }
			global variables used to look up values

		"""
		# produce all the Jobs from inputs/outputs
		jobs = list()

		# store array of REAL output paths
		outputs = [self.outputs]

		# store array of dictionaries that produced output paths, so that the
		# same variables to be reused for inputs
		valreal = [dict()]
		outputs, valreal = expand_variables(self.outputs, dict(), gvars)
		if VERBOSE > 5:
			print('============ Generating Jobs From =============')
			print(self.outputs, self.inputs, self.cmds, outputs, valreal)
			print("Creating...")

		# now that we have expanded the outputs, just need to expand input
		# and create a job to store each realization of the expansion process
		for curouts, curvars in zip(outputs, valreal):
			# for each input, fill in variable values from outputs
			curins,trash = expand_variables(self.inputs, curvars, gvars)
			tmp = []
			for curin in curins:
				tmp.extend(curin)
			curins = tmp

			# insert finalized invals into curins
			if self.cmds:
				cmds = resolve_variables(self.cmds, curvars, gvars)
			else:
				cmds = []

			# change curins to list of Files, instead of strings by
			# finding inputs and outputs in the global files database, and then
			# pass them in as a list to the new job
			for ii, name in enumerate(curins):
				name = str(Path(name))
				if name in gfiles:
					curins[ii] = gfiles[name]
				else:
					# since the file doesn't exist yet, create as placeholder
					curins[ii] = File(name)
					gfiles[name] = curins[ii]

			# find outputs (checking for double-producing is done in Job, below)
			for ii, name in enumerate(curouts):
				name = str(Path(name))
				if name in gfiles:
					curouts[ii] = gfiles[name]
				else:
					curouts[ii] = File(name)
					gfiles[name] = curouts[ii]

			# append Job to list of jobs
			newjob = Job(curins, curouts, cmds, self)
			if VERBOSE > 2: print("New Job:%s"% str(newjob))

			# append job to list of jobs
			jobs.append(newjob)

		# find external files referenced as inputs
		if VERBOSE > 5:
			print("New Jobs")
			for j in jobs:
				print(j)
		return jobs

	def __str__(self):
		tmp = "Generator"
		for cc in self.cmds:
			tmp = tmp + ("\tCommand: %s\n" % cc)
		tmp = tmp + ("\tInputs: %s\n" % self.inputs)
		tmp = tmp + ("\tOutputs: %s\n" % self.outputs)
		return tmp

###############################################################################
# Job Class
###############################################################################
class Job:
	""" A job with a concrete set of inputs and outputs """

	# inputs are nested so that outer values refer to
	# values specified in the command [0] [1] : [0] [1]
	# and inner refer to any expanded values due to the
	# referred values above actually including lists ie:
	# subj = 1 2 3
	# /ello : /hello/${subj} /world
	# inputs = [[/hello/1,/hello/2,/hello/3],[/world]]
	inputs = list() #list of input files   (File)
	outputs = list() #list of output files (File)
	cmds = []
	parent = None
	pid = None
	status = None
	cmdqueue = [] # list of jobs that still need to be run

	# WAITING/RUNNING/SUCCESS/FAIL/DEPFAIL
	status = 'WAITING'

	def __init__(self, inputs, outputs, cmds, parent = None):
		self.inputs = inputs
		self.outputs = outputs
		self.parent = parent

		if "".join(cmds) != "":
			self.cmds = cmds
		else:
			self.cmds = []

		# make ourself the generator for the outputs
		for ff in self.outputs:
			if ff.genr:
				raise InputError(ff.path, "Error! Generator already given")
			else:
				ff.genr = self

		# add ourself to the list of users of the inputs
		for ff in self.inputs:
			ff.users.append(self)


	def __str__(self):
		out = "\n-----------------------------------------------\n"
#
#		for cc in self.cmds:
#			out = out + ("\t\tCommand: %s\n" % cc)
#		out = out + ("\t\tInputs: %s\n" % self.parent.inputs)
#		out = out + ("\t\tOutputs: %s\n" % self.parent.outputs)
#
		out = out + "\tInputs: "
		count = 0
		for ii in self.inputs:
			out = out + "\n\t\t" + str(ii)
		out = out + "\n\tOutputs:"
		for ii in self.outputs:
			out = out + "\n\t\t" + str(ii)
		out = out + '\n'
		out = out + "\n\tCommands:"
		for ii in self.cmds:
			out = out + "\n\t\t" + str(ii)
		out = out + '\n'
		return out

if __name__ == "__main__":
	main()
