#!/usr/bin/python3

import drmaa
import argparse
import os, time
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

## TODO add pipe to output expansion
## TODO handle quotes in variable definitions
md5re = re.compile('([0-9]{32})  (.*)|md5sum: (.*)')
gvarre = re.compile('(.*?)\${\s*([a-zA-Z0-9_.]+)\s*}(.*)')

VERBOSE=10

def main():
    parser = argparse.ArgumentParser(description='Run ENT on an ent script')
    parser.add_argument('--script', '-f', type=str,
            help='Input script file', required=True)
    parser.add_argument('--state', '-s', type=str, nargs=1,
            default="", help='State file (to store md5 sums in')
    parser.add_argument('--simulate', '-x', action='store_const', const=True,
            default="", help='Simulate running rather than actually running')

    args = parser.parse_args()

    entobj = Ent(args.script)
    if args.simulate:
        entobj.simulate()
    elif not args.state:
        print("Warning no state file specified")
        entobj.submit()
    else:
        entobj.submit(args.state)

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
            out[ff] = m.hexdigest()
        except FileNotFoundError:
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

    variables = dict()
    jobs = []
    with open(filename, "r") as f:
        infile = json.load(f)
        variables = infile['variables']

        # Check Variables
        for kk,vv in variables.items():
            if type(vv) != type([]):
                variables[kk] = [vv]

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

    return (jobs, variables)

def parseV1(filename):
    """Reads a file and returns Generators, and variables as a tuple

    Variable names may be [a-zA-Z0-9_]*
    Variable values may be anything but white space, multiple values may
    be separated by white space

    """
    iomre = re.compile("\s*([^:]*?)\s*:(?!//)\s*(.*?)\s*")
    varre = re.compile("\s*([a-zA-Z0-9_.]+)\s*=\s*(.*?)\s*")
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
            values = re.split("\s+", varmatch.group(2))
            if name in variables:
                print("Error! Redefined variable: %s" % name)
                return (None, None)
            if VERBOSE > 3: print("Defining: %s = %s"%(name, str(values)))
            variables[name] = values
        else:
            continue

    if VERBOSE > 3: print("Done With Initial Pass!")

    return (jobs, variables)


## Expand String Based on Local (First) then Global Variables
# only expands variables that match ${[a-zA-Z0-9_]}
def expand_variables(instr, localvars, gvars):
    inlist = [instr]

    # the expanded version (for instance if there are multi-value)
    # arguments
    final_vals = []
    while len(inlist) > 0:
        tmp = inlist.pop()
        match = gvarre.fullmatch(tmp)
        if match:
            pref = match.group(1)
            name = match.group(2)
            suff = match.group(3)

            # resolve variable name
            if name in localvars:
                # variable in the list of output vars, just sub in
                inlist.append(pref + localvars[name] + suff)
            elif name in gvars:
                # if it is a global variable, then we don't have a
                # value from output, if there are multiple values
                # then it is a compound (multivalue) input
                realvals = gvars[name]
                if len(realvals) == 1:
                    tmp = pref + realvals[0] + suff
                    inlist.append(tmp)
                else:
                    # multiple values, insert in reverse order,
                    # to make first value in list first to be
                    # resolved in next iteration
                    tmp = [pref + vv + suff for vv in
                            reversed(realvals)]
                    inlist.extend(tmp)
            else:
                raise InputError('expand_string', 'Error, input ' + \
                        '"%s" references variable "%s"' % (instr, name) + \
                        'which is a unknown!')
        else:
            final_vals.append(tmp)

    return final_vals

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
                for ii in range(len(job.cmds)):
                    job.cmds[ii] = expand_args(job.cmds[ii], job.inputs, \
                            job.outputs)

            # add jobs to list of jobs
            self.jobs.extend(jlist)

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
                print("All Outputs Exist for Job: %s" % str(job.cmds))
            else:
                job.status = 'WAITING'

    def run(self):
        # Set up Grid Engine
        self.gridengine = drmaa.Session()
        self.gridengine.initialize()
        template = self.gridengine.createJobTemplate()

        # Queues. Jobs move from waitqueue to startqueue, startqueue to either
        # runqueue or donequeue, and runqueue to startqueue or (in case of error)
        # donequeue
        waitqueue = [j for j in self.jobs] # jobs with unmet depenendencies
        runqueue = []   # jobs that are currently running
        startqueue = [] # Jobs the need to be started
        donequeue = []  # jobs that are done
        while waitqueue or runqueue:
            prevwlen = len(waitqueue)
            prevrlen = len(runqueue)

            ## For when jobs have unfulfilled deps
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
                    if VERBOSE > 3:
                        print("Job Ready to Run:\n%s"%str(job))
                    job.status = 'RUNNING'
                    job.cmdqueue = [cmd for cmd in job.cmds]
                    startqueue.append(job)
                    del(waitqueue[waitqueue.index(job)])

            for job in startqueue:
                ## Get the Next Job (If there is one)
                cmd = None
                while not cmd and job.cmdqueue:
                    cmd = job.cmdqueue.pop(0)

                if not cmd:
                    # No More Jobs Left, Check Outputs
                    job.status = 'SUCCESS'
                    tmpsums = md5sum([f.path for f in job.outputs])
                    for output in job.outputs:
                        if output.path not in tmpsums:
                            job.status = 'FAIL'
                            donequeue.append(job)
                            del(startqueue[startqueue.index(job)])
                            if VERBOSE > 1:
                                print("Output '%s' not generated by '%s'!"% (output.path, str(job)))
                            break
                        else:
                            output.md5sum = tmpsums[output.path]
                else:
                    # Start the next job
                    if VERBOSE > 1:
                        print("Submitting Job:\n%s " % str(cmd))
                    cmd = split_command(cmd)
                    template.remoteCommand = cmd[0]
                    template.args = cmd[1:]
                    job.pid = self.gridengine.runJob(template)
                    if VERBOSE > 2:
                        print("PID: %s" % job.pid)
                    job.status = 'RUNNING'
                    runqueue.append(job)
                    del(startqueue[startqueue.index(job)])

            ## For When a Job has been submitted (check on it)
            for job in runqueue:
                # Check Process with Grid Engine
                stat = self.gridengine.jobStatus(job.pid)
                if VERBOSE > 1:
                    print("Checking in on %s" % str(job.pid))
                    print("Status: %s " % decodestatus[stat])

                if stat == drmaa.JobState.FAILED:
                    job.status = 'FAIL'
                    donequeue.append(job)
                    del(runqueue[runqueue.index(job)])
                elif stat == drmaa.JobState.DONE:
                    # Move Back to the Start Queue
                    startqueue.append(job)
                    del(runqueue[runqueue.index(job)])


            if prevwlen == len(waitqueue) and prevrlen == len(runqueue):
                time.sleep(5)
                try:
                    with open(self.md5state, "w") as f:
                        sums = {f.path: f.md5sum for f in self.files if f.md5sum}
                        print(sums)
                        json.dump(sums, f, indent=1)
                except:
                    pass

        self.gridengine.deleteJobTemplate(template)
        self.gridengine.exit()

    def genmakefile(self, filename):

        # Identify Files without Generators
        rootfiles = []
        for k,v in self.files.items():
            if v.genr == None:
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
        print("May need to incorporate statefile...")

        # Identify Files without Generators
        rootfiles = []
        for k,v in self.files.items():
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
        self.inputs = inputs
        self.outputs = outputs


    def genJobs(self, gfiles, gvars):
        """ The "main" function of Generator is genJobs. It produces a list of
        Job (with concrete inputs and outputs)
        and updates files with any newly referenced files. May need global
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

        # First Expand All Variables in outputs, for instance if there is a
        # variable in the output and the variable referrs to an array, that
        # produces mulitiple outputs
        while True:
            # outer list realization of set of outputs (list)
            prevout = outputs
            match = None
            oii = None
            ii = 0
            for outs in outputs:
                for out in outs:

                    # find a variable
                    match = gvarre.fullmatch(out)
                    if match:
                        oii = ii
                        ii = len(outputs)
                        break;

                ii = ii+1
                if ii >= len(outputs):
                    break;

            # no matches in any of the outputs, break
            if not match:
                break

            # TODO add pipe syntax here
            pref = match.group(1)
            vname = match.group(2)
            suff = match.group(3)
            if vname not in gvars:
                raise InputError("genJobs", "Error! Unknown global variable "
                        "reference: %s" % vname)

            subre = re.compile('\${\s*'+vname+'\s*}')

            # we already have a value for this, just use that
            if vname in valreal[oii]:
                vv = valreal[oii][vname]

                # perform replacement in all the gvars
                for ojj in range(len(outputs[oii])):
                    outputs[oii][ojj] = subre.sub(vv, outputs[oii][ojj])

                # restart expansion process in case of references in the
                # expanded value
                continue

            # no previous match, go ahead and expand
            values = gvars[vname]

            # save and remove matching realization
            outs = outputs[oii]
            del outputs[oii]
            varprev = valreal[oii]
            del valreal[oii]

            for vv in values:
                newouts = []
                newvar = copy.deepcopy(varprev)
                newvar[vname] = vv

                # perform replacement in all the gvars
                for out in outs:
                    newouts.append(subre.sub(vv, out))

                outputs.append(newouts)
                valreal.append(newvar)

        # now that we have expanded the outputs, just need to expand input
        # and create a job to store each realization of the expansion process
        for curouts, curvars in zip(outputs, valreal):
            curins = []
            cmds = []

            # for each input, fill in variable values from outputs
            for inval in self.inputs:
                # insert finalized invals into curins
                curins.extend(expand_variables(inval, curvars, gvars))

            for cmd in self.cmds:
                # insert finalized invals into curins
                cmds.extend(expand_variables(cmd, curvars, gvars))

            # change curins to list of Files, instead of strings by
            # finding inputs and outputs in the global files database, and then
            # pass them in as a list to the new job
            for ii, name in enumerate(curins):
                if name in gfiles:
                    curins[ii] = gfiles[name]
                else:
                    # since the file doesn't exist yet, create as placeholder
                    curins[ii] = File(name)
                    gfiles[name] = curins[ii]

            # find outputs (checking for double-producing is done in Job, below)
            for ii, name in enumerate(curouts):
                if name in gfiles:
                    curouts[ii] = gfiles[name]
                else:
                    curouts[ii] = File(name)
                    gfiles[name] = curouts[ii]

            # append Job to list of jobs
            oring = Job(curins, curouts, cmds, self)
            if VERBOSE > 2: print("New Job:%s"% str(oring))

            # append job to list of jobs
            jobs.append(oring)

        # find external files referenced as inputs
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
        out = "Job\n"
        out = out + "\tParent:\n"

        for cc in self.cmds:
            out = out + ("\t\tCommand: %s\n" % cc)
        out = out + ("\t\tInputs: %s\n" % self.parent.inputs)
        out = out + ("\t\tOutputs: %s\n" % self.parent.outputs)

        out = out + "\tInputs: "
        count = 0
        for ii in self.inputs:
            out = out + "\n\t\t\t" + str(ii)
        out = out + "\n\tOutputs:"
        for ii in self.outputs:
            out = out + "\n\t\t\t" + str(ii)
        out = out + '\n'
        return out

if __name__ == "__main__":
    main()
