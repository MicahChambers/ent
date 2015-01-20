import asyncore
import asynchat
import logging
import socket
import os, time
import re
import copy
import sys
import asyncore
import asynchat
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

md5re = re.compile('([0-9]{32})  (.*)|md5sum: (.*)')
gvarre = re.compile('(.*?)\${\s*(.*?)\s*}(.*)')
VERBOSE=10

def md5sum(fname):
    out = subprocess.check_output(['md5sum', fname])
    m = md5re.match(out.decode('utf-8'))
    if m and not m.group(3):
        return m.group(1)
    else:
        return None

def submit(cmd, name):
    # TODO check upstream to see if job already exists with same name
    print("Sumitting")
    print(cmd)
    proc = subprocess.Popen(cmd)
    return proc.pid

class InputError(Exception):
    """Exception raised for errors in the input.
       Attributes:
       expr -- input expression in which the error occurred
       msg  -- explanation of the error
    """

    def __init__(self, expr, msg):
        self.expr = expr
        self.msg = msg

class EntCommunicator(asynchat.async_chat):
    """
    Requests information from EntMoot on currently running jobs. May eventually
    add information about filesystem as well.

    Usage:
    comm = EntCommunicator(host,port)
    jobinfo = comm.openSyncRW(['USER username'])
    jobinfo = comm.openSyncRW(['USER username username ...'])
    jobinfo = comm.openSyncRW(['PID pid'])
    jobinfo = comm.openSyncRW(['PID pid pid pid pid ... '])
    jobinfo = comm.openSyncRW(['PID pid pid pid pid ... ', 'USER username username'])
    """
    def __init__(self, host, port):
        self.received_data = []
        self.logger = logging.getLogger('EntCommunicator')
        asynchat.async_chat.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.host = host
        self.port = port

    def handle_connect(self):
        self.logger.debug('handle_connect()')
        self.set_terminator(b'\n\n')

    def collect_incoming_data(self, data):
        """Read an incoming message from the server"""
        self.logger.debug('collect_incoming_data() -> (%d)\n"""%s"""', len(data), data)
        self.received_data.append(data)

    def found_terminator(self):
        self.logger.debug('found_terminator()')
        received_message = ''.join([bstr.decode("utf-8") for bstr in self.received_data])
        self.response = json.loads(received_message)
        self.close()

    def openSyncRW(self, reqlist):
        self.logger.debug('connecting to %s', (self.host, self.port))
        self.connect((self.host, self.port))
        self.response = {}

        ## Connect
        self.logger.debug('connecting to %s', self.host+':'+str(self.port))
        self.connect((self.host, self.port))

        # Convert String To List
        if type(reqlist) != type([]):
            reqlist = [reqlist]

        # Push All the Requests
        for req in reqlist:
            req = req.strip()
            if len(req) == 0:
                continue
            self.logger.debug('Sending Request: %s' % req)
            self.push(bytearray(req+'\n', 'utf-8'))
        self.push(b'\n')

        tmpmap = {}
        asyncore.loop(map=tmpmap)
        return self.response

def parseV1(filename):
    """Reads a file and returns Generators, and variables as a tuple

    Variable names may be [a-zA-Z0-9_]*
    Variable values may be anything but white space, multiple values may
    be separated by white space

    """
    iomre = re.compile("\s*([^:]*?)\s*:(?!//)\s*(.*?)\s*")
    varre = re.compile("\s*([a-zA-Z0-9_.]*)\s*=\s*(.*?)\s*")
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


##
# @brief Parses a string with ${VARNAME} type syntax with the contents of
# defs. If a variable contains more variable those are resolved as well.
# Special definitions:
# ${<} replaced with all inputs
# ${<N} where N is >= 0, replaced the the N'th input
# ${>} replaced with all outputs
# ${>N} where N is >= 0, replaced the the N'th output
# ${*SEP*VARNAME}
#
# @param inputs List of Files
# @param outputs List of List of Files
# @param defs Global variables to check
#
# @return
def expand(string, inputs, outputs, defs):
    # $> - 0 - all outputs
    # $< - 1 - all inputs
    # ${>#} - 2 - output number outputs
    # ${<#} - 3 - output number outputs
    # ${*SEP*VARNAME} - 4,5 - expand VARNAME, separating them by SEP
    # ${VARNAME} - 6 - expand varname

    allout  = "(\$>|\${>})|"
    allin   = "(\$<|\${<})|"
    singout = "\${>\s*([0-9]*)}|"
    singin  = "\${<\s*([0-9]*)}|"
    sepvar  = "\${\*([^*]*)\*([^}]*)}|"
    regvar  = "\${([^}]*)}"
    r = re.compile(allout+allin+singout+singin+sepvar+regvar)

    ## Convert Inputs Outputs to Strings
    inputs = [f.path for f in inputs]
    outputs = [f.path for f in outputs]

    ostring = string
    m = r.search(ostring)
    ii = 0
    while m != None:
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
        elif m.group(5) and m.group(6):
            # sepvar
            k = str(m.group(6))
            if k not in defs:
                raise InputError("expand", "Error Resolving Variable "
                        "%s in\n%s" % (k, string))
            if type(defs[k]) == type([]):
                insert = m.group(5).join(defs[k])
            else:
                insert = m.group(5)+m.group(6)
        elif m.group(7):
            # var
            k = str(m.group(7))
            if k not in defs:
                raise InputError("expand", "Error Resolving Variable "
                        "%s in\n%s" % (k, string))
            if type(defs[k]) == type([]):
                insert = " ".join(defs[k])
            else:
                insert = defs[k]

        ostring = ostring[:m.start()] + insert + ostring[m.end():]
        m = r.search(ostring)
        if ii == 100:
            raise InputError("Circular Variable References Detected!")

    return ostring

###############################################################################
# Ent Class
###############################################################################
class Ent:
    """ Main Ent Class, all others are derived from this. This stores global
    variables, all jobs, all files etc.

    Organization:
    Generator: This is a generic rule of how a particular set of inputs generates
    a particular set of outputs.

    """
    error = 0
    files = dict()   # filename -> File
    variables = dict() # varname -> list of values
    jobs = list()

    def __init__(self, host, port, entfile = None):
        """ Ent Constructor """
        self.error = 0
        self.files = dict()
        self.variables = {'.PWD' : os.getcwd()}
        self.jobs = list()
        self.usetime = True
        self.comm = EntCommunicator(host,port)

        # load the file
        if entfile:
            self.load(entfile)

    def load(self, entfile):
        jobs, self.variables = parseV1(entfile)
        if not jobs or not self.variables:
            raise "Error Parsing input file %s" % entfile

        # expand all the Generators into Jobs
        for bb in jobs:
            # get jobs and files this generates
            jlist = bb.genJobs(self.files, self.variables)
            if jlist == None:
                return -1
            # add jobs to list of jobs
            self.jobs.extend(jlist)

    def loadstate(self, fname):
        """
        Loads a file of modification times
        """
        with open(fname, 'r') as f:
            lines = f.readlines()
        for line in lines:
            m = modre.match(line)
            if m and m.group(2) in self.files:
                self.files[m.group(2)].modtime = int(m.group(1))

    def submit(self):
        for f in self.files.values():
            p = Path(f.path)
            if not p.exists():
                f.finished = False
            elif p.is_dir():
                try:
                    p.mkdir(parents=True)
                except FileExistsError:
                    pass
                f.finished = True
            elif f.md5sum == md5sum(f.path):
                # no existing md5sum
                f.finished = True
            else:
                f.finished = False

#        unmet = []
#        for f in self.files.values():
#            if f.finished:
#                print("Using Existing: %s" % f.path)
#            elif f.genr:
#                print("Generating: %s" % f.path)
#            else:
#                print("The following file does not have a generator")
#                print(f)
#                return -1
#
#        ### Start Running Jobs ###
#        outqueue = []
#        rqueue = self.jobs
#        watching = []
#        while len(rqueue) > 0:
#            for i, job in enumerate(rqueue):
#                try:
#                    cmds = job.getcmd(self.variables)
#
#                    hasher = hashlib.md5()
#                    hasher.update(repr(cmds).encode('utf-8'))
#                    for f in job.inputs:
#                        if f.md5sum:
#                            hasher.update(repr(f.md5sum).encode('utf-8'))
#
#                    watching.append(submit(cmds, cmds[0][0:5]+hasher.hexdigest()))
#                except InputError:
#                    pass
#
#            # Check On Watching Processes
#            jobinfo = self.comm.openSyncRW('PID '+' '.join([w for w in str(watching)]))
#            print("Job Info:\n%s\n" % str(jobinfo))
#
#            # To Do Find a Way to determine whethe jobs FAIL
#            for w in watching:
#                if w not in jobinfo
#            if len(done) > 0:
#                done.reverse()
#                for i in done:
#                    del(rqueue[i])
#            else:
#                print("Error The Following Job has Unresolved Dependencies!")
#                for rr in rqueue:
#                    print(rr)
#                raise InputError("Unresolved Dependencies")
#

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
                        cmd = expand(cmd, job.inputs, job.outputs, self.variables)
                    except InputError as e:
                        print("While Expanding Command %s" % cmd)
                        print(e.msg)
                        sys.exit(-1)
                    f.write('\n\t%s' % cmd)
                f.write('\n')

    def simulate(self):
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

        outqueue = []
        rqueue = copy.deepcopy(self.jobs)
        while len(rqueue) > 0:
            thispass = []
            done = []
            for i, job in enumerate(rqueue):
                try:
                    cmds = job.simulate(self.variables)
                    thispass.append(" &&".join(cmds))
                    done.append(i)
                except InputError:
                    pass
            outqueue.extend(thispass)

            # In the Real Runner we will also have to check weather there
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

    path = ""  # final path to use for input/output
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

        self.path = path
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
            raise InputError(self.path, "No Generator Creates File")

    def __str__(self):
        if self.finished:
            return self.path + " (done) "
        else:
            return self.path + " (incomplete) "

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
        and updates files with any newly refernenced files. May need global
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

            # for each input, fill in variable values from outputs
            for inval in self.inputs:
                invals = [inval]

                # the expanded version (for instance if there are multi-value)
                # arguments
                final_invals = []
                while len(invals) > 0:
                    tmp = invals.pop()
                    match = gvarre.fullmatch(tmp)
                    if match:
                        pref = match.group(1)
                        name = match.group(2)
                        suff = match.group(3)

                        # resolve variable name
                        if name in curvars:
                            # variable in the list of output vars, just sub in
                            invals.append(pref + curvars[name] + suff)
                        elif name in gvars:
                            # if it is a global variable, then we don't have a
                            # value from output, if there are multiple values
                            # then it is a compound (multivalue) input
                            realvals = gvars[name]
                            if len(realvals) == 1:
                                tmp = pref + realvals[0] + suff
                                invals.append(tmp)
                            else:
                                # multiple values, insert in reverse order,
                                # to make first value in list first to be
                                # resolved in next iteration
                                tmp = [pref + vv + suff for vv in
                                        reversed(realvals)]
                                invals.extend(tmp)
                        else:
                            print('Error, input "%s" references variable "%s"'\
                                    'which is a unknown!' % (inval, name))
                            return None

                    else:
                        final_invals.append(tmp)

                # insert finalized invals into curins
                curins.extend(final_invals)

            # find inputs and outputs in the global files database, and then
            # pass them in as a list to the new job

            # change curins to list of Files, instead of strings
            print(curins)
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
            oring = Job(curins, curouts, self.cmds, self)
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
        for igrp in self.inputs:
            ff.users.append(self)

    def getcmd(self, globvars):
        fullcmd = []
        first = True
        print(self)
        if not self.cmds:
            print("Error no command for %s\n"%self)
            raise InputError("getcmd", "Missing command!")

        for cmd in self.cmds:
            print(cmd)
            try:
                print(cmd)
                cmd = " ".join(re.split("\s+", cmd))
                print(cmd)
                cmd = expand(cmd, self.inputs, self.outputs, globvars)
                if not first:
                    fullcmd.append('&&')
                print(cmd)
                print(fullcmd)
                fullcmd.extend(re.split("\s+", cmd))
                print(fullcmd)
            except InputError as e:
                print("While Expanding Command %s" % cmd)
                print(e.msg)
                sys.exit(-1)

        return fullcmd

    def simulate(self, globvars):
        # Check if all the inputs are ready
        ready = True
        for infile in self.inputs:
           if not infile.finished:
               ready = False
               break

        if ready:
            cmds = []
            for cmd in self.cmds:
                try:
                    cmd = " ".join(re.split("\s+", cmd))
                    cmd = expand(cmd, self.inputs, self.outputs, globvars)
                except InputError as e:
                    print("While Expanding Command %s" % cmd)
                    print(e.msg)
                    sys.exit(-1)
                cmds.append(cmd)

            for out in self.outputs:
                out.finished = True
            return cmds
        else:
            raise InputError("Job: "+str(self)+" not yet ready")



    def __str__(self):
        out = "Job\n"
        out = out + "\tParent:\n"

        if self.parent:
            for cc in self.parent.cmds:
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
