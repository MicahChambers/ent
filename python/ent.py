import os, time
import re
import uuid
import hashlib
import copy
        
urire = re.compile("^\s*ssh://(?:([a-z_][a-z0-9_]{0,30})@)?([a-zA-Z.-]*)(?:([0-9]+))?(/.*)?")
varre = re.compile('(.*?)\${\s*(.*?)\s*}(.*)')
VERBOSE=4

def parseURI(uri):
    rmatch = urire.search(uri)
    ruser = rmatch.group(1)
    rhost = rmatch.group(2)
    if rmatch.group(3):
        rport = int(rmatch.group(3))
    else:
        rport = 22
    rpath = rmatch.group(4)
    return (ruser, rhost, rport, rpath)

class InputError(Exception):
    """Exception raised for errors in the input.
       Attributes:
       expr -- input expression in which the error occurred
       msg  -- explanation of the error
    """
                                    
    def __init__(self, expr, msg):
        self.expr = expr
        self.msg = msg

class Ent:
    """ Main Ent Class, all others are derived from this. This stores global 
    variables, all jobs, all files etc.
    """
    error = 0
    files = dict()   # filename -> File
    variables = dict() # varname -> list of values
    remotes = dict() # user@host:port -> Remote
    rings = list()
    branches = list()

    def __init__(self):
        self.error = 0
        self.files = dict()
        self.variables = dict()
        self.rings = list()
        self.branches = list()

    def batch(self):
        """ Batch up all the jobs and generate a list of jobs that can be
        run simultaneously as a group"""

        fexist = set() # files that exist
        fronts = []

        queue = [rr for rr in self.rings]
        change = True 

        while len(queue) > 0 and change:
            # add any jobs that can be run to the current front
            print("Files created: %s " % fexist)
            front = []
            newexist = set()
            for rr in range(len(queue)):
                ready = True
                # check to see if all the inputs exist
                for ii in queue[rr].inputs:
                    if ii not in fexist:
                        print("%s doesn't exist yet" % ii)
                        ready = False
                        break

                # if its ready add to current front, add output
                # to list of done 
                if ready:
                    newexist.update(queue[rr].outputs)
                    front.append(queue[rr])
                    queue[rr] = None
                    change = True

            # done with current pass of queue, remove elements and 
            # add new exist files to fexist
            fexist.update(newexist)
            fronts.append(front)
            queue = [qq for qq in queue if qq != None];

        if len(queue) > 0:
            print("Error, jobs exist without a way to generate their inputs")
            return -1

        for ff in fronts:
            for rr in ff:
                print(rr.cmd)

    def run(self):
        """ Start the jobs """
        for rr in self.rings:
            rr.run()

    def setuptodate(self, filename):
        """ Reads a history file. Determines which files are 
        up to date, and which need to be updated.
        
        """ 
        
        # set all as out-of-date
        for fn, fclass in self.files.items():
            fclass.uptodate = False

        fhist = [()]
        
        # for each history file, check whether cache is out of date
        for fn in fhist:

            if fn.path in self.files:
                fclass = self.files[fn.path]
                cachef = join(self.cachedir, fclass.abspath)
                outf = fclass.abspath

                if fn.md5 == md5(cachef):
                    # cache file up to date
                    fclass.uptodate = True
                elif fn.md5 == md5(outf):
                    # cache not up to date, but out is
                    copy(source = outf, target = cachef)
                    fclass.uptodate = True
        
        # find all the generator-less inputs and make them uptodate by copying
        for fn, fclass in self.files.items():

            if fclass.genr == None and not fclass.uptodate:
                cachef = join(self.cachedir, fclass.abspath)
                outf = fclass.abspath
                copy(source = outf, target = cachef)

                fclass.uptodate = True

        # propagate uptodate status
        queue = [fclass for fclass in self.files.values() if not fclass.uptodate]
        while len(queue) > 0:
            fclass = queue.pop()
            
            # if not up to date, make all the children no up to date, and 
            # add to queue
            if not fclass.uptodate:
                # user is a ring that uses this as an input
                for user in fclass.users:

                    # child is an output from the given user ring
                    for child in user.outputs:
                        if fclass not in child.inputs:
                            return -1

                        # only enqueue if it hadn't been fixed yet
                        # prevents loops
                        if child.uptodate:
                            child.uptodate = False
                            queue.append(child)
        
        # alright everything should be up to date
        return 0

    def parseV1(self, filename):
        """Reads a file and makes modification to Ent class to incorporate
        the read files. This populates 

        self.files      - all inputs or outputs, dict of name->File
        self.rings      - jobs with specific inputs and outputs
        self.branches   - jobs prior to resolution
        self.variables  - global variables

        Variable names may be [a-zA-Z0-9_]*
        Variable values may be anything but white space, multiple values may
        be separated by white space

        """
        iomre = re.compile("\s*([^:](?!//)*?)\s*:\s*(.*?)\s*")
        varre = re.compile("\s*([a-zA-Z0-9_.]*)\s*=\s*(.*?)\s*")
        cmdre = re.compile("\t\s*(.*?)\s*")
        commentre = re.compile("(.*?)#.*")

        with open(filename, "r") as f:
            lines = f.readlines()
        
        # so that any remaining branches get cleared
        lines.append("")
        lineno=0
        fullline = ""
        cbranch = None
        
        for line in lines:
            # remove endlines
            line = line.rstrip("\n")

            # remove comments
            match = commentre.search(line)
            if match:
                line = match.group(1)

            # check for continuation
            lineno = lineno + 1
            fullline = fullline + line
            if len(line) > 0 and line[-1] == '\\':
                fullline = fullline[0:-1]
                continue
            
            # no continuation, so elminated fullline
            line = fullline
            fullline = ""
            if VERBOSE > 2: print("line %i:\n%s" % (lineno, line))

            # if there is a current branch we are working
            # the first try to append commands to it
            if cbranch:
                cmdmatch = cmdre.fullmatch(line)
                if cmdmatch:
                    # append the extra command to the branch
                    cbranch.cmds.append(cmdmatch.group(1))
                    continue
                else:
                    # done with current branch, remove current link
                    self.branches.append(cbranch)
                    if VERBOSE > 0: print("Adding branch: %s" % cbranch)
                    cbranch = None
            
            # if this isn't a command, try the other possibilities
            iomatch = iomre.fullmatch(line)
            varmatch = varre.fullmatch(line)
            if iomatch:
                # expand inputs/outputs
                inputs = re.split("\s+", iomatch.group(2))
                outputs = re.split("\s+", iomatch.group(1))

                # create a new branch
                cbranch = Branch(inputs, outputs, self)
                if VERBOSE > 1: print("New Branch: %s:%s" % (inputs, outputs))
            elif varmatch:
                # split variables
                name = varmatch.group(1)
                values = re.split("\s+", varmatch.group(2))
                if name in self.variables:
                    print("Error! Redefined variable: %s" % name)
                    return -1
                if VERBOSE > 1: print("Defining: %s = %s"%(name, str(values)))
                self.variables[name] = values
            else:
                continue

        remote = self.variables.getdefault(".REMOTE")

        # now go ahead and expand all the branches into rings
        for bb in self.branches:

            # get rings and files this generates
            rlist = bb.genRings(self.files, self.variables, 
                    self.remotes, remote)

            # add rings to list of rings
            self.rings.extend(rlist)
            
        return 0

class File:
    """ Keeps track of a particular files metdata. Obviously this includes a
    path, but also keeps track of whether the file has been updated since the
    last run. """

    cachehost = None  # server that cache is on (must be same as processor)
    cachepath = ""  # path in the cache to use, usually a variant of finalpath 
    finalhost = None  # server that final is on
    finalpath = ""  # final path to use for input/output
    cmtime = None # previous cache mtime, modification time of cache file
    fmtime = None # previous final mtime, modification time of final file

    genr = None   # pointer to the ring which generates the file
    users = []

    # state variables
    mtime = None    # modification time

    def __init__(self, path, cachedir = "", remote ="", remotes = None):
        """ Constructor for File class. 

        Parameters
        ----------
        path : string
            the input/output file path that may be on the local machine or on 
            any remote server
        cachedir : string, optional
            base directory where cache file should be. If this is provided then
            the cachepath will be this+path. If not provided then cachepath
            will match finalpath.
        remote : string, optional 
            where processing will be performed. If not set then it will be 
            the local machine. If it is set then cachehost will be set to this
        remote : dict, optional 
            dict of uri -> connection classes. If a new connection has to be
            opened for the file, the connection will be added here
        """
        
        # parse the input string
        fuser, fhost, fport, fpath = parseURI(path)

        # go ahead and set finalpath
        self.finalpath = fpath

        # if there is a host then see if we are already created, otherwise
        # create the Remote
        if fhost:
            tmp = fuser + "@" + fhost + ":" + fport
            if tmp in remotes:
                self.finalhost = remotes[tmp]
            else:
                self.finalhost = Remote(fuser, fhost, fport)
                remotes[tmp] = self.finalhost
        else:
            # just assume its local
            self.finalhost = None
        
        # parse remote
        if remote:
            ruser, rhost, rport, rpath = parseURI(remote)

        # parse cachedir
        if cachedir:
            cuser, chost, cport, cpath = parseURI(cachedir)
            self.cachepath = os.path.join(cpath, self.finalpath)

            # check that chost, 
            #   if set, matches remote, 
            #   if not set , just set the chost to remote
            if chost or rhost:
                if not chost and remote:
                    cuser = ruser
                    chost = rhost
                    cport = rport
                elif chost and not remote:
                    raise InputError(cachedir+","+remote, "Error! Remote " \
                            "cache specified, but non-remote processing is " \
                            "specified")
                elif rhost != chost or rport != cport or ruser != cuser:
                    raise InputError(cachedir+","+remote, "Error! Remote " \
                            "cache specified but it doesn't match the remote "\
                            "processing node")

                tmp = cuser + "@" + chost + ":" + cport
                if tmp in remotes:
                    self.cachehost = remotes[tmp]
                else:
                    self.cachehost = Remote(cuser, chost, cport)
                    remotes[tmp] = self.cachehost
            else:
                # just local
                self.cachehost = None
        else:
            # no cachedir, just set to finalpath
            self.cachepath = self.finalpath

        self.users = []

    def updateTime(self):
        try:
            #(mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime)
            ftup = os.stat(self.abspath)
            self.mtime = ftup[8]
            print("last modified: %s" % self.mtime)
            return 0
        except FileNotFoundError:
            print("File doesn't exist!\n%s" % self.abspath)
            return -1 
        except PermissionError:
            print("Don't have permissions to access:\n%s" % self.abspath)

    # check if the file is up-to-date
    def ready(self):
        if self.genr != None and self.genr.updated:
            return False

        try:
            ftup = os.stat(self.abspath)
            mtime = ftup[8]
        except FileNotFoundError:
            # doesn't exist, not ready
            return False

        # file is out of date 
        if self.mtime != mtime:
            return False

        return True

    # does whatever is necessary to produce this file
    def makeready(self):
        if self.ready():
            return 0
        else:
            return self.genr.run()

    def __str__(self):
        out = "File (" + self.abspath + ")"
        return out

class Branch:
    """ A branch is a job that has not been split up into rings (which are the
    actual jobs with resolved filenames"""

    uuid = uuid.uuid4()
    cmds = list()
    inputs = list()
    outputs = list()

    def __init__(self, inputs, outputs):
        self.cmds = list()
        self.inputs = inputs
        self.outputs = outputs

    def genRings(self, gfiles, gvars, gremotes, remote):
        """ The "main" function of Branch is genRings. It produces a list of 
        rings (which are specific jobs with specific inputs and outputs)
        and updates files with any newly refernenced files. May need global
        gvars to resolve file names.
        
        Parameters
        ----------
        gfiles : dict, {filename: File}
            global dictionary of files, will be updated with any new files found
        gvars : {varname: [value...] }
            global variables used to look up values
        remotes: dict, { "user@host:port" : Remote }
            remote servers that we may access, lazily connect
        remote: ssh://user@host:port
            remote processing server (defualt for cachedir)
        
        """
        
        # produce all the rings from inputs/outputs
        rings = list()
        outputs = [self.outputs]
        valreal = [dict()]

        while True:
            print("outputs: %s" %outputs)
            
            # outer list realization of set of outputs (list)
            prevout = outputs
            match = None
            oii = None
            ii = 0
            for outs in outputs:
                for out in outs:
                    
                    # find a variable
                    match = varre.fullmatch(out)
                    if VERBOSE > 2: print(out)
                    if match:
                        if VERBOSE > 1: print("Match found!")
                        oii = ii
                        ii = len(outputs)
                        break;

                ii = ii+1
                if ii >= len(outputs):
                    break;

            # no matches in any of the outputs, break
            if not match:
                if VERBOSE > 1: print("No Matches found!")
                break

            pref = match.group(1)
            vname = match.group(2)
            suff = match.group(3)
            if VERBOSE > 3: print("Match: %s" %  vname)
            if vname not in gvars:
                print("Error! Unknown global variable reference: %s" % vv)
                return -1

            if VERBOSE > 1: print("Replacing %s" % vname)
            subre = re.compile('\${\s*'+vname+'\s*}')
 
            # we already have a value for this, just use that
            if vname in valreal[oii]:
                vv = valreal[oii][vname]
                if VERBOSE > 2: print("Previous match: %s" , vname)
                if VERBOSE > 4: print(outputs[oii])

                # perform replacement in all the gvars 
                for ojj in range(len(outputs[oii])):
                    outputs[oii][ojj] = subre.sub(vv, outputs[oii][ojj])

                if VERBOSE > 4: print(outputs[oii])
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
                if VERBOSE > 3: print("%s, %s, %s" % (vv, newouts, newvar))


            if VERBOSE > 2: print(outputs)

        # now that we have expanded the outputs, just need to expand input
        # and create a ring to store each realization of the expansion process
        for curouts, curvars in zip(outputs, valreal):
            curins = []

            # fill in variable values from outputs
            for inval in self.inputs:
                if VERBOSE > 1: print("inval: %s" % inval)
                match = varre.fullmatch(inval)
                while match:
                    pref = match.group(1)
                    name = match.group(2)
                    suff = match.group(3)

                    # resolve variable name
                    if name in curvars:
                        inval = pref + curvars[name] + suff
                    elif name in gvars:
                        # if it is a global variable, the it must not have multiple
                        # values (list) 
                        realvals = gvars[name]
                        if len(realvals) != 1:
                            print('Error, input "%s" references variable "%s"'\
                                    'which is a list, but that same variable '\
                                    'doesn\'t exist in the output specification'\
                                    'without referencing the output, there would'\
                                    'be not way of differentiating different '\
                                    'inputs! Please at the variable to the output '\
                                    'somehow or rethink your Ent setup' % \
                                    (inval, name))
                            return -1
                        else:
                            inval = pref + gvars[name][0] + suff
                    else:
                        print('Error, input "%s" references variable "%s"'\
                                'which is a unknown!' % (inval, name))
                        return -1
                    
                    # find next match
                    match = varre.fullmatch(inval)
            
                if VERBOSE > 1: print("expanded inval: %s" % inval)
                curins.append(inval)
            
            # find inputs and outputs in the global files database, and then
            # pass them in as a list to the new ring
            
            # change curins to list of Files, instead of strings
            for ii in range(len(curins)):
                if curins[ii] in gfiles:
                    curins[ii] = gfiles[curins[ii]]
                else:
                    # since the file doesn't exist yet, create as placeholder
                    name = curins[ii]
                    curins[ii] = File(name, cachedir, remote, gremotes)
                    gfiles[name] = curins[ii]

            # find outputs (checking for double-producing is done in Ring, below)
            for ii in range(len(curouts)):
                if curouts[ii] in gfiles:
                    curouts[ii] = gfiles[curouts[ii]]
                else:
                    name = curouts[ii]
                    curouts[ii] = File(name, cachedir, remote, gremotes)
                    gfiles[name] = curouts[ii]

            # append ring to list of rings
            oring = Ring(curins, curouts)
            
            if VERBOSE > 0: print(str(oring))

            # append ring to list of rings
            rings.append(oring)

        # find external files referenced as inputs
        return rings 

    def __str__(self):
        tmp = str(self.uuid) + "\n"
        for cc in self.cmds:
            tmp = tmp + ("\tCommand: %s\n" % cc)
        tmp = tmp + ("\tInputs: %s\n" % self.inputs)
        tmp = tmp + ("\tOutputs: %s\n" % self.outputs)
        return tmp

class Ring:
    """ A job with a concrete set of inputs and outputs """
    inputs = list() #list of input files   (File)
    outputs = list() #list of output files (File)
    cmd = "" 
    updated = True  # when updated leave set until next run

    def __init__(self, inputs, outputs):
        self.inputs = inputs
        self.outputs = outputs

        # make ourself the generator for the outputs
        for ff in self.outputs:
            if ff.genr:
                raise InputError(ff.finalpath, "Error! Generator already given")
            else:
                ff.genr = self

        # add ourself to the list of users of the inputs
        for ff in self.inputs:
            ff.users.append(self)

    # TODO instead of run, just prepare batches
    def batch(self):
        print("Batch Ring: %s" % self)


    def run(self):
        print("Run Ring: %s" % self)

    def __str__(self):
        out = "Ring\n"
        out = out + "Inputs: " + str(self.inputs) + "\n"
        out = out + "Outputs: " + str(self.outputs) + "\n"
        out = out + "Updated: " + str(self.updated) + "\n"
        return out
