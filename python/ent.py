import os, time
import re
import uuid
VERBOSE=4

class Ent:
    error = 0
    files = dict()   # filename -> File
    variables = dict() # varname -> list of values
    rings = list()
    branches = list()

    def __init__(self):
        self.error = 0
        self.files = dict()
        self.variables = dict()
        self.rings = list()
        self.branches = list()

    def run(self):

        for path,ff in files.items():
            print(path)
            if ff.makeready() != 0:
                return -1

    def parseV1(self, filename):
        """Reads a file and makes modification to Ent class to incorporate
        the read files. 

        Variable names may be [a-zA-Z0-9_]*
        Variable values may be anything but white space, multiple values may
        be separated by white space

        """
        iomre = re.compile("\s*([^:]*?)\s*:\s*(.*?)\s*")
        varre = re.compile("\s*([a-zA-Z0-9_]*)\s*=\s*(.*?)\s*")
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
                inputs = re.split("\s+", iomatch.group(1))
                outputs = re.split("\s+", iomatch.group(2))

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


        # now go ahead and expand all the branches into rings
        for bb in self.branches:

            # get rings and files this generates
            rlist, fdict = bb.genRings()

            # add rings to list of rings
            self.rings.extend(rlist)
            
            # add all the output files to the internal dict of files
            for fname,fobj in fdict.items():
                if fname in self.files:
                    print("Error, two commands generating file %s, "\
                            "keeping first" % fname)
                    return -1
                else:
                    self.files[fname] = fobj

        return 0

class File:
    abspath = ""  # absolute path
    genr = None   # pointer to the ring which generates the file

    # state variables
    mtime = None    # modification time

    def updateTime(self):
        try:
            #(mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime)
            ftup = os.stat(self.abspath)
            self.mtime = ftup[8]
            print("last modified: %s" % self.mtime)
            return 0
        except FileNotFoundError:
            return -1 

    # check if the file is up-to-date
    def ready(self):

        # if the parent has changed, then not ready
        if genr != None and genr.updated:
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
        if ready(self):
            return 0
        else:
            return genr.run()


class Branch:
    uuid = uuid.uuid4()
    cmds = list()
    inputs = list()
    outputs = list()

    def __init__(self, inputs, outputs, parent):
        self.cmds = list()
        self.inputs = inputs
        self.outputs = outputs
        self.parent = parent

    def genRings(self):
        # produce all the rings from inputs/outputs
        ofiles = dict()
        rings = list()
        varre = re.compile('(.*?)\${\s*(.*?)\s*}(.*)')
        outputs = [self.outputs]
        
        # what if a variable is hidden inside of another. need to create a list of 
        # lookups for each
        while True:
            print("outputs: %s" %outputs)
            
            # outer list realization of set of outputs (list)
            prevout = outputs
            match = None
            oii = None
            for ii in range(len(outputs)):
                for out in outputs[ii]:
                    
                    # find a variable
                    match = varre.fullmatch(out)
                    if match:
                        oii = ii
                        break;

            # no matches in any of the outputs, break
            if not match:
                break

            print("Match: %s" %  match.group(2))
            if match.group(2) not in self.parent.variables:
                print("Error! Unknown global variable reference: %s" % vv)
                return -1

            if VERBOSE > 1: print("Replacing %s" % match.group(2))
            
            subre = re.compile('\${\s*'+match.group(2)+'\s*}')
            values = self.parent.variables[match.group(2)]
            
            # save and remove matching realization
            outs = outputs[oii]
            del outputs[oii]

            for vv in values:
                newouts = []
                for out in outs:
                    newouts.append(subre.sub(vv, out))
                print("%s, %s" % (vv, newouts))
                outputs.append(newouts)

            print(outputs)

        return rings, ofiles

    def __str__(self):
        tmp = str(self.uuid) + "\n"
        for cc in self.cmds:
            tmp = tmp + ("\tCommand: %s\n" % cc)
        tmp = tmp + ("\tInputs: %s\n" % self.inputs)
        tmp = tmp + ("\tOutputs: %s\n" % self.outputs)
        return tmp

class Ring:
    inputs = list() #list of input files
    outputs = list() #list of output files
    cmd = "" 
    updated = True  # when updated leave set until next run
    parent = None

    def run(self):

        # make sure all the inputs are ready
        for ii in inputs:
            if parent == None:
                print("Error, parent has not been set!")
                return -1
            inputf = parent.get(ii)

            if inputf == None:
                print("Error, unknown input: %s" % ii)
                return -1
            elif inputf.makeready() != 0:
                return -1

            return 0

        # run process
        cmd = ""

        # update mtimes, and command used to generate
        for ff in outputs:
            if ff.updateTime() != 0:
                return -1

        updated = False
        return 0

