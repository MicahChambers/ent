import os, time
import re
import uuid
import copy
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

        for path,ff in self.files.items():
            print("Readying %s" % path)
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

    def __init__(self, path, gener):
        self.abspath = os.path.abspath(path)
        self.genr = gener

    def updateTime(self):
        try:
            #(mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime)
            print(self.abspath)
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

        # if the parent has changed, then not ready
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
        valreal = [dict()]

        # what if a variable is hidden inside of another. need to create a list of 
        # lookups for each
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

            if VERBOSE > 3: print("Match: %s" %  match.group(2))
            if match.group(2) not in self.parent.variables:
                print("Error! Unknown global variable reference: %s" % vv)
                return -1

            if VERBOSE > 1: print("Replacing %s" % match.group(2))
            subre = re.compile('\${\s*'+match.group(2)+'\s*}')
 
            # we already have a value for this, just use that
            if match.group(2) in valreal[oii]:
                vv = valreal[oii][match.group(2)]
                if VERBOSE > 2: print("Previous match: %s" , match.group(2))
                if VERBOSE > 4: print(outputs[oii])

                # perform replacement in all the variables
                for ojj in range(len(outputs[oii])):
                    outputs[oii][ojj] = subre.sub(vv, outputs[oii][ojj])

                if VERBOSE > 4: print(outputs[oii])
                continue

            # no previous match, go ahead and expand
            values = self.parent.variables[match.group(2)]
            
            # save and remove matching realization
            outs = outputs[oii]
            del outputs[oii]
            varprev = valreal[oii]
            del valreal[oii]

            for vv in values:
                newouts = []
                newvar = copy.deepcopy(varprev)
                newvar[match.group(2)] = vv

                # perform replacement in all the variables
                for out in outs:
                    newouts.append(subre.sub(vv, out))
                
                outputs.append(newouts)
                valreal.append(newvar)
                if VERBOSE > 3: print("%s, %s, %s" % (vv, newouts, newvar))


            if VERBOSE > 2: print(outputs)

        # now that we have expanded the outputs, just need to expand input
        # and create a ring to store each realization of the expansion process
        print(outputs)
        for outs,dicts in zip(outputs, valreal):
            print(outs, dicts)
            # create ring
            oring = Ring()
            oring.inputs = []

            print(self.inputs)
            # fill in variable values from outputs
            for inval in self.inputs:
                if VERBOSE > 1: print("inval: %s" % inval)
                match = varre.fullmatch(inval)
                while match:
                    if match.group(2) in dicts:
                        inval = match.group(1) + dicts[match.group(2)] + \
                                match.group(3)
                    elif match.group(2) in self.parent.variables:
                        realvals = self.parent.variables[match.group(2)]
                        if len(realvals) != 1:
                            print('Error, input "%s" references variable "%s"'\
                                    'which is a list, but that same variable '\
                                    'doesn\'t exist in the output specification'\
                                    'without referencing the output, there would'\
                                    'be not way of differentiating different '\
                                    'inputs! Please at the variable to the output '\
                                    'somehow or rethink your Ent setup' % \
                                    (inval, match.group(2)))
                            return -1
                        else:
                            inval = match.group(1) + \
                                    self.parent.variables[match.group(2)][0] + \
                                    match.group(3)
                    else:
                        print('Error, input "%s" references variable "%s"'\
                                'which is a unknown!' % (inval, match.group(2)))
                        return -1
                    
                    # find next match
                    match = varre.fullmatch(inval)
                
                if VERBOSE > 1: print("expanded inval: %s" % inval)
                oring.inputs.append(inval)

            oring.updated = True
            oring.parent = self

            if VERBOSE > 0: print(str(oring))

            # append ring to list of rings
            rings.append(oring)

            # link files to oring
            for out in outs: 
                tmp = File(out, oring)
                ofiles[out] = tmp
                oring.outputs.append(tmp)

        print(ofiles)
        if VERBOSE > 0: print("DONE")
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
        for ii in self.inputs:
            if self.parent == None:
                print("Error, parent has not been set!")
                return -1

            if ii not in self.parent.parent.files:
                print("External input: %s" % ii)
                continue

            inputf = self.parent.parent.files[ii]
            print("Make ready: %s" % inputf)
            if inputf.makeready() != 0:
                return -1

            return 0

        # run process
        cmd = ""

        # update mtimes, and command used to generate
        for ff in self.outputs:
            if ff.updateTime() != 0:
                return -1

        self.updated = False
        return 0

