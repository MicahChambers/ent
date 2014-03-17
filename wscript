#!/bin/env python3
from waflib.Utils import subprocess
import os
import sys
from waflib import Options, Node, Build, Configure
import re

out = 'build'

def configure(conf):
    join = os.path.join
    isabs = os.path.isabs
    abspath = os.path.abspath

    opts = vars(conf.options)
    conf.load('compiler_cxx')

    env = conf.env

    ############################### 
    # Basic Compiler Configuration
    ############################### 

    conf.env.RPATH = []
    if opts['enable_rpath'] or opts['enable_build_rpath']:
        for pp in ['fmri', 'graph', 'libs', 'point3d', 'R']:
            conf.env.RPATH.append(join('$ORIGIN', '..', pp))

    if opts['enable_rpath'] or opts['enable_install_rpath']:
        conf.env.RPATH.append(abspath(join(conf.env.PREFIX, 'lib')))

    conf.env.DEFINES = ['AE_CPU=AE_INTEL', 'VCL_CAN_STATIC_CONST_INIT_FLOAT=0', 'VCL_CAN_STATIC_CONST_INIT_INT=0']
    conf.env.CXXFLAGS = ['-Wall', '-std=c++11']
    if opts['profile']:
        conf.env.DEFINES.append('DEBUG=1')
        conf.env.CXXFLAGS.extend(['-Wno-unused-parameter', '-Wno-sign-compare', '-Wno-unused-local-typedefs', '-Wall', '-Wextra','-g', '-pg'])
        conf.env.LINKFLAGS = ['-pg']
    elif opts['debug']:
        conf.env.DEFINES.append('DEBUG=1')
        conf.env.CXXFLAGS.extend(['-Wno-unused-parameter', '-Wno-sign-compare', '-Wno-unused-local-typedefs', '-Wall', '-Wextra','-g'])
    else:
        conf.env.DEFINES.append('NDEBUG=1')
        conf.env.CXXFLAGS.extend(['-O3', '-march=nocona'])

    conf.check(header_name='stdio.h', features='cxx cxxprogram', mandatory=True)

    if sys.platform != 'win32':
        if not conf.env['LIB_PTHREAD']:
            conf.check_cxx(lib = 'pthread')

def options(ctx):
    ctx.load('compiler_cxx')

    gr = ctx.get_option_group('configure options')

    gr.add_option('--enable-rpath', action='store_true', default = False, help = 'Set RPATH to build/install dirs')
    gr.add_option('--enable-install-rpath', action='store_true', default = False, help = 'Set RPATH to install dir only')
    gr.add_option('--enable-build-rpath', action='store_true', default = False, help = 'Set RPATH to build dir only')

    gr.add_option('--debug', action='store_true', default = False, help = 'Build with debug flags')
    gr.add_option('--profile', action='store_true', default = False, help = 'Build with debug and profiler flags')
    gr.add_option('--release', action='store_true', default = False, help = 'Build with compiler optimizations')

def gitversion():
    if not os.path.isdir(".git"):
        print("This does not appear to be a Git repository.")
        return
    try:
        HEAD = open(".git/HEAD");
        headtxt = HEAD.read();
        HEAD.close();

        headtxt = headtxt.split("ref: ")
        if len(headtxt) == 2:
            fname = ".git/"+headtxt[1].strip();
            master = open(fname);
            mastertxt = master.read().strip();
            master.close();
        else:
            mastertxt = headtxt[0].strip()

    except EnvironmentError:
        print("unable to get HEAD")
        return "unknown"
    return mastertxt

def build(bld):
    bld.env.DEFINES.append('__version__="%s"' % gitversion())

    bld.recurse('src')
