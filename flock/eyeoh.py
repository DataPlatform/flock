import tempfile
import shutil
import os
import contextlib
import glob
import sys
from .output import safefile


@contextlib.contextmanager
def fileinput(args):
    """
        Set up file-like input vector based on args named in file_input_parser
    """
    if args.infile:
        assert os.path.exists(args.infile)
        infile = open(args.infile)
    else:
        infile = sys.stdin
    yield infile


@contextlib.contextmanager
def multifileinput(args):
    """
        Set up file-like input vectors based on args named in multifile_input_parser
    """
    print args
    infiles = (open(f) for f in args.infiles)
    yield infiles


@contextlib.contextmanager
def fileoutput(args):
    """
        Set up file-like output vector based on args named in file_output_parser
    """
    if args.outfile:
        parent_dir = os.path.split(args.outfile)[0]
        assert os.path.exists(parent_dir)
        if os.path.exists(args.outfile):
            # do writing in a tempfile and swap it in on close
            with safefile(args.outfile) as outfile:
                yield outfile
        else:
            outfile = open(args.outfile, 'w')
            yield outfile
            outfile.close()
    else:
        outfile = sys.stdout
        yield outfile
        outfile.close()
