import tempfile
import shutil
import os
import contextlib
import os


@contextlib.contextmanager
def safefile(actual_path):
    """
        Don't actually write to the file; write to a tmp file and swap it in when done.
    """
    handle, path = tempfile.mkstemp(dir=os.path.abspath('./'))
    outfd, outsock_path = tempfile.mkstemp()
    outsock = open(outsock_path, 'w')
    yield outsock
    outsock.close()
    os.close(outfd)
    # print 'closing ',outsock_path
    shutil.move(outsock_path, actual_path)
    if os.path.exists(outsock_path):
        try:
            shutil.rmtree(outsock_path)
        except:
            pass
