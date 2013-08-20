# inspired by Alex Martelli's solution to
# http://stackoverflow.com/questions/1096216/override-namespace-in-python/1096247#1096247
import imp,os,sys
def import_from_path(fullpath):
    """Dynamic script import using full path."""

    script_dir, filename = os.path.split(fullpath)
    script, ext = os.path.splitext(filename)
    sys.path.insert(0,script_dir)
    (file, pathname, description) = imp.find_module(script)
    module = imp.load_module(script, file, pathname, description)
    del sys.path[0]
    return module
