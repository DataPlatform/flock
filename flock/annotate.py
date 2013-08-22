#enable annotation & inspoection for bridging 
#flock.py with interfaces defined in a specific schema
from datetime import datetime
import inspect,traceback


def command(cmd):
    cmd.__dict__['command'] = True
    return cmd

def operation(op):

    name = op.func_name
    
    def logged_operation(self, *args, **kwargs):
        report = ''
        ref_args = inspect.getargs(op.func_code).args
        for i,k in enumerate(ref_args):
            if k != 'self':
                try:
                    v = args[i-1]#offest because I am matching self
                    
                except IndexError:
                    if k in kwargs:
                        v = kwargs[k]
                    else:
                        v = None

                report += ' {0}:{1}'.format(k,str(v)[:100])




        self.logger.info('{0} - @Operation `{1}` Starting with args: {2}'.format(self.name,name,report))
        try:
            data = op(self, *args, **kwargs)
        except Exception as e:
            self.logger.error('{0} - @Operation `{1}` Failed with args: {2}'.format(self.name,name,e))
            self.logger.error('\n{0}'.format(traceback.format_exc()))
            raise e
        self.logger.info('{0} - @Operation `{1}` Succeded with args: {2}'.format(self.name,name,report))
        return data

    logged_operation.__dict__['operation'] = True
    logged_operation.__doc__ = op.__doc__
    logged_operation.__name__ = op.__name__
    return logged_operation

def test(test):

    def logged_test(self, *args, **kwargs):
        name = test.func_name
        ans = test(self,*args,**kwargs)
        if ans:
            self.logger.info("Test {0} has passed".format(name))
        else:
            self.logger.error("Test {0} has failed".format(name))
        return ans

    logged_test.__dict__['test'] = True
    logged_test.__doc__ = test.__doc__
    logged_test.__name__ = test.__name__

    return logged_test
