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
        for i,k in enumerate(inspect.getargs(op.func_code).args):
        	if k != 'self':
	            v = args[i-1]#offest because I am matching self
	            report += ' {0}:{1}'.format(k,str(v)[:100])

        self.logger.info('Starting {0} with {1}'.format(name,report))
        try:
            data = op(self, *args, **kwargs)
        except Exception as e:
            self.logger.error('Operation {0} failed with {1}'.format(name,e))
            self.logger.error('\n{0}'.format(traceback.format_exc()))
            raise e
        self.logger.info('Operation {0} succeded with {1}'.format(name,report))
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
