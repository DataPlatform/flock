# enable annotation & inspoection for bridging
# flock.py with interfaces defined in a specific schema
from datetime import datetime
import inspect
import traceback
import logging


def log_func(func, func_type, log_level):

    name = func.func_name

    def logged_function(self, *args, **kwargs):
        ref_args = inspect.getargs(func.func_code).args
        report = ''
        ref_args = inspect.getargs(func.func_code).args
        for i, k in enumerate(ref_args):

            if k == 'self':
                continue
            try:
                v = args[i - 1]
            except IndexError:
                if k in kwargs:
                    v = kwargs[k]
                else:
                    v = None

            report += '{0}:{1} '.format(k, str(v)
                                        [:100].encode('utf-8').replace('\n', ' '))

        self.logger.log(
            log_level, '@{0} `{1}` Starting with args: {2}'.format(func_type, name, report))
        try:
            data = func(self, *args, **kwargs)
        except (Exception, KeyboardInterrupt) as e:
            self.logger.error('@{0} `{1}` Failed with error: {2} and args: {3}\n{4}'.format(
                func_type, name, e, report,
                traceback.format_exc()
            ))
            raise e
        self.logger.log(
            log_level, '@{0} `{1}` Succeded with args: {2}'.format(func_type, name, report))
        return data

    logged_function.__dict__[func_type] = True
    logged_function.__doc__ = func.__doc__
    logged_function.__name__ = func.__name__
    return logged_function


def command(cmd):
    return log_func(cmd, 'command', logging.INFO)


def operation(op):
    return log_func(op, 'operation', logging.DEBUG)


def test(test):

    def logged_test(self, *args, **kwargs):
        name = test.func_name
        ans = test(self, *args, **kwargs)
        if ans:
            self.logger.info("Test {0} has passed".format(name))
        else:
            self.logger.error("Test {0} has failed".format(name))
        return ans

    logged_test.__dict__['test'] = True
    logged_test.__doc__ = test.__doc__
    logged_test.__name__ = test.__name__

    return logged_test
