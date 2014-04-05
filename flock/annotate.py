# enable annotation & inspoection for bridging
# flock.py with interfaces defined in a specific schema
from datetime import datetime
import inspect
import traceback
import logging
from flock.exceptions import *

logger = logging.getLogger('flock.pre_init')


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


def flock(application_class):
    """ Wraps a flock application. Does two things:
     - When using mixins not all __init__ methods are called: This changes 
       that behavior to ensure that all __init__methods are called.
     - Checks for conflicts on class methods annotated as operations.

    """






    # Get the __init__ method for the decorated class
    this_init = getattr(application_class, '__init__', None)

    # Make a new method to replace the current __init__ that links the chain
    def new_init(self, *args, **kwargs):

        logger.info(
            "Initializing {0}".format(application_class.__name__))


        # In the absence of having interfaces, we should at least check for
        # conflicts among operations

        # Keys are method names and values are the name of the class that provides
        # the method
        all_methods = dict()

        # iterate through base classes, reversed so it is easy to see what is
        # being overridden

        interface_errors = False

        for cls in reversed(application_class.__bases__):
            # all attributes
            for name in dir(cls):
                attr = getattr(cls, name)

                # check that the method and that it has been annotated as an
                # @operation
                if inspect.ismethod(attr) and 'operation' in attr.im_func.__dict__:

                    if name in all_methods:
                        application_class.logger.error("Operation {0}() defined in {1} is being overridden by {2}".format(
                            name, all_methods[name], cls.__name__))
                        interface_errors = True
                    all_methods[name] = cls.__name__

        if interface_errors:

            raise InterfaceConflict()

        # Do top level application initialization
        if this_init:
            this_init(self, *args, **kwargs)

        # Iterate through each component of the application and perform
        # __init__
        for component_class in application_class.__bases__[:]:

            component_init = getattr(component_class, '__init__', None)
            if component_init:
                logger.debug(
                    "Initializing {0}".format(component_class.__name__))
                component_init(self, *args, **kwargs)



    # Swap in the new method
    setattr(application_class, '__init__', new_init)



    return application_class
