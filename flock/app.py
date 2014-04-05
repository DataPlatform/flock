import os
import traceback
import inspect
import uuid
import glob
import json
import contextlib
import re
from collections import OrderedDict
from collections import defaultdict

from psycopg2 import IntegrityError, ProgrammingError, DataError
import argparse
from argparse import RawTextHelpFormatter

from .exceptions import *
from .parsers import optional_named_environment_parser, schema_parser, optional_tables_parser
from .tools.csv_import import csv_import
from .tools.csv_to_ddl import csv_to_ddl
from .annotate import operation, command, test
from .log import get_logger
from .fancycsv import FancyReader, UnicodeWriter
from .table import Table



class BaseApp(object):

    """
        Class to structure interactions with schema definitions and data import workflows.
        i.e. Layer all the file management and some db operations

    """

    def __init__(self):

        # create the top-level parser
        parser = argparse.ArgumentParser(
            prog='python schema.py',
            description='Set off all operations specific to this schema',
            formatter_class=RawTextHelpFormatter,
            parents=[optional_named_environment_parser, schema_parser]
        )

        subparsers = parser.add_subparsers(
            title='Availible commands',
            dest='command',
            metavar='<command>',
        )

        # Iterate through every attribute on the schema class and register
        # commands
        for attr_name in dir(self):
            attr = getattr(self, attr_name)
            # Only callables (methods in this context) can have decorators
            if inspect.ismethod(attr):

                if 'command' in attr.im_func.__dict__:
                    # schema_parser = command_subparsers.add_parser(
                    subparsers.add_parser(
                        attr.im_func.func_name,
                        help=attr.im_func.func_doc,
                        parents=[optional_tables_parser]
                    )

        # Parse the args
        self.args = args = parser.parse_args()

        # Check that settings have been configured
        assert self.settings
        assert self.settings.SCHEMA_NAME

        # Assign a custom Table class to InjectedTableClass
        # in your schema to override or create new functionality
        try:
            self.TableClass = self.InjectedTableClass
        except AttributeError:
            self.TableClass = Table

        assert self.TableClass

        self.name = os.path.split(self.settings.SCHEMA_NAME)[1]

        # Init the database name early so we can test which environment we are
        if args.environment_name:
            self.environment_name = args.environment_name
        else:
            assert self.settings.ENVIRONMENT
            self.environment_name = self.settings.ENVIRONMENT

        # set up logging
        if self.settings.LOG_TO_EMAIL:
            #mailhost, fromaddr, toaddrs, subject
            smtp_args = [
                self.settings.SMTP_SERVER,
                self.settings.OWNER_EMAIL,
                self.settings.LOG_DIST_LIST,
                "Flock log for {0} in {1}".format(self.name, self.environment_name)
            ]
        else:
            smtp_args = None

        log_directory = os.path.join(self.settings.LOG_DIRECTORY, self.environment_name)
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)
        log_filename = os.path.join(
            log_directory, '{0}.log'.format(args.command if args.command else 'schema'))
        self.logger = get_logger(
            'schema.' + self.name, log_filename, smtp_args=smtp_args)
        self.logger.info('Logging online {0}'.format(self.name))

        # Establish schema folder
        self.schema_dir = self.settings.SCHEMA_NAME


        # Check for table list in CL args
        if args.tables:
            self.settings.TABLES = args.tables

        # Instantiate tables
        self.tables = OrderedDict(((table_name, self.TableClass(table_name, self))
                                  for table_name in self.settings.TABLES))



    def get_path(self, function, *args):
        """
            Calculate the directory path where files should go. Additional 
            arguments are mapped to subdirectories in the schema directory.
        """
        if not override:
            return os.path.join(self.schema_dir, function, *args)
        else:
            return override



    def log_traceback(self):
        self.logger.error('\n{0}'.format(traceback.format_exc()))

    def uuid(self, slug):
        return slug + str(uuid.uuid4()).replace('-', '')








    # Default Commands
    @command
    def shell(self):
        "Initialize Schema and drop into an IPython shell"

        self.logger.info("Entering interactive shell")
        schema = self

        # Make a datastructure fot tables that is tab completion friendly
        class O:
            pass
        tables = O()
        for k, v in self.tables.iteritems():
            setattr(tables, k, v)

        # Make a shell
        import IPython
        IPython.embed()
        self.logger.info("Exiting interactive shell")

    @command
    def clean_database(self):
        "Cleans everything under the schema in the active database. Use with caution!"

        if raw_input("Are you sure you want to delete everything in schema {0} on database {1}? (N/y) ".format(self.name, self.environment_name)) in ('y', 'Y'):
            self.logger.info(
                "Cleaning schema {0} on database {1}".format(self.name, self.environment_name))
            with self.transaction():
                self.execute("drop schema {schema.name} cascade;")
                self._init_schema_with_db()
        else:
            self.logger.info("Nothing to do, exiting.")

    @command
    def clean(self):
        "Delete all data in the database and on the filesystem"
        self.clean_database()
        for table in self.tables.itervalues():
            table.clean_slice_data()

    @command
    def test(self):
        for test in self._get_annotated_methods('test'):
            test(self)

    @command
    def main(self):
        "Default command (not yet implemented)"
        raise NotImplementedError
    # Internal

    def _get_annotated_methods(self, annotation):
        "Returns references to operations/commands/tests that have the corresponding decorator attached"

        for attr_name in dir(self.__class__):
            attr = getattr(self.__class__, attr_name)
            if inspect.ismethod(attr):
                if annotation in attr.im_func.__dict__:
                    yield attr

    # Entry Points
    def enter(self):
        "Run the command given in the CLI"
        if self.args.command:
            # Retrieve and call the command
            getattr(self, self.args.command)()
        else:
            self.main()








