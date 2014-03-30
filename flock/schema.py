import os
import traceback
import inspect
import uuid
import glob
import json
import contextlib
import re
from collections import defaultdict
# from psycopg2.extras import Json

from flock.tools.csv_import import csv_import
from .annotate import operation, command, test
from .log import get_logger
from .fancycsv import FancyReader, UnicodeWriter
from .error import *
from .table import Table
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
from flock.tools.csv_to_ddl import csv_to_ddl

from psycopg2 import IntegrityError, ProgrammingError, DataError
from flock.parsers import optional_named_db_parser, schema_parser, optional_tables_parser
import argparse
from argparse import RawTextHelpFormatter


class Schema(object):

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
            parents=[optional_named_db_parser, schema_parser]
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
        # in
        if args.db_name:
            self.db_name = args.db_name
        else:
            assert self.settings.DEFAULT_DATABASE
            self.db_name = self.settings.DEFAULT_DATABASE

        # set up logging
        if self.settings.LOG_TO_EMAIL:
            #mailhost, fromaddr, toaddrs, subject
            smtp_args = [
                self.settings.SMTP_SERVER,
                self.settings.OWNER_EMAIL,
                self.settings.LOG_DIST_LIST,
                "Flock log for {0} in {1}".format(self.name, self.db_name)
            ]
        else:
            smtp_args = None

        log_directory = os.path.join(self.settings.LOG_DIRECTORY, self.db_name)
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)
        log_filename = os.path.join(
            log_directory, '{0}.log'.format(args.command if args.command else 'schema'))
        self.logger = get_logger(
            'schema.' + self.name, log_filename, smtp_args=smtp_args)
        self.logger.info('Logging online {0}'.format(self.name))

        # establish schema folder

        self.schema_dir = self.settings.SCHEMA_NAME

        # if not os.path.exists(self.schema_dir):
        #     if raw_input('Schema {0} does not exist. Do you want to create it? [y/N]:'.format(self.schema_dir)) in ('y', 'Y'):
        #         os.makedirs(self.schema_dir)
        #         open(os.path.join(self.schema_dir, '.generated'), 'w').write(
        #             'Code generation in play. Look out and use scm!')
        #     else:
        #         exit()
        #     for d in ['definitions', 'pipelines', 'access']:
        #         os.mkdir(os.path.join(self.schema_dir, d))

        # get list of targeted tables
        # - targeted tables are a reaction to the constant desire to pass a table name
        #   around with the schema class which should definitely be avoided
        # - there is room for improvement here

        if args.tables:
            self.settings.TABLES = args.tables



        # set up tables
        self.tables = OrderedDict(((table_name, self.TableClass(table_name, self))
                                  for table_name in self.settings.TABLES))





    def export_metadata(self):
        """
            Only call after successful operations
        """
        self.logger.info('Exporting metadata to' + self.metadatafile_path)
        open(self.metadatafile_path, 'w').write(
            json.dumps(self.metadata, indent=2))

    def get_path(self, function, *args):
        """
            Calculate the directory path where files should go. Additional 
            arguments are mapped to subdirectories in the schema directory.
        """
        if not override:
            return os.path.join(self.schema_dir, function, *args)
        else:
            return override

    def get_mapper(self, md_key):
        "Returns a function that maps unstandardized fieldnames to standardized ones"

        print 'Attempting to find metadata for key:', md_key
        if md_key in self.metadata:
            # A mapping is configured for this key
            fieldmap = self.metadata[md_key]
            assert type(fieldmap) == OrderedDict
            mapper = lambda x: fieldmap[x]
            # self.logger.debug("*Using the following field mappings*")
            # for k,v in fieldmap.iteritems():
            #     self.logger.debug('{0}: {1}'.format(k,v))
        else:
            mapper = lambda x: x
        return mapper

    def set_metadata(self, key, value):
        assert json.dumps(value)
        self.metadata[key] = value

    def log_traceback(self):
        self.logger.error('\n{0}'.format(traceback.format_exc()))

    def uuid(self, slug):
        return slug + str(uuid.uuid4()).replace('-', '')

    # Database helpers
    def execute(self, sql, *args, **kwargs):
        """
            Does some string formatting before calling cursor.execute. 
        """
        cursor = self.db.cursor()
        sql = sql.format(*args, schema=self, **kwargs)
        sql_log_msg = 'Running sql `{0}` (clipped at 200 chars)'.format(
            ' '.join(sql.split())[:200])
        self.logger.debug(sql_log_msg)
        cursor.execute(sql, *args)
        return cursor

    def selectone(self, sql, *args, **kwargs):
        " Convenience method for queries that return one value"
        assert sql.strip().lower().startswith('select')
        cursor = self.db.cursor()
        cursor.execute(sql, *args, **kwargs)
        answer = cursor.fetchone()
        if answer != None:
            answer = answer[0]
        sql_log_msg = 'Statement is {0} `{1}` (clipped at 200 chars)'.format(
            answer, ' '.join(cursor.query.split())[:200])
        self.logger.debug(sql_log_msg)
        return answer

    def _schema_exists(self):
        "Test if this schema exists in the active database"
        return self.selectone(
            'select (SELECT count(*) FROM information_schema.schemata WHERE schema_name =%(name)s) > 0;',
            dict(name=self.name)
        )

    def _init_schema_with_db(self):
        "Activate database with this schema"
        assert self.db
        self.execute('create schema {schema.name};')
        self.execute('grant usage on schema {schema.name} to {users}',
                     **dict(users=','.join(self.settings.DATABASE_USERS)))
        self.execute('''CREATE table {schema.name}.flock (
                            id serial PRIMARY KEY,
                            time timestamp default current_timestamp,
                            key text,
                            function text,
                            data json
             );''')

    @contextlib.contextmanager
    def transaction(self):
        """
            Use this to open and close tranactions. The yielded Transaction 
            object has an interface for managing savepoints. 
        """
        if not self.transaction_open:
            try:
                self.logger.debug("Opening database transction")
                self.transaction_is_open = True
                yield Transaction(self)
            except Exception as e:
                self.db.rollback()
                self.logger.error(
                    'Database transaction rolled back. \n{0}'.format(traceback.format_exc()))
                raise e
        else:
            self.logger.error("Transaction is already open.")
            raise Exception("Transactions cannot be opened twice")

        self.transaction_is_open = False
        self.db.commit()
        self.logger.debug('Committing database transaction')

    @operation
    def set_database_specific_metadata(self, key, function, data):
        sql_template = "insert into {0}.flock (key,function,data) VALUES (%s,%s,%s)".format(
            self.name)
        self.execute(sql_template, [key, function, CustomJson(data)])

    @operation
    def get_database_specific_metadata(self, key, function):
        sql_template = "select data from {0}.flock where key = %s and function = %s order by id desc".format(
            self.name)
        return self.selectone(sql_template, [key, function])

    @operation
    def query_database_specific_metadata(self, key, function):
        sql_template = "select data from {0}.flock where key = %s and function = %s order by id desc".format(
            self.name)
        c = self.execute(sql_template, [key, function])
        data = [row[0] for row in c.fetchall()]
        return data

    # Operations

    @operation
    def run_pipeline(self, pipeline):
        "Runs the specified pipeline"
        path = os.path.join(self.schema_dir, 'pipelines', pipeline)
        if os.path.isdir(path):
            steps = glob.glob(os.path.join(path, "*"))
        else:
            steps = [path]
        for i, step in enumerate(steps):
            step_name = os.path.basename(step)
            self.logger.info(
                "Running pipeline step {0}: {1}".format(i, step_name))
            self.execute(open(step).read())

    # Commands
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

        if raw_input("Are you sure you want to delete everything in schema {0} on database {1}? (N/y) ".format(self.name, self.db_name)) in ('y', 'Y'):
            self.logger.info(
                "Cleaning schema {0} on database {1}".format(self.name, self.db_name))
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
        "Introspection to discover registered commands and present a"

        # Retrieve and call the command
        getattr(self, self.args.command)()

# Dont call it a factory!


@contextlib.contextmanager
def get_schema(args, self=Schema):
    "Context manager to set up and tear down Schemas"
    schema = self(args)
    yield schema
    schema.export_metadata()


class Transaction:

    "For managing savepoints inside Postgres transactions"

    def __init__(self, schema):
        self.schema = schema

    def savepoint(self):
        "Sets a savepoint"
        id = self.schema.uuid('sp')
        self.schema.execute('SAVEPOINT {0};'.format(id))
        self.schema.logger.debug('Setting database savepoint {0}'.format(id))
        return id

    def return_to_savepoint(self, id):
        "Returns to the specified savepoint"
        self.schema.logger.warn(
            'Returning to database savepoint {0}'.format(id))
        self.schema.execute('ROLLBACK TO SAVEPOINT {0};'.format(id))
