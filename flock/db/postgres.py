import sys
import os
import argparse
import psycopg2
import glob
import contextlib
from psycopg2.extras import Json, register_json
import json
import datetime as dt
from collections import OrderedDict
from functools import partial
from ..annotate import operation


db_parser = argparse.ArgumentParser(add_help=False)
db_parser.add_argument("db_uri", help="Database URI", type=str)


def dial(uri):
    """
        Parses URI's that loook like this and returns a psycopg connection:
            db://test_user:test_pw@localhost/test_db
    """
    import urlparse
    uri = urlparse.urlparse(uri.strip())
    username = uri.username
    password = uri.password
    database = uri.path[1:]
    hostname = uri.hostname
    connection = psycopg2.connect(
        database=database,
        user=username,
        password=password,
        host=hostname
    )
    register_json(connection, loads=loads)
    return connection


def cull_date_strings(v):
    if isinstance(v, basestring):
        try:
            v = dt.datetime.strptime(v, '%Y-%m-%d').date()
        except ValueError:
            try:
                v = dt.datetime.strptime(v, '%Y-%m-%dT%H:%M:%S.%f%Z')
            except ValueError:
                pass
    return v


def loads(str):
    obj = json.loads(str, object_hook=smart_object_load)

    # object_hook only runs on dicts so need to check for singleton datestrings
    if isinstance(obj, basestring):
        obj = cull_date_strings(obj)
    return obj


def smart_object_load(pairs):
    "For custom rehydrating of postgres Json data"
    data = OrderedDict()
    for k, v in pairs.iteritems():
        data[k] = cull_date_strings(v)

    return data

# def loads(str):
#     return json.loads(str, object_pairs_hook=smart_object_load)


class CustomJson(Json):

    def dumps(self, obj):
        if isinstance(obj, dt.datetime) or isinstance(obj, dt.date):
            obj = obj.isoformat()
        return json.dumps(obj)

# todo This is test logic

dthandler = lambda obj: obj.isoformat() if isinstance(
    obj, dt.datetime) else None
dumps = partial(json.dumps, default=dthandler)
json.dumps(dt.datetime.now(), default=dthandler)


# class DateTimeJSONEncoder(json.JSONEncoder):
#     def default(self, obj):
#         if isinstance(obj, dt.datetime):
#             return obj.isoformat()
#         else:
#             return super(DateTimeJSONEncoder, self).default(obj)
# dumps =  partial(DateTimeJSONEncoder().encode)


class Driver(object):

    def __init__(self):
        assert self.settings
        # set up db connection

        self.transaction_open = False

        db_uri = self.settings.DATABASES[self.environment_name]
        self.logger.info("Dialing {0}".format(db_uri))
        self.db = dial(db_uri)

        # set up schema inside database if it isn't already there

        if self.db and not self._schema_exists():
            with self.transaction() as transaction:
                self._init_schema_with_db()


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


    # The database journal is a way to store state and structured log
    # information that is scoped the database (i.e. This info goes away when
    # the database is swapped out.)

    @operation
    def set_database_journal(self, key, function, data):
        sql_template = "insert into {0}.flock (key,function,data) VALUES (%s,%s,%s)".format(
            self.name)
        self.execute(sql_template, [key, function, CustomJson(data)])

    @operation
    def get_database_journal(self, key, function):
        sql_template = "select data from {0}.flock where key = %s and function = %s order by id desc".format(
            self.name)
        return self.selectone(sql_template, [key, function])

    @operation
    def query_database_journal(self, key, function):
        sql_template = "select data from {0}.flock where key = %s and function = %s order by id desc".format(
            self.name)
        c = self.execute(sql_template, [key, function])
        data = [row[0] for row in c.fetchall()]
        return data

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


class Transaction:

    "For managing savepoints inside Postgres transactions"

    def __init__(self, driver):
        self.driver = driver

    def savepoint(self):
        "Sets a savepoint"
        id = self.driver.uuid('sp')
        self.driver.execute('SAVEPOINT {0};'.format(id))
        self.driver.logger.debug('Setting database savepoint {0}'.format(id))
        return id

    def return_to_savepoint(self, id):
        "Returns to the specified savepoint"
        self.driver.logger.warn(
            'Returning to database savepoint {0}'.format(id))
        self.driver.execute('ROLLBACK TO SAVEPOINT {0};'.format(id))

class Pipeline(object):

    def __init__(self):
        assert self.settings
