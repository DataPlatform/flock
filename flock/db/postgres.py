import sys
import os
import argparse
import psycopg2
import glob
from psycopg2.extras import Json, register_json
import json
import datetime as dt
from collections import OrderedDict
from functools import partial
from flock.annotate import operation


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

class Pipeline(object):

    def __init__(self):
        assert self.settings
