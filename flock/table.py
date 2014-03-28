import os
import glob
import re
# from psycopg2.extras import Json

from .tools.csv_import import csv_import
from .annotate import operation
from .fancycsv import FancyReader, UnicodeWriter
from .error import *
from .tools.csv_to_ddl import csv_to_ddl


class Table(object):

    def __init__(self, table_name, schema):
        """
            For table specific ops
        """
        self.name = table_name
        self.database_table_name = table_name.lower()
        self.schema = schema
        self.full_name = self.schema.name + '.' + self.database_table_name
        self.ddl_filename = self.get_schema_filename('definition')
        self.data_root = os.path.abspath(
            '{self.schema.settings.DATA_DIRECTORY}/{self.schema.db_name}/{self.name}'.format(**dict(self=self)))

        # Need to store mappings of field names when they are transformed from
        # incoming csv's

        self.logger = self.schema.logger

        if not os.path.exists(self.data_root):
            os.makedirs(self.data_root)

    def __unicode__(self):
        "Returning only the name. (useful for string formatting)"
        return self.full_name

    # DDL File interactions
    def ddl_exists(self):
        return os.path.exists(self.ddl_filename)

    def get_schema_filename(self, function):
        "Returns a calculated filename where code should be stored for the given function"
        filename = os.path.join(
            self.schema.schema_dir, function, self.name + '.sql')
        directory = os.path.dirname(filename)
        if not os.path.exists(directory):
            os.makedirs(directory)
        return filename

    @operation
    def get_ddl(self, temporary=False):
        "Returns the stored DDL from a file"
        ddl = open(self.ddl_filename).read()
        if temporary:
            ddl = re.sub("create table", "create temporary table",
                         ddl, re.IGNORECASE)
            temp_tablename = self.schema.uuid('tmp')
            ddl = ddl.replace(self.full_name, temp_tablename)
            return ddl, temp_tablename
        else:
            # only applying constraints to the actual table and not staging
            # tables
            ddl += self.get_constraints()
            return ddl

    @operation
    def set_ddl(self, ddl, fieldmap=None):
        "Stores the provided DDL in definitions/{self.name}.sql"
        open(self.ddl_filename, 'wb').write(ddl)
        self.logger.debug('Wrote ddl to {0}'.format(self.ddl_filename))
        key = self.name + '/csvfieldmap'
        self.schema.set_metadata(key, fieldmap)
        self.logger.debug('Updated metadata at key {0}'.format(key))

    @operation
    def get_constraints(self):
        "Looks for any table constraints in constraints/{self.name}.sql"
        filename = self.get_schema_filename('constraint')
        if os.path.exists(filename):
            sql = open(filename).read()
        else:
            sql = ''
        return sql

    @operation
    def apply_ddl(self):
        self.schema.logger.debug("Applying ddl from file to database")
        sql = self.get_ddl()
        if self.table_exists():
            self.schema.logger.warn("Deleting existing database table")
            self.clean_database()
        self.schema.execute(sql)

    @operation
    def clean_ddl(self):
        """
            Delete ddl statemnents from the definitions directory
        """
        if os.path.exists(table.ddl_filename):
            os.remove(table.ddl_filename)

    @operation
    def generate_ddl_from_slice_data(self):
        """
            Generate DDL statements based on the shape of the slice data
        """
        slices = self.get_slice_filenames()
        if not slices:
            raise FlockNoData("Refusing to build ddl with empty slices")
        try:
            infiles = (open(f, 'rb') for f in slices)
            ddl, fieldmap = csv_to_ddl(
                infiles, self.full_name, logger=self.logger, encoding='utf-8')
            self.set_ddl(ddl, fieldmap=fieldmap)
        except StopIteration as e:
            raise FlockNoData('Table appears to have no slice data ' + str(e))

    def mapper(self):
        return self.schema.get_mapper('{0}/csvfieldmap'.format(self.name))

    # Data file interactions

    # Slices represent chunks of the table stored in memory or as csv files
    # The order and manner in which they are applied to the database is
    # completely undefined by default.

    @operation
    def write_slice_to_file(self, slice, slice_name=None):
        "Takes a 2d array (with headers) and dumps it to a csv file"
        if not slice_name:
            slice_name = self.schema.uuid('slice')
        filename = self.get_slice_filename(slice_name)

        if os.path.exists(filename):
            self.logger.debug("Overwriting slice file at {0}".format(filename))
        else:
            self.logger.debug("New slice file at {0}".format(filename))

        writer = UnicodeWriter(open(filename, 'wb'))
        ct = -1
        for row in slice:
            writer.writerow(row)
            ct += 1

        if ct <= 0:
            self.logger.error("Disregarding empty slice {0}".format(filename))
            os.remove(filename)
        else:
            self.logger.info(
                "Wrote {0} records to slice file {1}".format(ct, filename))

        return filename, slice_name

    @operation
    def read_slice_from_file(self, slice_name):
        "Reads data from a named csv file"
        filename = os.path.join(self.data_root, 'slice', slice_name)
        reader = FancyReader(open(filename, 'rb'), encoding='utf-8')
        return list(reader)

    @operation
    def apply_slices_to_database(self, transaction):
        "Makes sure all slices are loaded. Does not attempt to load a slice if it has been loaded before and there is a record"
        inserted_slices = self.query_metadata('inserted_slice')
        filenames = self.get_slice_filenames()
        new_slices = [
            f for f in filenames if os.path.basename(f) not in inserted_slices]
        for n in new_slices:
            self.logger.debug('New slice is about to be applied {0}'.format(n))
        report = self.hot_insert_file_data(new_slices, transaction)
        for f in new_slices:
            self.set_metadata('inserted_slice', os.path.basename(f))
        return report

    def get_slice_filenames(self):
        "Globs a list of availible files for the given function."
        data = dict(self=self, function='slice')
        p = "{self.data_root}/{function}/*".format(**data)
        filenames = sorted(glob.glob(p))
        return filenames

    def get_slice_names(self):
        "Returns a list of slice names based on filesystem contents."
        return [os.path.basename(filename) for filename in self.get_slice_filenames()]

    def get_slice_filename(self, slice_name):
        filename = os.path.join(self.data_root, 'slice', slice_name)
        directory = os.path.dirname(filename)
        if not os.path.exists(directory):
            os.makedirs(directory)
        return filename

    @operation
    def clean_slice_data(self):
        for filename in self.get_slice_filenames():
            self.logger.debug("Cleaning {0}".format(filename))
            os.remove(filename)

    # Database interactions
    def primary_keys(self):
        cursor = self.schema.execute(""" SELECT               
          pg_attribute.attname, 
          format_type(pg_attribute.atttypid, pg_attribute.atttypmod) 
        FROM pg_index, pg_class, pg_attribute 
        WHERE 
          pg_class.oid = '{name}'::regclass AND
          indrelid = pg_class.oid AND
          pg_attribute.attrelid = pg_class.oid AND 
          pg_attribute.attnum = any(pg_index.indkey)
          AND indisprimary """.format(name=self.full_name))

        return [row[0] for row in cursor.fetchall()]

    def fieldnames(self):
        sql = """SELECT column_name 
            from information_schema.columns 
            where table_name = '{0}'
            and table_schema = '{1}'; """.format(self.database_table_name, self.schema.name)
        self.logger.debug(sql)
        cursor = self.schema.execute(sql)

        return [row[0] for row in cursor.fetchall()]

    def get_metadata(self, function):
        return self.schema.get_database_specific_metadata(self.name, function)

    def query_metadata(self, function):
        return self.schema.query_database_specific_metadata(self.name, function)

    def set_metadata(self, function, data):
        return self.schema.set_database_specific_metadata(self.name, function, data)

    def table_exists(self):
        return self.schema.selectone("""SELECT (SELECT count(*) FROM information_schema.tables 
            where table_name = %(table_name)s
            and table_schema = %(schema_name)s) > 0""",
                                     dict(table_name=self.database_table_name,
                                          schema_name=self.schema.name)
                                     )

    def number_of_rows_in_db(self):
        if self.table_exists():
            return self.schema.selectone('select count(*) from {0}'.format(self.full_name))
        else:
            return 0

    def clean_database(self,deep=False):
        if not deep:
            #preserving metadata that does not relate directly to the state of the database
            sql = 'delete from {0}.flock where key = \'{1}\' and function = \'inserted_slice\';'.format(
                self.schema.name,self.name)
        else:
            sql = 'delete from {0}.flock where key = \'{1}\';'.format(
                self.schema.name,self.name)
        self.schema.execute(sql)
        return self.schema.execute('drop table {0}'.format(self.full_name))

    def clean(self):
        if self.table_exists():
            self.clean_database(deep=True)
        self.schema.execute('delete from {0}.flock where key = \'{1}\';'.format(
                self.schema.name,self.name))
        self.clean_slice_data()


    @operation
    def grant(self):
        for privilege, users in self.schema.settings.DATABASE_PERMISSIONS.iteritems():
            self.schema.execute('grant {privilege} on {full_name} to {users}', **dict(
                users=','.join(users),
                privilege=privilege,
                full_name=self.full_name
            ))

    @operation
    def hot_insert_file_data(self, infiles, transaction, encoding='utf-8'):
        """
            Take data from staging csv's and insert them to the database
             - Uses a temp table to stage data
             - Guards agains dupes
             - Needs an open transaction
        """

        num_files = len(infiles)
        primary_keys = ','.join(self.primary_keys())

        if primary_keys:
            # Upload to temp table remove duped records and then flush temp table to perm table
            # must do one file at a time do guard against dupes that can
            # sometimes come of the wire from RN
            report = dict(
                method='fancy',
                files_inserted=0,
                rows_updated=0,
                rows_inserted=0
            )
            for infile in infiles:

                temp_table_ddl, temp_tablename = self.get_ddl(temporary=True)

                args = infile, temp_tablename
                self.logger.debug(
                    "PK found! Hot inserting data from {0} via {1}".format(*args))
                rows_before = self.schema.selectone(
                    'select count(*) from {0};'.format(self.full_name))

                self.schema.execute(temp_table_ddl)
                self.logger.debug(
                    'hot_insert {0} {1}'.format(encoding, infile))
                csv_import([open(infile, 'rb')], temp_tablename,
                           self.schema.db, mapper=self.mapper(), encoding=encoding)
                kwargs = dict(full_name=self.full_name,
                              temp_name=temp_tablename, primary_keys=primary_keys)

                slice_size = self.schema.selectone(
                    'select count(*) from {0};'.format(temp_tablename))
                try:
                    savepoint1 = transaction.savepoint()
                    self.logger.debug('Trying dumb insert')
                    self.schema.execute("INSERT into {full_name} \
                        select * from {temp_name}".format(**kwargs))

                except IntegrityError:
                    # Dupes were found somewhere. Take latter value.
                    self.logger.debug(
                        'Dumb insert yielded IntegrityError: Trying with lookahead dupe deletion...')
                    transaction.return_to_savepoint(savepoint1)
                    self.schema.execute("delete from {full_name} \
                        where {primary_keys} in \
                        (select {primary_keys} from {temp_name});".format(**kwargs))

                    try:
                        savepoint2 = transaction.savepoint()
                        self.schema.execute("INSERT into {full_name} \
                            select * from {temp_name}".format(**kwargs))

                    except IntegrityError:
                        # Dupes were found inside the same file. Take first
                        # value.
                        transaction.return_to_savepoint(savepoint2)
                        self.logger.debug(
                            'Lookahead dupe deletion also yielded IntegrityError: Trying rank() insert...')
                        kwargs['fieldnames'] = ','.join(self.fieldnames())
                        self.schema.execute("""INSERT into {full_name} 
                            SELECT {fieldnames} from  (
                              SELECT * ,rank() OVER (PARTITION BY ID order by random()) 
                                FROM {temp_name}
                            ) t where t.rank = 1;""".format(**kwargs))

                        self.logger.debug('rank() insert completed')

                self.schema.execute("drop table {temp_name};".format(**kwargs))

                rows_after = self.schema.selectone(
                    'select count(*) from {full_name};'.format(**kwargs))
                args = rows_before, rows_after
                self.logger.debug(
                    "There were {0} rows before and {1} rows after the file was processed.".format(*args))

                # Calculate report
                rows_inserted = rows_after - rows_before
                report['files_inserted'] += 1
                report['rows_updated'] += (slice_size - rows_inserted)
                report['rows_inserted'] += rows_inserted

        else:
            data = self.full_name, num_files
            self.logger.debug(
                "No PK found on {0}. Inserting data from {1} files.".format(*data))
            report = dict(
                method='simple',
                rows_updated=0,
                files_inserted=len(infiles),
            )
            infiles = (open(f) for f in infiles)
            csv_import(infiles, self.full_name, self.schema.db,
                       mapper=self.mapper(), encoding=encoding)

        return report




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
