import os,sys,traceback,inspect
import tempfile,shutil
import uuid,glob,json,contextlib,re
from collections import defaultdict
# from psycopg2.extras import Json

from bin.csv_import import csv_import
from .fancyimport import import_from_path
from .db import dial,CustomJson
from .annotate import operation,command,test
from .log import get_logger
from .fancycsv import FancyReader,UnicodeWriter
from .error import *
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
from bin.csv_to_ddl import csv_to_ddl

from psycopg2 import IntegrityError,ProgrammingError,DataError


class FlockTable(object):

    def __init__(self,table_name,schema):
        """
            For table specific ops
        """
        self.name = table_name
        self.database_table_name = table_name.lower()
        self.schema = schema
        self.full_name = self.schema.name + '.' + self.database_table_name
        self.ddl_filename = self.get_schema_filename('definition')
        self.data_root = os.path.abspath('{self.schema.settings.DATA_DIRECTORY}/{self.schema.db_name}/{self.name}'.format(**dict(self=self)))

        #Need to store mappings of field names when they are transformed from incoming csv's
        
        self.logger = self.schema.logger

        if not os.path.exists(self.data_root):
            os.makedirs(self.data_root)


    def __unicode__(self):
        "Returning only the name. (useful for string formatting)"
        return self.full_name

    #DDL File interactions
    def ddl_exists(self):
        return os.path.exists(self.ddl_filename)

    def get_schema_filename(self,function):
        "Returns a calculated filename where code should be stored for the given function"
        filename = os.path.join(self.schema.schema_dir,function,self.name + '.sql')
        directory = os.path.dirname(filename)
        if not os.path.exists(directory):
            os.makedirs(directory)
        return filename

    @operation
    def get_ddl(self,temporary=False):
        "Returns the stored DDL from a file"
        ddl = open(self.ddl_filename).read()
        if temporary:
            ddl = re.sub("create table","create temporary table", ddl, re.IGNORECASE)
            temp_tablename = self.schema.uuid('tmp')
            ddl = ddl.replace(self.full_name,temp_tablename)
            return ddl,temp_tablename
        else:
            #only applying constraints to the actual table and not staging tables
            ddl += self.get_constraints()
            return ddl

    @operation
    def set_ddl(self,ddl,fieldmap=None):
        "Stores the provided DDL in definitions/{self.name}.sql"
        open(self.ddl_filename,'wb').write(ddl)
        self.logger.debug('Wrote ddl to {0}'.format(self.ddl_filename))
        key = self.name + '/csvfieldmap'
        self.schema.set_metadata(key,fieldmap)
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
            infiles = (open(f,'rb') for f in slices)
            ddl,fieldmap = csv_to_ddl(infiles,self.full_name,logger=self.logger,encoding='utf-8')
            self.set_ddl(ddl,fieldmap=fieldmap)
        except StopIteration as e:
            raise FlockNoData('Table appears to have no slice data ' + str(e))

    def mapper(self):
        return self.schema.get_mapper('{0}/csvfieldmap'.format(self.name))

    # Data file interactions

    # Slices represent chunks of the table stored in memory or as csv files
    # The order and manner in which they are applied to the database is
    # completely undefined by default.

    @operation
    def write_slice_to_file(self,slice,slice_name=None):
        "Takes a 2d array (with headers) and dumps it to a csv file"
        if not slice_name:
            slice_name = self.schema.uuid('slice')
        filename = self.get_slice_filename(slice_name)


        if os.path.exists(filename):
            self.logger.debug("Overwriting slice file at {0}".format(filename))
        else:
            self.logger.debug("New slice file at {0}".format(filename))

        writer = UnicodeWriter(open(filename,'wb'))
        ct = -1
        for row in slice:
            writer.writerow(row)
            ct += 1

        if ct <= 0:
            self.logger.error("Disregarding empty slice {0}".format(filename))
            os.remove(filename)
        else:
            self.logger.info("Wrote {0} records to slice file {1}".format(ct,filename))

        return filename,slice_name

    @operation
    def read_slice_from_file(self,slice_name):
        "Reads data from a named csv file"
        filename = os.path.join(self.data_root,'slice',slice_name)
        reader = FancyReader(open(filename,'rb'),encoding='utf-8')
        return list(reader)

    @operation
    def apply_slices_to_database(self,transaction):
        "Makes sure all slices are loaded. Does not attempt to load a slice if it has been loaded before and there is a record"
        inserted_slices = self.query_metadata('inserted_slice')
        filenames = self.get_slice_filenames()
        new_slices = [f for f in filenames if os.path.basename(f) not in inserted_slices]
        for n in new_slices:
            self.logger.debug('New slice is about to be applied {0}'.format(n))
        report = self.hot_insert_file_data(new_slices,transaction)
        for f in new_slices:
            self.set_metadata('inserted_slice',os.path.basename(f))
        return report

    def get_slice_filenames(self):
        "Globs a list of availible files for the given function."
        data = dict(self=self,function='slice')
        p = "{self.data_root}/{function}/*".format(**data)
        filenames = sorted(glob.glob(p))
        return filenames

    def get_slice_names(self):
        "Returns a list of slice names based on filesystem contents."
        return [os.path.basename(filename) for filename in self.get_slice_filenames()]

    def get_slice_filename(self,slice_name):
        filename = os.path.join(self.data_root,'slice',slice_name)
        directory = os.path.dirname(filename)
        if not os.path.exists(directory):
            os.makedirs(directory)
        return filename

    @operation
    def clean_slice_data(self):
        for filename in self.get_slice_filenames():
            self.logger.debug("Cleaning {0}".format(filename))
            os.remove(filename)

    #Database interactions
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
            and table_schema = '{1}'; """.format(self.database_table_name,self.schema.name)
        self.logger.debug(sql)
        cursor = self.schema.execute(sql)

        return [row[0] for row in cursor.fetchall()]

    def get_metadata(self,function):
        return self.schema.get_database_specific_metadata(self.name,function)

    def query_metadata(self,function):
        return self.schema.query_database_specific_metadata(self.name,function)

    def set_metadata(self,function,data):
        return self.schema.set_database_specific_metadata(self.name,function,data)

    def table_exists(self):
        return self.schema.selectone("""SELECT (SELECT count(*) FROM information_schema.tables 
            where table_name = %(table_name)s
            and table_schema = %(schema_name)s) > 0""",
            dict(table_name=self.database_table_name,schema_name=self.schema.name)
        )
    def number_of_rows_in_db(self):
        if self.table_exists():
            return self.schema.selectone('select count(*) from {0}'.format(self.full_name))
        else:
            return 0

    def clean_database(self):
        self.schema.execute('delete from {0}.flock where key = \'{1}\' and function = \'inserted_slice\'; commit;'.format(
            self.schema.name,self.name))
        return self.schema.execute('drop table {0}'.format(self.full_name))

    @operation
    def grant(self):
        for privilege,users in self.schema.settings.DATABASE_PERMISSIONS.iteritems():
            self.schema.execute('grant {privilege} on {full_name} to {users}',**dict(
                users=','.join(users),
                privilege=privilege,
                full_name=self.full_name
            )) 

    @operation
    def hot_insert_file_data(self,infiles,transaction,encoding='utf-8'):
        """
            Take data from staging csv's and insert them to the database
             - Uses a temp table to stage data
             - Guards agains dupes
             - Needs an open transaction
        """

        num_files = len(infiles)
        primary_keys = ','.join(self.primary_keys())

        if primary_keys:
            #Upload to temp table remove duped records and then flush temp table to perm table
            #must do one file at a time do guard against dupes that can sometimes come of the wire from RN
            report = dict(
                method = 'fancy',
                files_inserted = 0,
                rows_updated = 0,
                rows_inserted = 0
            )
            for infile in infiles:

                temp_table_ddl,temp_tablename = self.get_ddl(temporary=True)

                args = infile,temp_tablename
                self.logger.debug("PK found! Hot inserting data from {0} via {1}".format(*args))
                rows_before = self.schema.selectone('select count(*) from {0};'.format(self.full_name))


                self.schema.execute(temp_table_ddl)
                self.logger.debug('hot_insert {0} {1}'.format(encoding,infile))
                csv_import([open(infile,'rb')],temp_tablename,self.schema.db,mapper=self.mapper(),encoding=encoding)
                kwargs = dict(full_name=self.full_name,temp_name=temp_tablename,primary_keys=primary_keys)

                slice_size = self.schema.selectone('select count(*) from {0};'.format(temp_tablename))
                try:
                    savepoint1 = transaction.savepoint()
                    self.logger.debug('Trying dumb insert')
                    self.schema.execute("INSERT into {full_name} \
                        select * from {temp_name}".format(**kwargs))

                except IntegrityError:
                    #Dupes were found somewhere. Take latter value.
                    self.logger.warn('Dumb insert yielded IntegrityError: Trying with lookahead dupe deletion...')
                    transaction.return_to_savepoint(savepoint1)
                    self.schema.execute("delete from {full_name} \
                        where {primary_keys} in \
                        (select {primary_keys} from {temp_name});".format(**kwargs))
                    
                    try:
                        savepoint2 = transaction.savepoint()
                        self.schema.execute("INSERT into {full_name} \
                            select * from {temp_name}".format(**kwargs))

                    except IntegrityError:
                        #Dupes were found inside the same file. Take first value.
                        transaction.return_to_savepoint(savepoint2)
                        self.logger.warn('Lookahead dupe deletion also yielded IntegrityError: Trying rank() insert...')
                        kwargs['fieldnames'] = ','.join(self.fieldnames())
                        self.schema.execute("""INSERT into {full_name} 
                            SELECT {fieldnames} from  (
                              SELECT * ,rank() OVER (PARTITION BY ID order by random()) 
                                FROM {temp_name}
                            ) t where t.rank = 1;""".format(**kwargs))

                        self.logger.warn('rank() insert completed')


                self.schema.execute("drop table {temp_name};".format(**kwargs))

                rows_after = self.schema.selectone('select count(*) from {full_name};'.format(**kwargs))
                args = rows_before,rows_after
                self.logger.debug("There were {0} rows before and {1} rows after the file was processed.".format(*args))

                #Calculate report
                rows_inserted = rows_after - rows_before
                report['files_inserted'] += 1
                report['rows_updated'] += (slice_size - rows_inserted)
                report['rows_inserted'] += rows_inserted


        else:
            data = self.full_name,num_files
            self.logger.debug("No PK found on {0}. Inserting data from {1} files.".format(*data))
            report = dict(
                method = 'simple',
                rows_updated = 0,
                files_inserted = len(infiles),
            )
            infiles = (open(f) for f in infiles)
            csv_import(infiles,self.full_name,self.schema.db,mapper=self.mapper(),encoding=encoding)

        return report








class FlockSchema(object):
    """
        Class to structure interactions with schema definitions and data import workflows.
        i.e. Layer all the file management and some db operations

    """


    def __init__(self,args):

        assert args.schema

        #Check that settings have been configured
        assert self.settings

        # Assign a custom Table class to InjectedTableClass 
        # in your schema to override or create new functionality
        try:
            self.TableClass = self.InjectedTableClass
        except AttributeError:
            self.TableClass = FlockTable

        assert self.TableClass

        self.args = args
        self.name = os.path.split(args.schema)[1]

        #Init the database name early so we can test which environment we are in
        if args.db_name:
            self.db_name = args.db_name
        else:
            assert self.settings.DEFAULT_DATABASE
            self.db_name = self.settings.DEFAULT_DATABASE

        #set up logging
        if self.settings.LOG_TO_EMAIL:
            #mailhost, fromaddr, toaddrs, subject
            smtp_args = [
                self.settings.SMTP_SERVER,
                self.settings.OWNER_EMAIL,
                self.settings.LOG_DIST_LIST,
                "Flock log for {0} in {1}".format(self.name,self.db_name)
            ]
        else:
            smtp_args = None

        log_directory = os.path.join(self.settings.LOG_DIRECTORY,self.db_name)
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)
        log_filename = os.path.join(log_directory,'{0}.log'.format(args.command if args.command else 'schema'))
        self.logger = get_logger('schema.' + self.name,log_filename,smtp_args=smtp_args)
        self.logger.info('Logging online {0}'.format(self.name))

        #establish schema folder

        self.schema_dir = args.schema

        if not os.path.exists(self.schema_dir):
            if raw_input('Schema {0} does not exist. Do you want to create it? [y/N]:'.format(self.schema_dir)) in ('y','Y'):
                os.makedirs(self.schema_dir)
                open(os.path.join(self.schema_dir,'.generated'),'w').write('Code generation in play. Look out and use scm!')
            else:
                exit()
            for d in ['definitions','pipelines','access']:
                os.mkdir(os.path.join(self.schema_dir,d))

        #get list of targeted tables
        # - targeted tables are a reaction to the constant desire to pass a table name 
        #   around with the schema class which should definitely be avoided
        # - there is room for improvement here
        
        if args.tables:
            self.settings.TABLES = args.tables

        #set up file based metadata

        if not self.args.metadatafile:
            self.metadatafile_path = os.path.join(self.schema_dir,'.metadata.json')
        else:
            self.metadatafile_path = os.path.abspath(args.metadatafile)
        if os.path.exists(self.metadatafile_path):
            try:
                data = json.loads(open(self.metadatafile_path).read(),object_hook=OrderedDict)
            except Exception as e:
                print e
                raise Exception('Couldn\'t read metadata in file {0}'.format(self.metadatafile_path))
            self.metadata = defaultdict(dict,data)
        else:
            self.metadata = defaultdict(dict)

        #set up tables
        self.tables = OrderedDict(((table_name,self.TableClass(table_name,self)) for table_name in self.settings.TABLES))

        #set up db connection

        self.transaction_open = False

        db_uri = self.settings.DATABASES[self.db_name]
        self.db = dial(db_uri)

        #set up schema inside database if it isn't already there

        if self.db and not self._schema_exists():
            with self.transaction() as transaction:
                self._init_schema_with_db()


    def export_metadata(self):
        """
            Only call after successful operations
        """
        self.logger.info('Exporting metadata to' + self.metadatafile_path)
        open(self.metadatafile_path,'w').write(json.dumps(self.metadata,indent=2))

    def get_path(self,function,*args):
        """
            Calculate the directory path where files should go. Additional 
            arguments are mapped to subdirectories in the schema directory.
        """
        if not override:
            return os.path.join(self.schema_dir,function,*args)    
        else:
            return override

    def get_mapper(self,md_key):
        "Returns a function that maps unstandardized fieldnames to standardized ones"

        print 'Attempting to find metadata for key:',md_key
        if md_key in self.metadata:
            #A mapping is configured for this key
            fieldmap = self.metadata[md_key]
            assert type(fieldmap) == OrderedDict
            mapper = lambda x: fieldmap[x]
            # self.logger.debug("*Using the following field mappings*")
            # for k,v in fieldmap.iteritems():
            #     self.logger.debug('{0}: {1}'.format(k,v))
        else:
            mapper = lambda x: x
        return mapper

    def set_metadata(self,key,value):
        assert json.dumps(value)
        self.metadata[key] = value

    def log_traceback(self):
        self.logger.error('\n{0}'.format(traceback.format_exc()))

    def uuid(self,slug):
        return slug + str(uuid.uuid4()).replace('-','')


    #Database helpers

    def execute(self,sql,*args,**kwargs):
        """
            Does some string formatting before calling cursor.execute. 
        """
        cursor = self.db.cursor()
        sql = sql.format(*args,schema=self,**kwargs)
        sql_log_msg ='Running sql `{0}` (clipped at 200 chars)'.format(' '.join(sql.split())[:200])
        self.logger.debug(sql_log_msg)
        cursor.execute(sql,*args)
        return cursor
    
    def selectone(self,sql,*args,**kwargs):
        " Convenience method for queries that return one value"
        assert sql.strip().lower().startswith('select')
        cursor = self.db.cursor()
        cursor.execute(sql,*args,**kwargs)
        answer = cursor.fetchone()
        if answer != None:
            answer = answer[0]
        sql_log_msg ='Statement is {0} `{1}` (clipped at 200 chars)'.format(answer,' '.join(cursor.query.split())[:200])
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
        self.execute('grant usage on schema {schema.name} to {users}',**dict(users=','.join(self.settings.DATABASE_USERS)))
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
                self.logger.error('Database transaction rolled back. \n{0}'.format(traceback.format_exc()))
                raise e
        else:
            self.logger.error("Transaction is already open.")
            raise Exception("Transactions cannot be opened twice")

        self.transaction_is_open = False
        self.db.commit()
        self.logger.debug('Committing database transaction')



    @operation
    def set_database_specific_metadata(self,key,function,data):
        sql_template = "insert into {0}.flock (key,function,data) VALUES (%s,%s,%s)".format(self.name)
        self.execute(sql_template,[key,function,CustomJson(data)])

    @operation
    def get_database_specific_metadata(self,key,function):
        sql_template = "select data from {0}.flock where key = %s and function = %s order by id desc".format(self.name)
        return self.selectone(sql_template,[key,function])

    @operation
    def query_database_specific_metadata(self,key,function):
        sql_template = "select data from {0}.flock where key = %s and function = %s order by id desc".format(self.name)
        c = self.execute(sql_template,[key,function])
        data = [row[0] for row in c.fetchall()]
        return data

    # Operations 

    @operation
    def run_pipeline(self,pipeline):
        "Runs the specified pipeline"
        path = os.path.join(self.schema_dir,'pipelines',pipeline)
        if os.path.isdir(path):
            steps = glob.glob(os.path.join(path,"*"))
        else:
            steps = [path]
        for i,step in enumerate(steps):
            step_name = os.path.basename(step)
            self.logger.info("Running pipeline step {0}: {1}".format(i,step_name))
            self.execute(open(step).read())


    # Commands

    @command
    def shell(self):
        "Initialize Schema and drop into an IPython shell"

        self.logger.info("Entering interactive shell")
        schema = self

        #Make a datastructure fot tables that is tab completion friendly
        class O: pass
        tables = O()
        for k,v in self.tables.iteritems():
            setattr(tables,k,v)

        #Make a shell
        import IPython
        IPython.embed()     
        self.logger.info("Exiting interactive shell")

    @command
    def clean_database(self):
        "Cleans everything under the schema in the active database. Use with caution!"

        if raw_input("Are you sure you want to delete everything in schema {0} on database {1}? (N/y) ".format(self.name,self.db_name)) in ('y','Y'):
            self.logger.info("Cleaning schema {0} on database {1}".format(self.name,self.db_name))
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

    #Internal

    def _get_annotated_methods(self,annotation):
        "Returns references to operations/commands/tests that have the corresponding decorator attached"
        
        for attr_name in dir(self.__class__):
            attr = getattr(self.__class__,attr_name)
            if inspect.ismethod(attr):
                if annotation in attr.im_func.__dict__:
                    yield attr

#Dont call it a factory!
@contextlib.contextmanager
def get_schema(args,schema_class=FlockSchema):
    "Context manager to set up and tear down Schemas"
    schema = schema_class(args)
    yield schema
    schema.export_metadata()



class Transaction:
    "For managing savepoints inside Postgres transactions"

    def __init__(self,schema):
        self.schema = schema

    def savepoint(self):
        "Sets a savepoint"
        id = self.schema.uuid('sp')
        self.schema.execute('SAVEPOINT {0};'.format(id))
        self.schema.logger.debug('Setting database savepoint {0}'.format(id))
        return id

    def return_to_savepoint(self,id):
        "Returns to the specified savepoint"
        self.schema.logger.warn('Returning to database savepoint {0}'.format(id))
        self.schema.execute('ROLLBACK TO SAVEPOINT {0};'.format(id))

