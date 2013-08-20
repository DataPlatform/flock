"""
        Example schema for the flock framework.
"""

import os,os.path,shutil,json
from flock.schema import FlockSchema,FlockTable,command,operation,test
from datetime import datetime,date,timedelta
from psycopg2 import IntegrityError,ProgrammingError,DataError
from bin.csv_to_ddl import csv_to_ddl
from fabric.api import lcd,local
#Schema specific
from fdps import get_fdps_slice



class Schema(FlockSchema):
    """
        A module to curate FDPS data exports
    """
    #Required (Importing here leaves a reference to the module as an attribute)
    import settings
    @operation
    def download_new_data(self,procurements):

        last_update = procurements.get_metadata('api_download_upper_bound')

        if last_update == None:
            last_update = beginning = date(2010, 1, 1)
        else:
            beginning = last_update + timedelta(days=1)

        yesterday = datetime.now().date()-timedelta(days=1)

        if beginning < yesterday:

            self.logger.info("Last update was {0}, attempting to update slice filedata".format(last_update.isoformat()))
            days = (yesterday - beginning).days
            slice = get_fdps_slice(beginning,days)
            slice_name = beginning.isoformat()
            slice_name = procurements.write_slice_to_file(slice,slice_name=slice_name)

            procurements.set_metadata('api_download_upper_bound',yesterday)

        else:
            self.logger.info("Last update was {0}, refusing to update slice filedata".format(last_update.isoformat()))

    @command
    def bootstrap(self):
        "Go from empty to fully up to date states"

        procurements = self.tables['procurements']

        #Get some sample data (sample size = all the data to date, as it is quite small)
        self.download_new_data(procurements)

        #Generate a create table statement based on sample data
        infiles = (open(f,'rb') for f in procurements.get_slice_filenames())
        ddl,fieldmap = csv_to_ddl(infiles,procurements.full_name)
        procurements.set_ddl(ddl,fieldmap=fieldmap)

        with self.transaction() as transaction:

            #Execute the create table statement
            procurements.apply_ddl()

            #Load initial data as well
            procurements.hot_insert_file_data(procurements.get_slice_filenames(),transaction)



            
    @command
    def update(self,max_days=None):
        "Update the data in the database"

        procurements = self.tables['procurements']
        seen_slices = procurements.get_slice_filenames()
        self.download_new_data(procurements)
        new_slices = [s for s in procurements.get_slice_filenames() if s not in seen_slices]

        self.logger.info('Found the following slices' + str(new_slices))
        with self.transaction() as transaction:
            procurements.hot_insert_file_data(new_slices,transaction)


    @command
    def is_healthy(self):
        "Determine if the schema in a healthy state"

        table_is_nonzero = self.selectone("select (SELECT count(*) FROM fdps.procurements) > 0;")
        table_has_recent_data = self.selectone("select (SELECT count(*) FROM fdps.procurements where transactionInformation_lastModifiedDate >= current_date - 7) > 0;")
        return table_is_nonzero and table_has_recent_data

    @command
    def clean(self):
        if os.path.exists('.cache'):
            for filename in os.listdir('.cache'):
                os.remove(os.path.join('.cache',filename))
        super(Schema,self).clean()


    @test
    def bootstrap_test(self):
        "Test that the schema is working"
        self.clean()
        self.bootstrap()
        return self.is_healthy()

    @test
    def update_test(self):
        "Test that the schema is working"
        self.update()
        return self.is_healthy()
