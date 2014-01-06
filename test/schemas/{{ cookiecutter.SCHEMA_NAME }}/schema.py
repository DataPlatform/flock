"""
        Template schema for the flock framework.
"""

import os,os.path,shutil,json
from flock.schema import FlockSchema,FlockTable,command,operation,test
from datetime import datetime,date,timedelta
from psycopg2 import IntegrityError,ProgrammingError,DataError
#Schema specific


class Table(FlockTable):

    @operation
    def download_new_data(self,procurements):
        """
            An example to suggest workflow. Not required!
             - Perhaps pull in your data in two a 2d list then use
               FlockTable.write_slice_to_file() to store it in csv?
             - Feel free to change everything though!
        """
        pass

class Schema(FlockSchema):
    """
        My new schema. 
         - Some sample methods prepopulated to give an idea of structure.
         - Feel free to change everything: There are no rules!
         - 
    """
    #Required (Importing here leaves a reference to the module as an attribute)
    from schemas.fdps import settings

    #Assigne like this if you want to create custom table logic above
    InjectedTableClass = Table

    #Some control flow is defined here. 
    #Use the @command decorator to declare an entry point to the CLI. 
    # - Check for default commands on FlockSchema

    @command
    def bootstrap(self):
        self._up(True)

            
    @command
    def update(self,max_days=None):
        self._up(False)

    @command
    def is_healthy(self):
        "A stateless health check"
        return None


    #Use the @test decorator to register a test with the CLI. (Tests return True/False)

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

    #Private helper method

    def _up(bootstrapping):
        for table in self.tables:
            with self.transaction() as transaction:
                table.download_new_data()
                if bootstrapping:
                    table.clean_database()
                    table.generate_ddl_from_slice_data()
                    table.apply_ddl()
                table.apply_slices_to_database(transaction)
                table.set_metadata('updated',datetime.now())
