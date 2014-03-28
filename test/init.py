import unittest
import sys
sys.path.append('../')
from flock.schema import Schema
from flock.table import Table
from flock.db.postgres import Pipeline
from flock.db.postgres import Driver as PGDriver
import settings




class MySchema(Schema, Driver):

    #this is necessary
    settings = settings

    def custom_init(self):

    	pass


    def hydrate_schema(self):
        pass

class PGApp(MySchema, Pipeline, PGDriver):

	pass

class AlchemyApp(MySchema, Pipeline, AlchemyDriver):

	pass


class TestInitializationWithPostgres(unittest.TestCase):


    def test_init(self):
        """ Test we can initialize an app that does nothing at all"""
        
        app = App()
        self.assertEqual(app.name,'test')

    def test_command(self):
        """ Test we can initialize an app that does nothing at all"""
        
        app = App()
        app.enter()

class TestInitializationWithAlchemy(unittest.TestCase):


    def test_init(self):
        """ Test we can initialize an app that does nothing at all"""
        
        app = App()
        self.assertEqual(app.name,'test')

    def test_command(self):
        """ Test we can initialize an app that does nothing at all"""
        
        app = App()
        app.enter()


if __name__ == '__main__':
    unittest.main()