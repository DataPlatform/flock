import unittest
import sys
sys.path.append('../')
from flock.schema import Schema
from flock.table import Table
from flock.global_metadata.file import Metadata
from flock.db.postgres import Pipeline
from flock.db.postgres import Driver as PGDriver
import settings


class MySchema(Schema, PGDriver):

    #this is necessary

    def custom_init(self):

        pass

    def hydrate_schema(self):
        pass


class PGApp(MySchema, Pipeline, PGDriver, Metadata):

    settings = settings


# class AlchemyApp(MySchema, Pipeline, AlchemyDriver):

#     settings = settings


class TestInitializationWithPostgres(unittest.TestCase):

    def setUp(self):
        sys.argv = ['init.py', 'test']
        self.app = PGApp()

    def test_init(self):
        """ Test we can initialize an app that does nothing at all"""

        self.assertEqual(self.app.name, 'tests')

    def test_command(self):
        """ Test we can initialize an app that does nothing at all"""

        app = PGApp()
        app.enter()

# class TestInitializationWithAlchemy(unittest.TestCase):


#     def test_init(self):
#         """ Test we can initialize an app that does nothing at all"""

#         app = App()
#         self.assertEqual(app.name,'test')

#     def test_command(self):
#         """ Test we can initialize an app that does nothing at all"""

#         app = App()
#         app.enter()


if __name__ == '__main__':
    unittest.main()
