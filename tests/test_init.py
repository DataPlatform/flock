import unittest
import sys
sys.path.append('../')
from flock.schema import Schema
from flock.table import Table
from flock.global_metadata.file import Metadata
from flock.db.postgres import Pipeline
from flock.db.postgres import Driver as PGDriver
from flock.annotate import flock,command, operation
import settings


class MySchema(Schema):

    @operation
    def hydrate_schema(self):
        pass


class BadSchema(Schema):
    """Implements an operation belonging to the Metadata component"""
    @operation
    def get_metadata(self):
        pass


@flock
class PGFlockApp(MySchema, Pipeline, PGDriver, Metadata):

    settings = settings

    @command
    def test(self):
        return True



@flock
class OpConflictApp(BadSchema, Metadata):

    settings = settings



# class AlchemyApp(MySchema, Pipeline, AlchemyDriver):

#     settings = settings


class TestInitializationWithPostgres(unittest.TestCase):

    def setUp(self):
        sys.argv = ['init.py', 'test']
        self.app = PGFlockApp()

    def test_init(self):
        """ Test we can initialize an app that does nothing at all"""

        self.assertEqual(self.app.name, 'tests')

    def test_command(self):
        """ Test we can initialize an app and run a @command"""

        app = PGFlockApp()
        app.enter()


class TestOperationCollisionChecking(unittest.TestCase):
    """Checks operation collision detection"""

    def test_op_conflict_checking(self):
        "not implemented yet"
        self.assertRaises(Exception,PGFlockApp())
        
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
