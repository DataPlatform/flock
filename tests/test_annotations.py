import unittest
import sys
import logging
sys.path.append('../')
from flock.annotate import flock, operation
from flock.exceptions import *
import logging
# Need a global to accumulate as we cannot return data from __init__

# Logging
logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(ch)


class MixMaster(object):

    def __init__(self):
        self.accumulate = list()
        self.accumulate.append('MixMaster')


class Mix1(object):

    def __init__(self):
        self.accumulate.append('Mix1')

    @operation
    def a(self):
        return 'Mix1'


class Mix1_Conflict(object):

    def __init__(self):
        self.accumulate.append('Mix1_Conflict')

    @operation
    def a(self):
        return 'Mix1_Conflict'

class Mix2(object):

    def __init__(self):
        self.accumulate.append('Mix2')

    @operation
    def b(self):
        return 'Mix2'

@flock
class TestApp(
    MixMaster,
    Mix1,
    Mix2):

    logger = logger


@flock
class TestAppWithInit(
    MixMaster,
    Mix1,
    Mix2):

    logger = logger

    def __init__(self):
        pass

@flock
class TestConflict(
    MixMaster,
    Mix1,
    Mix1_Conflict):

    logger = logger



class TestInitializationWithComponents(unittest.TestCase):

    def setUp(self):
        self.app = TestApp()

    def test_init(self):
        """ Test all __init__ methos are called on flock components """
        self.assertEqual(self.app.accumulate, ['MixMaster', 'Mix1', 'Mix2'])

class TestInitializationWithComponentsAndInit(unittest.TestCase):

    def setUp(self):
        self.app = TestAppWithInit()

    def test_init(self):
        """ Test all __init__ methos are called on flock components """
        self.assertEqual(self.app.accumulate, ['MixMaster', 'Mix1', 'Mix2'])

class TestInitializationWithConflict(unittest.TestCase):

    def test_conflict(self):
        """ Test conflicts between flock component opeartions thows an InterfaceConflict """
        self.assertRaises(InterfaceConflict,TestConflict)



if __name__ == '__main__':
    unittest.main()
