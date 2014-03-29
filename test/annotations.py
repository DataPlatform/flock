import unittest
import sys
sys.path.append('../')
from flock.annotate import flock

# Need a global to accumulate as we cannot retun data from __init__
accumulate = []
global accumulate


class MixMaster(object):
    def __init__(self):
        accumulate.append('MixMaster')


class Mix1(object):
    def __init__(self):
        accumulate.append('Mix1')


class Mix2(object):
    def __init__(self):
        accumulate.append('Mix2')

@flock
class TestApp(
    MixMaster,
    Mix1,
    Mix2):
    pass

class TestInitializationWithComponents(unittest.TestCase):

    def test_init(self):
        """ Test we can initialize an app that does nothing at all"""
        
        app = TestApp()
        print accumulate

        self.assertEqual(accumulate,['MixMaster','Mix1','Mix2'])


if __name__ == '__main__':
    unittest.main()