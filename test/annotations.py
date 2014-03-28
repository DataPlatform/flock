import unittest
import sys
sys.path.append('../')
from flock.annotate import component

# Need a global to test the initialization is as expected since we cannot retun data from __init__
accumulate = [None,None,None]
global accumulate


@component
class MixMaster(object):
    def __init__(self):
        accumulate.append('MixMaster')


@component
class Mix1(object):
    def __init__(self):
        accumulate.append('Mix1')


@component
class Mix2(object):
    def __init__(self):
        accumulate.append('Mix2')

class TestApp(
    Mix1,
    Mix2,
    MixMaster):
    pass

class TestInitializationWithComponents(unittest.TestCase):


    def test_init(self):
        """ Test we can initialize an app that does nothing at all"""
        
        app = TestApp()
        print accumulate

        self.assertEqual(accumulate,['MixMaster','Mix1','Mix2'])


if __name__ == '__main__':
    unittest.main()