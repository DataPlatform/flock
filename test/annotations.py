import unittest
import sys
import logging
sys.path.append('../')
from flock.annotate import flock, operation

# Need a global to accumulate as we cannot retun data from __init__

# Logging
logger = logging.getLogger('flock.tests')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


class MixMaster(object):

    def __init__(self):
        self.accumulate.append('MixMaster')


class Mix1(object):

    def __init__(self):
        self.accumulate.append('Mix1')

    @operation
    def a(self):
        return 'Mix1'


class Mix2(object):

    def __init__(self):
        self.accumulate.append('Mix2')

    @operation
    def a(self):
        return 'Mix2'


@flock
class TestApp(
    MixMaster,
    Mix1,
        Mix2):

    logger = logger

    def __init__(self):
        self.accumulate = list()


class TestInitializationWithComponents(unittest.TestCase):

    def setUp(self):
        self.app = TestApp()

    def test_init(self):
        """ Test we can initialize an app that does nothing at all"""
        self.assertEqual(self.app.accumulate, ['MixMaster', 'Mix1', 'Mix2'])

    def test_collision(self):
        self.assertEqual(self.app.a(), 'Mix1')


if __name__ == '__main__':
    unittest.main()
