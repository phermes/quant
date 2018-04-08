# Authors : Pascal Hermes - Tom Mertens
# Version : 1.0
# Last modified : 02-04-2018

# performs basic tests on the QAnt code

from unittest import TestCase
import os

# catch error when relative import is not working
# ref : https://stackoverflow.com/questions/33837717/systemerror-parent-module-not-loaded-cannot-perform-relative-import
try:
    from .context import perform_initialization_checks
except SystemError:
    from QAntTom import perform_initialization_checks

try:
    from .context import logging
except SystemError:
    from QAntTom.output import logging


class InitTest(TestCase):
    def setUp(self):
        """
        Setting up the test environment
        """
        pass

    def tearDown(self):
        """
        code is performed after tests are run - clean up
        """
        os.removedirs("output/")

    def test___perform_init_checks___expect_pass(self):
        """
        test if output directory check is working
        """
        perform_initialization_checks()
        self.assertTrue(os.path.isdir("output/"))

# TODO : first need to have a financial instrument defined
# class LoggingTest(TestCase):
#     """
#     Tests the Logging class
#     """
#     def setUp(self):
#         # create class
#         logclass = logging()
#
#     def tearDown(self):
#         pass
#
#     def test___write_error_msg_to_errorfile___expect_pass(self):
#         message = 'error test'
