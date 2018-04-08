# Authors : Pascal Hermes - Tom Mertens
# Version : 1.0
# Last modified : 02-04-2018
#
# The purpose of this file is to allow the unit tests to be run at installation of the package without having to
# worry about where the package will be installed. The file is based on the example from the ref.
#
# Ref. : http://docs.python-guide.org/en/latest/writing/structure/

import os
import sys


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from QAntTom import perform_initialization_checks
from QAntTom.output import logging
