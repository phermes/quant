# Authors : Pascal Hermes - Tom Mertens
# Version : 1.0
# Last modified : 08-04-2018

# takes care of all the files and directory creation and deletion

import os

import logging

from QAntTom import databasedir


# create a logger for this module
log = logging.getLogger('quantlog')

def find_or_create_output_dirs():
    """
    Checks if the necessary output directories exist - if not they are created
    """
    log.debug('Checking for output directory...')
    for directory in [databasedir + "output/"]:
        if not os.path.isdir(directory):
            log.warning('Directory does not exist - creating ...')
            os.makedirs(directory)
        else:
            log.debug('Directory {} found...'.format(directory))

# def create_directory(dir):


