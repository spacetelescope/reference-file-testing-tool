"""
Logging setup etc.
"""
from __future__ import absolute_import, division, print_function

import logging
REFTEST_ROOT_LOGGER = 'reftest'
DEFAULT_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
import logging

# create logger
logger = logging.getLogger(REFTEST_ROOT_LOGGER)
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)
