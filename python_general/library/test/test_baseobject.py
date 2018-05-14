#!/usr/bin/env python

"""test_baseobject.py: docstring"""

__author__ = 'thomas'
__creation_date__ = '14.05.18'


import pytest

import python_general.library.baseobject


@pytest.mark.parametrize('loglevel',
                         ['DEBUG',
                          'INFO'])
def test_logger_init(loglevel):
    bs = python_general.library.baseobject.BaseObject(loglevel=loglevel)
    bs.log.debug('DEBUG')
    bs.log.error('ERROR')
