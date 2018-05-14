#!/usr/bin/env python

"""test_configreader.py: docstring"""

__author__ = 'thomas'
__creation_date__ = '14.05.18'

import pytest

import python_general.library.configreader


def test_get_config():
    cfg = python_general.library.configreader.ConfigReader(config_file='test_config.yml')
    assert 'Test' in cfg.config.get('configreader').keys()