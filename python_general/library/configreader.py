#!/usr/bin/env python

"""configreader.py: docstring"""

__author__ = 'thomas'
__creation_date__ = '09.03.18'


import yaml

import python_general.library.baseobject


class ConfigReader(python_general.library.baseobject.BaseObject):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if kwargs.get("config_file", None):
            config = open(kwargs.get("config_file"), "r")
            self.config = yaml.load(config)
            self.log.debug(self.config)