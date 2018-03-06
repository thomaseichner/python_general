#!/usr/bin/env python

"""baseobject.py: docstring"""

__author__ = 'thomas'
__creation_date__ = '06.03.18'

import logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class BaseObject:
    def __init__(self, *args, **kwargs):
        self.log = logging.getLogger(self.__class__.__name__)
        super().__init__()
        self.log.setLevel(kwargs.get("loglevel", "WARN"))


if __name__ == '__main__':
    log = BaseObject(loglevel='DEBUG')
    log.log.warning('WARN')
    log.log.error("ERROR")
    log.log.info("INFO")
    log.log.debug("DEBUG")
