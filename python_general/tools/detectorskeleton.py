#!/usr/bin/env python

"""detectorskeleton.py: docstring"""

__author__ = 'thomas'
__creation_date__ = '22.05.18'

import time
import abc

import python_general.tools.singleton

import sensor_reader
import tools.data_logger


class DetectorSkeleton(python_general.tools.singleton.Singleton):
    __metaclass__ = abc.ABCMeta

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log.info('Starting controlflow')
        self.config = self.config.get(kwargs.get('config_toplevel')).get(self.__class__.__name__.lower())
        self.start_control(*args, **kwargs)

    @abc.abstractmethod
    def startup(self, *args, **kwargs):
        return

    @property
    def read_arguments(self):
        return self.config.get('read_arguments')

    def run_once(self):
        self.log.debug('Starting single cycle')
        values = self.sr.read_value(self.read_arguments)
        self.dl.write_values(self.read_arguments, values)
        return values

    def run_cycles(self, max_cycles=float('inf')):
        current_cycle = 0
        try:
            while current_cycle < max_cycles:
                self.log.info('Starting cycle {} of {}'.format(current_cycle, max_cycles))
                self.update_status_file()
                ret = self.run_once()
                self.log.info('Asked: {}, Sensor return: {}'.format(self.read_arguments, ret))
                current_cycle += 1
                time.sleep(self.config.get('readout_interval'))
        finally:
            self.delete_status_file()
