#!/usr/bin/env python

"""data_logger.py: docstring"""

__author__ = 'thomas'
__creation_date__ = '14.05.18'


import datetime

import python_general.library.configreader
import python_general.library.db.sqlite_db


class DataLogger(python_general.library.configreader.ConfigReader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def ensure_table(self):
        if not self.db.check_table_exists(self.config.get('table_name')):
            self.log.warning('Table {} not existing, creating it with columns: {}'.format(self.config.get('table_name'),
                                                                                          self.config.get('columns')))
            self.db.create_table(self.config.get('table_name'), self.config.get('columns'),
                                 self.config.get('column_types'))

    def write_values(self, read_args, sensor_readings):
        values = [datetime.datetime.now()] + list(read_args) + list(sensor_readings)
        self.log.debug('Writing entry: {}'.format(values))
        self.db.insert_single_row(self.config.get('table_name'), values, self.config.get('columns'))

