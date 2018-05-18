#!/usr/bin/env python

"""singleton.py: docstring"""

__author__ = 'thomas'
__creation_date__ = '18.05.18'


import os
import datetime
import sys

import python_general.library.configreader


class Singleton(python_general.library.configreader.ConfigReader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log.info('Starting singleton controlflow')

    def start_control(self, *args, **kwargs):
        if self.check_for_running():
            self.log.info('Program is already running, exiting attempt')
            sys.exit()
        else:
            self.log.info('Starting programm')
            self.startup(*args, **kwargs)

    def check_for_running(self):
        files = self.get_status_files()
        filedates = [i.split('_')[1] for i in files]
        filedates = [datetime.datetime.strptime(i, self.config.get('time_format')) for i in filedates]
        file_found = False
        for filedate in filedates:
            if (datetime.datetime.now() - filedate) <= datetime.timedelta(seconds=self.config.get('restart_delay')):
                file_found = True
        return file_found

    def get_status_files(self):
        files = [i for i in os.listdir(self.tmp_path) if i.startswith(self.config.get('running_identifier'))]
        return files

    @property
    def tmp_path(self):
        return self.config.get('tmpfolder', '.')

    def update_status_file(self):
        self.delete_status_file()
        open(os.path.join(self.tmp_path, '{}_{}'.format(self.config.get('running_identifier'),
                                                        datetime.datetime.now().strftime(self.config.get('time_format')))), 'w').close()

    def delete_status_file(self):
        existing_files = self.get_status_files()
        self.log.debug('Found existing files: {}'.format(existing_files))
        for file in existing_files:
            os.remove(os.path.join(self.tmp_path, file))