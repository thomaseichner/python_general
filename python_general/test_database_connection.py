__author__ = 'thomas'

from sqlite3 import InterfaceError, OperationalError, IntegrityError
import datetime

from unittest import TestCase
import pytest

from Library.databaseconnection import DatabaseConnection


TEST_TABLE_NAME = 'UserLoginTest'
TEST_TABLE_NAME2 = 'TestEmptyTable'


class TestDatabaseConnection(TestCase):

    def setUp(self):
        self.db = DatabaseConnection(db_path=u'', db_name=u':memory:')
        self.db.create_table(TEST_TABLE_NAME, ['id', 'name', 'pwd'], ['integer', 'text', 'text'])
        self.db.insert_single_row(TEST_TABLE_NAME, ['User4', 'user4'], ['name', 'pwd'])
        self.db.get_data(u'''select name, pwd from {}'''.format(TEST_TABLE_NAME)) == [('User4', 'user4')]

    def tearDown(self):
        del self.db

    def test_create_table_string(self):
        assert self.db.create_table('TestTable', 'ID', 'integer')
        with pytest.raises(OperationalError):
            assert self.db.create_table('TestTable', 'ID', 'integer')

    def test_create_table_list(self):
        assert self.db.create_table('TestTable', ['ID'], ['integer'])

    def test_create_table_multiple_columns(self):
        assert self.db.create_table('TestTable', ['ID', 'name'], ['integer', 'text'])

    def test_create_table_datatypes(self):
        assert self.db.create_table('TestTable', ['decimal', 'integer', 'string', 'blob'],
                                    ['real', 'integer', 'text', 'blob'])

    def test_create_table_primary_key(self):
        assert self.db.create_table('TestTable', ['ID'], ['integer'], ['ID'])
        assert self.db.insert_single_row('TestTable', [1], ['ID'])
        assert self.db.insert_single_row('TestTable', [2], ['ID'])
        with pytest.raises(IntegrityError):
            self.db.insert_single_row('TestTable', [1], ['ID'])

    def test_check_table_exists(self):
        assert self.db.check_table_exists(TEST_TABLE_NAME)
        assert not self.db.check_table_exists('TestTable')

    def test_get_columns(self):
        col = self.db.get_columns(TEST_TABLE_NAME)
        assert col == ('id', 'name', 'pwd')
        with pytest.raises(InterfaceError):
            self.db.get_columns('Dummy_non_existing_table')

    def test_insert_single_row(self):
        assert self.db.insert_single_row(TEST_TABLE_NAME, ['User5', 'user5'], ['name', 'pwd'])
        assert self.db.get_data(u'''select name, pwd from {} where name = 'User5' '''.format(TEST_TABLE_NAME)) == \
            [('User5', 'user5')]
        self.db.execute_sql(u'''delete from {} where name = 'User5' '''.format(TEST_TABLE_NAME))
        with pytest.raises(OperationalError):
            assert self.db.insert_single_row(TEST_TABLE_NAME, ['User5', 'user5'], ['name', 'dummy_col'])

    def test_insert_single_row_non_string(self):
        assert self.db.insert_single_row(TEST_TABLE_NAME, [1], ['id'])
        assert self.db.insert_single_row(TEST_TABLE_NAME, [datetime.datetime(2014, 1, 1)], ['name'])

    def test_insert_single_row_None_values(self):
        assert self.db.insert_single_row(TEST_TABLE_NAME, [-25, None], ['id', 'name'])
        assert self.db.get_data('select * from {} where id = -25'.format(TEST_TABLE_NAME)) == [(-25, None, None)]

    def test_add_column_to_table(self):
        self.db.add_column_to_table(TEST_TABLE_NAME, 'active', 'integer')
        col = self.db.get_columns(TEST_TABLE_NAME)
        assert 'active' in col

    def test_update_value(self):
        self.db.update_value(TEST_TABLE_NAME, 'pwd', "'user4new'", ['name'], ["'User4'"])
        assert self.db.get_data(u'''select name, pwd from {} where name = 'User4' '''.format(TEST_TABLE_NAME)) == \
            [('User4', 'user4new')]

    def test_get_data(self):
        ret = self.db.get_data(u'''select name, pwd from {}'''.format(TEST_TABLE_NAME))
        assert ret == [('User4', 'user4')]
        self.db.insert_single_row(TEST_TABLE_NAME, ['User5', 'user5'], ['name', 'pwd'])
        ret = self.db.get_data(u'''select name, pwd from {}'''.format(TEST_TABLE_NAME))
        assert ret == [('User4', 'user4'), ('User5', 'user5')]
        assert not self.db.get_data(u'''dummysql''')
