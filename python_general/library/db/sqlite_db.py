"""
Module to provide basic interface functionality to a sqlite3 database
"""
__author__ = 'thomas'


#[]{}

import sqlite3
import os

import python_general.library.baseobject


class DatabaseConnection(python_general.library.baseobject.BaseObject):
    """
    Class that provides the database connectivity. On init, a connection to a given
    database is established. Database is selected by giving its path and filename
    """

    def __init__(self, db_path='', db_name='', *args, **kwargs):
        super().__init__(*args, **kwargs)
        if db_path:
            database_descriptor = os.path.join(db_path, db_name)
        elif db_name:
            database_descriptor = db_name
        else:
            database_descriptor = ':memory:'
        self.conn = sqlite3.connect(database_descriptor)
        self.cursor = self.conn.cursor()
        self.log.info('Setup of Database connection: {}'.format(database_descriptor))

    def create_table(self, table_name, columns, col_types, primary_key=None):
        """
        Creates the specified table. Verifies the existence of the table.
        :param table_name: Name of the table that should be created
        :param columns: list of column names (can be a string if only one column)
        :param col_types: types of the given columns, same order (can be a string if only one column)
        :return: True if existence could be verified, False otherwise
        """
        if isinstance(columns, str) and isinstance(col_types, str):
            columns = [columns]
            col_types = [col_types]
        assert len(columns) == len(col_types)
        entries = []
        for i, colname in enumerate(columns):
            entries.append(u'{} {}'.format(colname, col_types[i]))
        if primary_key:
            if isinstance(primary_key, str):
                primary_key = [primary_key]
            pk_sql = u''', PRIMARY KEY ({})'''.format(', '.join(primary_key))
        else:
            pk_sql = u''
        sql = u'''create table {} (
        {}{} )'''.format(table_name, ', '.join(entries), pk_sql)
        self.log.debug(sql)
        self.cursor.execute(sql)
        if self.check_table_exists(table_name):
            return True
        else:
            self.log.warning('Error creating and varifying table {}'.format(table_name))
            return False

    def check_table_exists(self, table_name):
        """
        Checks if given table exists
        :param table_name: 
        :return: True if table exists, false otherwise
        """
        sql = u'''select count(1) from {} where 1=2'''.format(table_name)
        try:
            self.log.debug('Checking for table {}'.format(table_name))
            self.cursor.execute(sql).fetchone()
            return True
        except sqlite3.OperationalError:
            self.log.debug('Table {} not there'.format(table_name))
            return False

    def get_columns(self, table_name):
        """
        Returns the names of the columns of the given table
        :param table_name: 
        :return: tuple of column names (type strings)
        """
        if not self.check_table_exists(table_name):
            raise sqlite3.InterfaceError
        self.log.debug('Getting column names for table {}'.format(table_name))
        sql = u''' select * from {table_name} where 1=2'''.format(table_name=table_name)
        cols = self.cursor.execute(sql).description
        return tuple([col[0] for col in cols])

    def insert_single_row(self, table, entries, columns, commit=True):
        """
        Inserts a single give row into the table
        :param table: The table it should be put in
        :param entries: sequence of entries
        :param columns: sequence of columns for the entries (has to have same ordering as entries)
        :param commit: True if row should be commited (default)
        :return: True it insert succeeded
        """
        if not self.check_table_exists(table):
            raise sqlite3.InterfaceError
        entries = [str(i) if i is not None else 'null' for i in entries]
        tmp_entries = ''
        for entry in entries:
            if entry.lower() != 'null':
                tmp_entries = '''{},"{}"'''.format(tmp_entries, entry)
            else:
                tmp_entries = '''{},{}'''.format(tmp_entries, entry)
        entries = tmp_entries[1:]
        cols = ', '.join(columns)
        sql = u''' insert into {tname} ({columns}) values ({entries})'''.format(tname=table, columns=cols,
                                                                                entries=entries)
        self.log.debug('Inserting values into table {}'.format(table))
        return self.execute_sql(sql, commit)

    def execute_sql(self, sql, commit=True):
        """
        Execute given sql on database
        :param sql: 
        :param commit: True if commit should be done (default)
        :return: True if succeeded, otherwise none
        """
        self.log.debug('Executing query: {}'.format(sql))
        self.cursor.execute(sql)
        if commit:
            self.conn.commit()
        return True

    def add_column_to_table(self, table, column_name, column_type, commit=True):
        """
        Modify a table by adding a column
        :param table: 
        :param column_name: 
        :param column_type: sqlite3 understandable type
        :param commit: True if commit should be done (default)
        :return: True if succeeded
        """
        sql = u'''alter table {} add column {} {}'''.format(table, column_name, column_type)
        if not self.check_table_exists(table):
            raise sqlite3.InterfaceError
        self.log.debug('Adding column {} to table {}'.format(column_name, table))
        return self.execute_sql(sql, commit)

    def update_value(self, table, updated_column, value, cols_hit_condition, hit_con_values, commit=True):
        """
        Update values in a table based on hit condition1 = hit value1 and hit condition2 = hit value2 and ...
        TODO: add check for hit_cond and hit_values are sequences
        :param table: 
        :param updated_column: column that should be updated
        :param value: new value
        :param cols_hit_condition: sequence of columns that should meet its values
        :param hit_con_values: sequence of to be hit values
        :param commit: 
        :return: True if succeeded
        """
        assert len(cols_hit_condition) == len(hit_con_values)
        if cols_hit_condition:
            comp_fields = []
            for i, col_hit_cond in enumerate(cols_hit_condition):
                comp_fields.append(u' {} = {} '.format(col_hit_cond, hit_con_values[i]))
            comp_str = u'and {}'.format(u'and'.join(comp_fields))
        else:
            comp_str = u''
        sql = u''' update {} set {} = {} where 1=1 {}'''.format(table, updated_column, value, comp_str)
        self.execute_sql(sql, commit)

    def get_data(self, sql):
        """
        Return the data on the database for a given sql query
        :param sql:
        :return: return of the query, probably list of lists / None in case of error
        """
        try:
            self.log.debug('Executing query: {}'.format(sql))
            self.cursor.execute(sql)
            data = self.cursor.fetchall()
            return data
        except sqlite3.OperationalError:
            self.log.error('Error executing sql {}'.format(sql))
            return None


if __name__ == '__main__':
    DB = DatabaseConnection(loglevel='DEBUG')
    DB.create_table('users_login_test', ['name', 'pwd'], ['VARCHAR', 'VARCHAR'])
    print(DB.get_columns('users_login_test'))
    print(DB.insert_single_row('users_login_test', ['User4', 'user4'], ['name', 'pwd']))
    print(DB.get_data('select * from users_login_test'))
