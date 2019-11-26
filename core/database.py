# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 14:41:36 2019

@author: WXS
"""
import os
import shutil
from Table import Table

class Database:
    def __init__(self, name):
        self.tables = []
        self.name = name
        self.create_dir(self.name)
        
    def add_table(self, info):
        table = Table(info)
        self.tables.append(table)

    def create_dir(self, name):
        database = name.replace(';', '').lower()
        _database = './ZibiDB/database/'

        # If the database directory is exist, pass
        if not os.path.exists(_database):
            os.makedirs(_database)

        # If the database is exist, ERROR
        if os.path.exists(_database + database):
            raise Exception('ERROR: The database is exist already.')

        # If the database is not exist, create and PASS
        elif not os.path.exists(_database + database):
            os.makedirs(_database + database)
            print('PASS: The database is created.')
            return

        else:
            raise Exception('ERROR: Invalid command.')

    def drop_dir(self, name):
        database = name.replace(';', '').lower()
        _database = './ZibiDB/database/'

        # If the database directory is exist, pass
        if not os.path.exists(_database):
            raise Exception('ERROR: No any databases')

        # If the database is exist, drop and PASS
        if os.path.exists(_database + database):
            shutil.rmtree(_database + database)
            print('PASS: The database is dropped.')
            return

        # If the database is not exist, ERROR
        elif not os.path.exists(_database + database):
            raise Exception('ERROR: The database is not exist.')

        else:
            raise Exception('ERROR: Invalid command.')
