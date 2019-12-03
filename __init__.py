import os
import shutil
import csv
import json
import pandas
import sys
import pickle
from ZibiDB.parser import parse
from ZibiDB.core.database import Database
from ZibiDB.core.table import Table

# All attributes, table names, and database names will be stored in lower case

# database engine
class Engine:

    # action functions
    # CREATE DATABASE test;
    def createDatabase(self, name):
        db = Database(name)
        print ('PASS: Database %s is created. Do remember to save it.' %name)
        return db

    # save database test;
    def saveDatabase(self, db):
        db.save()

    # DROP DATABASE test;
    def dropDatabase(self, db):
        db.drop_database()

    # use database test;
    def useDatabase(self, name):
        _database = sys.argv[0]      
        _database = _database[:-11] + 'database/'

        if not os.path.exists(_database + name):
            raise Exception('ERROR: Database %s doesnt exist.' % name)

        elif os.path.exists(_database + name):
            file = open(_database + name, "rb")
            db = pickle.load(file)
            file.close()
            print('You are now using Database: %s !' % name)

        else:
            raise Exception('ERROR: Invalid command.')
        return db

    # show databases;
    def show_database(self):
        t = sys.argv[0]      
        t = t[:-11] + 'database/'
        dirs = os.listdir(t)
        for dir in dirs:
            if '.' not in dir:
                print(dir)

    # show tables;
    def show_table(self, db):
        db.display()

    """
    CREATE TABLE table_name (column_name1 data_type not_null, column_name2 data_type null) primary_key (column_name1);
    CREATE TABLE table_name (column_name1 data_type not_null, column_name2 data_type null) primary_key (column_name1, column_name2) foreign_key (column_name_f, column_namef1) references database_name.table_name (column_name);
    CREATE TABLE table_name (column_name1 data_type not_null unique, column_name2 data_type null) primary_key (column_name1, column_name2) foreign_key (column_name_f, column_namef1) references database_name.table_name (column_name);
    """
    def createTable(self, db, attrs, info):
        db.add_table(attrs, info)
        print('PASS: Table %s is created.' %info['name'])
        return db

    # DROP TABLE a;
    def dropTable(self, db, table_name):
        db.drop_table(table_name)


    # insert into perSON (id, position, name, address) values (2, 'eater', 'Yijing', 'homeless')
    def insertTable(self, db, table_name, attrs, data):
        db.tables[table_name].insert(attrs, data)
        print (db.tables[table_name].data)
        print (db.tables[table_name].datalist)
        return db
        
<<<<<<< HEAD
    def selectQuery(self, db, info):
        # Return select table
        db.select(info)   # Parse
        print(db)
        return db
=======
    def selectQuery(self, db, attrs, tables, where):
        # Return lect table 
        ats = list(attrs.keys())
        table = db.tables[tables[0]]
        if where:
            cond = {'tag': False, 'sym': where[0]['symbol'], 'condition': [where[0]['attr'],  where[0]['value']]}
        else:
            cond = {}
        print ("cond")
        print (cond)
        restable = self.subselect(table, ats, cond)
        return restable

    def subselect(self, table, attrs, where):
        sym = ''
        tag = False
        gb = False
        condition = []
        if where:
            sym = where['sym']
            tag = where['tag']
            condition = where['condition']
        df = table.search(attrs, sym, tag, condition, gb)
        return df

    def join(self, table1, table2, attr):
        df = database('jointempdb').join_table(table1, table2, attr)
        return df

    def addor(self, table1, table2):
        return 

>>>>>>> 341f6b4f05e45ed0d11910ea092df18a49889b10
        
    # lauch function: receieve a command and send to execution function.
    def start(self):
        db = None
        # continue running until recieve the exit command.
        while True:
            inputstr = 'ZibiDB>'
            if db :
                inputstr = db.name + '>'
            else:
                inputstr = 'ZibiDB>'
            commandline = input(inputstr)
            if commandline=='':
                continue
            commandline=commandline.replace(';', '')
            try:
                result, db = self.execute(commandline, db)
                if result == 'exit':
                    print ('BYE')
                    sys.exit(0)
                    return

            # print information of exception
            except Exception as err:
                print (err)

    # execution function: send commandline to parser and get an action as return and execute the mached action function.
    def execute(self, commandline, database):

        db = database

        # send commandline to parser to get an action
        action = parse(commandline)

        if action['mainact'] == 'exit':
            return 'exit', db

        if action['mainact'] == 'create':
            if action['type'] == 'database':
                db = self.createDatabase(action['name'])
            elif action['type'] == 'table':
                if db:
                    if action['name'] in db.tables.keys(): raise Exception('ERROR: Table %s is exsited.' %action['name'])
                    db = self.createTable(db, action['attrls'], action['info'])
                else:
                    raise Exception('ERROR: Please choose a database to use first.')
            return 'continue', db

        if action['mainact'] == 'drop':
            if action['type'] == 'database':
                if db:
                    if action['name'] == db.name:
                        self.dropDatabase(db)
                        db = None
                    else:
                        temp = db
                        db = Database(action['name'])
                        self.dropDatabase(db)
                        db = temp
                else:
                    db = Database(action['name'])
                    self.dropDatabase(db)
                    db = None
            elif action['type'] == 'table':
                self.dropTable(db, action['table_name'])
            return 'continue', db

        if action['mainact'] == 'insert':
            db = self.insertTable(db, action['table_name'], action['attrs'], action['data'])
            return 'continue', db

        if action['mainact'] == 'select':
<<<<<<< HEAD
            self.selectQuery(db, action['data'])
            # self.selectQuery(db, action['content'])
=======
            restable = self.selectQuery(db, action['attrs'], action['tables'], action['where'])
            print (restable)
>>>>>>> 341f6b4f05e45ed0d11910ea092df18a49889b10
            return 'continue', db

        if action['mainact'] == 'delete':
            pass

        if action['mainact'] == 'update':
            pass

        if action['mainact'] == 'save':
            if db:
                if action['name'] == db.name:
                    self.saveDatabase(db)
                else:
                    raise Exception('ERROR: Database %s doesnt exist.' % db.name)
            else:
                raise Exception('ERROR: Database %s doesnt exist.' % db.name)
            return 'continue', db

        if action['mainact'] == 'use':
            db = self.useDatabase(action['name'])
            return 'continue', db

        if action['mainact'] == 'show':
            if action['type'] == 'database':
                self.show_database()
            else:
                self.show_table(db)
            return 'continue', db

