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
        print("inserttable")
        db.tables[table_name].insert(attrs, data)
        print (db.tables[table_name].datalist)
        return db
        
    def selectQuery(self, db, attrs, tables, where):
        # Return restable
        """
        ats = list(attrs.keys())
        table = db.tables[tables[0]]
        if where:
            cond = {'tag': False, 'sym': where[0]['symbol'], 'condition': [where[0]['attr'],  where[0]['value']]}
        else:
            cond = {}
        restable = self.subselect(table, ats, cond)
        return restable
        """        
        ta={}
        attrs = list(attrs.keys())

        if attrs != ["*"]:
            for table_name in tables:
                att_ls=[]
                for attr in attrs:
                    if attr in db.tables[table_name].attrls:
                        att_ls.append(attr)

                ta[table_name]=att_ls

        else:
            for table_name in tables:
                ta[table_name] = ['*']

        print ("ta")
        print(ta)

        tc=[]
        used_attrs=[]
        join_con=[]
        for item in where:
            if item['tag']==1:
                join_con.append(item)
                where.remove(item)
        print(where)

        tbl = tables[::]

        print ("join_con")
        print(join_con)
        if join_con:
            condition=join_con.pop(0)
            print ("condition")
            print(condition)

            # Get first three elems 
            for table_name in tables:
                if condition['attr'] in db.tables[table_name].attrls:
                    tc.append(table_name)
                    tbl.remove(table_name)
                    used_attrs=used_attrs+db.tables[table_name].attrls
                    if (condition['attr'] not in ta[table_name]) & (ta[table_name]!=['*']):
                        ta[table_name].append(condition['attr'])

            for table_name in tables:
                if condition['value'] in db.tables[table_name].attrls:
                    tc.append(table_name)
                    tbl.remove(table_name)
                    used_attrs=used_attrs+db.tables[table_name].attrls
                    if (condition['attr'] not in ta[table_name]) & (ta[table_name]!=['*']):
                        ta[table_name].append(condition['value'])
                    tc.append(condition)

        # Append one table and one condition by order
        while join_con:
            for condition in join_con:
                if condition['attr'] in used_attrs:
                    for table_name in tables:
                        if condition['value'] in db.tables[table_name]:
                            tc.append(table_name)
                            tbl.remove(table_name)
                            used_attrs=used_attrs+db.tables[table_name].attrls
                            tc.append(condition)
                            join_con.remove(condition)
                            if (condition['attr'] not in ta[table_name]) & (ta[table_name]!=['*']):
                                ta[table_name].append(condition['value'])
                    continue
                elif condition['value'] in used_attrs:
                    for table_name in tables:
                        if condition['attr'] in db.tables[table_name]:
                            tc.append(table_name)
                            tbl.remove(table_name)
                            used_attrs=used_attrs+db.tables[table_name].attrls
                            tc.append(condition)
                            join_con.remove(condition)
                            if (condition['attr'] not in ta[table_name]) & (ta[table_name]!=['*']):
                                ta[table_name].append(condition['attr'])
                    continue

                else:
                    raise Exception('ERROR: Invalid symtax')


        while tbl:
            if len(tc) > 3 :
                tc.append(tbl[0])
                tc.append({})
                tbl.pop(0)
            elif (len(tc) == 0) & (len(tbl) > 1):
                tc.append(tbl[0])
                tc.append(tbl[1])
                tc.append({})
                tbl.pop(0)
                tbl.pop(0)
            else:
                tbl.pop(0)

        
        vc = where

        print( {
            'ta': ta,
            'tc': tc,
            'vc': where,

        })

        if tc:

            to = {}
            for tname in ta.keys():
                to[tname] = self.subselect(db.tables[tname], ta[tname], [])

            if tc[2]:
                jointable = self.join(to[tc[0]], to[tc[1]], [tc[2]['attr'], tc[2]['value']])
            else:
                jointable = self.join(to[tc[0]], to[tc[1]], [])
            tc.pop(0)
            tc.pop(0)
            tc.pop(0)
            while tc:
                jointable = self.join(jointable, to[tc[0]], [tc[1]['attr'], tc[1]['value']])
                tc.pop(0)
                tc.pop(0)

            info = {'name': 'test', 'attrs': [], 'primary': '', 'foreign': []}
            table = Table(jointable.columns, info)
            table.df = jointable
            table.flag = 1

        elif len(tables) == 1:
            table = db.tables[tables[0]]


        if vc:
            cond = {'tag': vc[0]['tag'], 'sym': vc[0]['symbol'], 'condition': [vc[0]['attr'],  vc[0]['value']]}
        else:
            cond = {} 

        restable = self.subselect(table, attrs, cond)
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
        else:
            df = table.search(attrs, sym, tag, condition, gb)
        return df

    def join(self, table1, table2, attrs):
        df = Database('jointempdb').join_table(table1, table2, attrs)
        return df

    def addor(self, table1, table2, ao):
        if ao == "0":
            df = Database('jointempdb').df_(table1, table2, attr)
        return df
    def delete(self, db, name, where):
        db.tables[name].delete(name, where)
        return db

    def update(self, db, name, where, set):
        db = self.delete(db, name, where)
        db = self.insertTable(db, name, set['attrs'], set['data'])
        return db

    def createIndex(self, db, table, iname, attr):
        db = db.tables[table].add_index(attr, iname)
        return db

    def dropIndex(self, db, table, iname):
        db = db.tables[table].drop_index(iname)
        return db
        
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
            if commandline=='test':
                _str='create database db;\n'+'create table test (aid int not_null unique, aid2 int unique) primary key (aid);\n'

                for i in range(1001):
                    _str+='insert into test (aid, aid2) values ('+str(i)+', '+str(i)+');\n'

                _str+='create table test1 (bid int not_null unique, bid2 int unique) primary key (bid);\n'

                j=1001
                for i in range(1001):
                    j-=1
                    _str+='insert into test1 (bid, bid2) values ('+str(i)+', '+str(j)+');\n'


                _str+='create table test4 (did int not_null unique, did2 int unique) primary key (did);\n'

                j=10001
                for i in range(10001):
                    j-=1
                    _str+='insert into test4 (did, did2) values ('+str(i)+', '+str(j)+');\n'

                _str+='create table test3 (cid int not_null unique, cid2 int unique) primary key (cid);\n'
                for i in range(10001):
                    _str+='insert into test3 (cid, cid2) values ('+str(i)+', '+str(i)+');\n'

                _str+='create table test5 (eid int not_null unique, eid2 int unique) primary key (eid);\n'
                for i in range(100001):
                    _str+='insert into test5 (eid, eid2) values ('+str(i)+', '+str(i)+');\n'

                _str+='create table test6 (fid int not_null unique, fid2 int unique) primary key (fid);\n'

                j=100001
                for i in range(100001):
                    j-=1
                    _str+='insert into test6 (fid, fid2) values ('+str(i)+', '+str(j)+');\n'

                # _str+='create table test6 (did int not_null unique, did2 int unique) primary key (did);\n'
                # j=100001
                # for i in range(100001):
                #     j-=1
                #     _str+='insert into test6 (fid, fid2) values ('+str(i)+', '+str(j)+');\n'
                
                _str+='save database db;\n'

                commandlines=_str.split(';\n')
                for commandline in commandlines:
                    # print(commandline)
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
                continue


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
            elif action['type'] == 'index':
                if db:
                    db = self.createIndex(db, action['table'], action['index_name'], action['attrs'])
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
            elif action['type'] == 'index':
                if db:
                    db = self.dropIndex(db, action['table'], action['index_name'])
                else:
                    raise Exception('ERROR: Please choose a database to use first.')

            return 'continue', db

        if action['mainact'] == 'insert':
            db = self.insertTable(db, action['table_name'], action['attrs'], action['data'])
            return 'continue', db

        if action['mainact'] == 'select':

            if db:
                restable = self.selectQuery(db, action['attrs'], action['tables'], action['where'])
                print (restable)
                return 'continue', db
            else:
                raise Exception('ERROR: Use database first.')

        if action['mainact'] == 'delete':
            db = self.delete(db, action['table'], action['where'])
            return 'continue', db 

        if action['mainact'] == 'update':
            db = self.update(db, action['table'], action['where'], action['set'])
            return 'continue', db 

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

