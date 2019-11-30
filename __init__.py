import os
import shutil
import csv
import json
import pandas
import sys
from ZibiDB.parser import parse
from ZibiDB.core.database import Database

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
        db = Database(name)
        db.load()
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
    def createTable(self, db, info):
        db.add_table(info)
        print('PASS: Table %s is created.' %info['name'])
        return db

    # DROP TABLE a;
    def dropTable(self, db, table_name):
        db.drop_table(table_name)


    # insert into perSON (id, position, name, address) values (2, 'eater', 'Yijing', 'homeless')
    def insertTable(self, db, table_name, attrs, data):
        db.tables[table_name].insert(attrs, data)
        print (db.tables[table_name].data)
        return db
        
    def selectQuery(self, info):

        symbols=['=', '<', '>', '<=', '>=', '<>', 'LIKE']

        key_words=['FROM', 'WHERE', 'ORDER', 'GROUP', 'BY']
        
        aggregate_key_words=['MAX', 'MIN', 'DISTINCT', 'AVG', 'COUNT', 'SUM']
        where_key_words=['OR', 'AND', 'IN', 'BETWEEN', 'NOT', 'EXIST']

        groupBy_key_words=['HAVING']
        orderBy_key_words=['DESC']


        print(info)

        # Get selected attrs
        select_attrs=[]
        
        while info[0].upper() not in key_words:
            select_attrs.append(info.pop(0).lower().strip(', '))

        print('attrs: ', select_attrs)

        # Get selected tables' names
        select_tables=[]
        if info.pop(0).upper()=='FROM':
            
            while info[0].upper() not in key_words:
                select_tables.append(info.pop(0).lower().strip(', '))

        else: raise Exception('ERROR: Invalid syntax.')
        print('tables: ', select_tables)

        # Get where clause
        # Where or not where both okay
        where_clause=[]
        conditions=[]
        op=[]
        if info:
            if info[0].upper()=='WHERE':
                info.pop(0) #Pop where

                while info[0].upper() not in key_words:
                    where_clause.append(info.pop(0).strip(', '))
                    if not info:
                        break

                # TODO: Parse where clause
                temp=[]
                for i in range(len(where_clause)):
                    condition=dict()
                    if (where_clause[i].upper() in ['OR', 'AND'] and where_clause[i-2].upper()!='BETWEEN') or i==len(where_clause)-1:
                        if i==len(where_clause)-1: 
                            temp.append(where_clause[i])
                        else:
                            op.append(where_clause[i].upper())

                        if temp:
                            temp=' '.join(temp)
                            if '<=' in temp:
                                tmp=temp.split('<=')
                                condition={'attr': tmp[0].lower(), 'value': tmp[1], 'symbol': '<='}
                            elif '>=' in temp:
                                tmp=temp.split('>=')
                                condition={'attr': tmp[0].lower(), 'value': tmp[1], 'symbol': '>='}
                            elif '<>' in temp:
                                tmp=temp.split('<>')
                                condition={'attr': tmp[0].lower(), 'value': tmp[1], 'symbol': '<>'}
                            elif '=' in temp:
                                tmp=temp.split('=')
                                condition={'attr': tmp[0].lower(), 'value': tmp[1], 'symbol': '='}
                            elif '<' in temp:
                                tmp=temp.split('<')
                                condition={'attr': tmp[0].lower(), 'value': tmp[1], 'symbol': '<'}
                            elif '>' in temp:
                                tmp=temp.split('>')
                                condition={'attr': tmp[0].lower(), 'value': tmp[1], 'symbol': '>'}
                            elif ' LIKE ' in temp.upper():
                                tmp=temp.split(' ')
                                condition={'attr': tmp[0].lower(), 'value': tmp[2].strip("'"), 'symbol': 'LIKE'}
                            elif 'BETWEEN' in temp.upper():
                                tmp=temp.split(' ')
                                tmp_attr=tmp.pop(0).lower() #Pop attr
                                if tmp.pop(0).upper()!='BETWEEN': raise Exception('ERROR: Invalid Where Clause.')   #Pop Between

                                try: 
                                    if float(tmp[0])>float(tmp[2]): 
                                        raise Exception('ERROR: Invalid Where Clause.')
                                except: raise Exception('ERROR: Invalid Where Clause.')

                                # Value 1
                                conditions.append({
                                    'attr': tmp_attr,
                                    'value': tmp.pop(0),
                                    'symbol': '>='
                                })

                                if tmp.pop(0).upper()!='AND': raise Exception('ERROR: Invalid Where Clause.')   # Pop AND

                                # Value 2
                                conditions.append({
                                    'attr': tmp_attr,
                                    'value': tmp.pop(0),
                                    'symbol': '<='
                                })
                                op.append('AND')
                                temp=[]
                                continue

                            elif ' IN ' in temp.upper():
                                # AND id in (1, 2, 3) OR
                                tmp=temp.split(' ')
                                tmp_attr=tmp.pop(0).lower()    # Pop attr
                                if tmp.pop(0).upper()!='IN': raise Exception('ERROR: Invalid Where Clause.')  # Pop IN
                                tmp=','.join(tmp).strip('() ').split(',')
                                for val in tmp:
                                    conditions.append({
                                        'attr': tmp_attr,
                                        'value': val.strip(', '),
                                        'symbol': '='
                                    })
                                for _ in range(len(tmp)-1):
                                    op.append('OR')
                                temp=[]
                                continue

                            else: raise Exception('ERROR: Invalid Where Clause.')
                            conditions.append(condition)
                        else: raise Exception('ERROR: Invalid Where Clause.')
                        temp=[]
                    else:
                        temp.append(where_clause[i])
                                                
        # print('where, ', where_clause)
        print('op: ', op, len(op))
        print('conditions: ', conditions, len(conditions))

        # Get group by clause
        groupBy_clause=[]
        group_dict=dict()
        if info:
            if info[0].upper()=='GROUP':
                info.pop(0) #Pop GROUP
                if info.pop(0).upper()!='BY': raise Exception('ERROR: Invalid syntax.') #Pop BY

                while info[0].upper() not in key_words:
                    groupBy_clause.append(info.pop(0).strip(', '))
                    if not info:
                        break

                # Parse group by clause
                # If the first elem is having, error
                if groupBy_clause[0].upper()=='HAVING': raise Exception('ERROR: Invalid syntax.')

                groupBy=[]
                having=[]
                
                for i in range(len(groupBy_clause)):
                    if groupBy_clause[i].upper()=='HAVING':
                        having=groupBy_clause[i+1:]
                        break
                    groupBy.append(groupBy_clause[i])
                group_dict={
                    'group_by': groupBy,
                    'having': having
                }
                                        
        print('group by: ', group_dict)

        # Get order by clause
        orderBy_clause=[]
        orderBy_dict=dict()
        if info:
            if info[0].upper()=='ORDER':
                info.pop(0) #Pop ORDER
                if info.pop(0).upper()!='BY': raise Exception('ERROR: Invalid syntax.') #Pop BY

                while info[0].upper() not in key_words:
                    orderBy_clause.append(info.pop(0).lower().strip(', '))
                    if not info:
                        break

                # Parse order by clause
                orderBy=[]
                # If desc is in clause, but last elem is not it, error
                # else desc is 1
                if 'DESC' in ' '.join(orderBy_clause).upper():
                    if orderBy_clause[-1].upper()!='DESC':
                        raise Exception('ERROR: Invalid syntax.')
                    else:
                        desc=1
                        orderBy=orderBy_clause[:len(orderBy_clause)-1]
                else:
                    desc=0
                    orderBy=orderBy_clause
                orderBy_dict={
                    'order_by': orderBy,
                    'desc': desc,
                }

        print('order by: ', orderBy_dict)

        if info!=[]:
            raise Exception('ERROR: Invalid syntax.')
        print(info)

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
                    db = self.createTable(db, action['info'])
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
            self.selectQuery(action['content'])
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

