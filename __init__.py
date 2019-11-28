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

    # show;
    def show(self):
        t = sys.argv[0]      
        t = t[:-11] + 'database/'
        dirs = os.listdir(t)
        for dir in dirs:
            if '.' not in dir:
                print(dir)

    """
    CREATE TABLE database_name.table_name (column_name1 data_type not_null, column_name2 data_type null) primary_key (column_name1);
    CREATE TABLE database_name.table_name (column_name1 data_type not_null, column_name2 data_type null) primary_key (column_name1, column_name2) foreign_key (column_name_f, column_namef1) references database_name.table_name (column_name);
    CREATE TABLE database_name.table_name (column_name1 data_type not_null unique, column_name2 data_type null) primary_key (column_name1, column_name2) foreign_key (column_name_f, column_namef1) references database_name.table_name (column_name);

    """
    def createTable(self, db, table_name, table_info):
        std_type=['CHAR', 'FLOAT', 'INT']

        # Get attributes
        # (column_name1 data_type not_null, column_name2 data_type null)
        table_attrs=[]
        while True:
            if '(' in table_info[0]:
                table_attrs.append(table_info.pop(0).replace('(', ''))
            elif ')' in table_info[0]:
                table_attrs.append(table_info.pop(0).replace(')', ''))
                break
            else:
                table_attrs.append(table_info.pop(0))
        # Each elements contains attrbute type constrain
        table_attrs=' '.join(table_attrs).split(',')

        # for each elem in table_attrs, get attribute name, types and constrains(null status and unique status)
        attrs=[]
        _type=[]
        null_status=[]
        unique_status=[]
        for i in table_attrs:
            tmp=i.strip().split(' ')
            attrs.append(tmp[0].lower())
            _type.append(tmp[1].upper())

            # There is null or/and unique status
            if len(tmp)==3: 
                # print(tmp)
                if tmp[2].upper()=='NOT_NULL' or tmp[2].upper()=='NULL':
                    null_status.append(tmp[2].upper())
                    unique_status.append('')

                elif tmp[2].upper()=='UNIQUE' or tmp[2].upper()=='NOT_UNIQUE':
                    unique_status.append(tmp[2].upper())
                    null_status.append('')
                        
            # Only have (attrs and type)
            elif len(tmp)==2:
                null_status.append('')
                unique_status.append('')
            elif len(tmp)==4:
                if tmp[2].upper()!='NOT_NULL' and tmp[2].upper()!='NULL': raise Exception('ERROR: Invalid syntax.')
                if tmp[3].upper()!='NOT_UNIQUE' and tmp[3].upper()!='UNIQUE': raise Exception('ERROR: Invalid syntax.')
                null_status.append(tmp[2].upper())
                unique_status.append(tmp[3].upper())
            else: raise Exception('ERROR: Invalid syntax.')
        # print(attrs)
        # print(_type)
        # print(null_status)
        # print(unique_status)

        # Get primary key
        primary_key=[]
        if table_info:
            if table_info[0].upper()=='PRIMARY':
                table_info.pop(0)   # Pop PRIMARY

                # Pop KEY
                if table_info.pop(0).upper()=='KEY':
                    if '(' in table_info[0] and ')' in table_info[0]:
                        primary_key.append(table_info.pop(0).strip('()'))
                    else:
                        while True:
                            if '(' in table_info[0]:
                                primary_key.append(table_info.pop(0).strip('( ').lower())
                            elif ')' in table_info[0]:
                                primary_key.append(table_info.pop(0).strip(') ').lower())
                                break
                            else:
                                primary_key.append(table_info.pop(0).strip().lower())
                else: raise Exception('ERROR: Invalid syntax.')
        # print(primary_key)
        foreign_key=[]
        if table_info:
            if table_info[0].upper()=='FOREIGN':
                table_info.pop(0)   # Pop foreign

                # Pop 'KEY'
                if table_info.pop(0).upper()=='KEY':
                    if '(' in table_info[0] and ')' in table_info[0]:
                        foreign_key.append(table_info.pop(0).strip('()'))
                    else:
                        while True:
                            if '(' in table_info[0]:
                                foreign_key.append(table_info.pop(0).strip('( ').lower())
                            elif ')' in table_info[0]:
                                foreign_key.append(table_info.pop(0).strip(') ').lower())
                                break
                            else:
                                foreign_key.append(table_info.pop(0).strip().lower())
                else: raise Exception('ERROR: Invalid syntax.')

        ref_table=[]
        ref_column=[]
        ref_columns=[]
        if table_info:
            while table_info[0].upper()=='REFERENCES':
                table_info.pop(0)   # Pop REFERENCES

                # [ref_database, ref_table]
                ref_name=table_info.pop(0)# Table basename

                ref_table.append(ref_name)

                if '(' in table_info[0] and ')' in table_info[0]:
                    ref_column.append(table_info.pop(0).strip('() '))
                    ref_columns.append(ref_column)
                else:
                    while True:
                        if '(' in table_info[0]:
                            ref_column.append(table_info.pop(0).strip('( ').lower())
                        elif ')' in table_info[0]:
                            ref_column.append(table_info.pop(0).strip(') ').lower())
                            ref_columns.append(ref_column)
                            break
                        else:
                            ref_column.append(table_info.pop(0).strip().lower())
                if not table_info: break
        # TODO: Each reference should have a on_delete and on_update
        # print(table_info)
        on_delete='NO_ACTION'
        if table_info:
            if table_info[0].upper()=='ON':

                if table_info[1].upper()=='DELETE':
                    table_info.pop(0)    # Pop on
                    table_info.pop(0)    # Pop delete
                    on_delete=table_info.pop(0)

        # print(table_info)
        on_update='NO_ACTION'
        if table_info:
            if table_info[0].upper()=='ON':
                    if table_info[1].upper()=='UPDATE':
                        table_info.pop(0)    # Pop on
                        table_info.pop(0)    # Pop 'UPDATE'
                        on_update=table_info.pop(0)

        ref_info=[{'schema': ref_table, 'columns': ref_columns, 'on_delete': on_delete.upper(), 'on_update': on_update.upper()}]
        # print(ref_info)

        attrs_ls=[]
        for i in range(len(attrs)):
            if _type[i] not in std_type:
                raise Exception('ERROR: Invalid type.')
            attrs_ls.append({
                    'name':attrs[i],
                    'type': _type[i],
                    'notnull': 1 if null_status[i].upper()=='NOT_NULL' else 0,
                    'unique': 1 if unique_status[i].upper()=='UNIQUE' else 0,
            })
        # print(attrs_ls)
        
        foreignk = {}

        for attri in foreign_key:
            foreignk[attri] = ref_info[0]
            i += 1
        
        info={
            'name':table_name,
            'attrs':attrs_ls,
            'primary': primary_key,
            'foreign': foreignk,
        }

        db.add_table(info)

        print('PASS: Table %s is created.' %table_name)

        return db

    # DROP TABLE a;
    def dropTable(self, db, table_name):
        db.drop_table(table_name)


    # insert into notap.perSON (id, position, name, address) values (2, 'eater', 'Yijing', 'homeless')
    def insertTable(self, database_name, table_name, table_info):
        
        database_dir='./ZibiDB/database/'+database_name

        # Check database and schema existence
        if not os.path.exists(database_dir): raise Exception('ERROR: '+database_name.upper()+' is invalid database.')
        if not os.path.exists(database_dir+'/'+table_name+'.json') or not os.path.exists(database_dir+'/'+table_name+'.csv'): raise Exception('ERROR: '+table_name.upper()+' is invalid schema.')

        # print(database_name)
        # print(table_name)
        table_info=' '.join(table_info)
        if 'values' in table_info: 
            table_info=table_info.split('values')
        elif ' values ' in table_info: 
            table_info=table_info.split(' values ')
        elif ' values' in table_info: 
            table_info=table_info.split(' values')
        elif 'values ' in table_info: 
            table_info=table_info.split('values ')
        elif 'VALUES' in table_info: 
            table_info=table_info.split('VALUES')
        elif ' VALUES ' in table_info: 
            table_info=table_info.split(' VALUES ')
        elif ' VALUES' in table_info: 
            table_info=table_info.split(' VALUES')
        elif 'VALUES ' in table_info: 
            table_info=table_info.split('VALUES ')
        else:
            raise Exception('ERROR: Invalid syntax.')

        # print(table_info)
        
        # Process attrs and values
        _attrs=list(map(str.strip, table_info[0].strip('() ').split(',')))
        vals=table_info[1].strip('() ').split(',')

        values=[]
        for val in vals:
            if "'" in val:
                values.append(val.replace("'", '').strip())
            else:
                values.append(val.strip())

        if len(_attrs)!=len(values): raise Exception('ERROR: Invalid syntax.')

        # print(_attrs)
        # print(values)

        # Get and Parse JSON
        with open(database_dir+'/'+table_name+'.json', 'r') as f:
            line=f.readline().strip()
            j=json.loads(line)
            f.close()

        keys=[]
        _t={}
        for attributes in j['person'][0]['Attributes']:
            for attribute in attributes:
                keys.append(attribute)
                _t[attribute]=attributes[attribute][0]

        # Check inserting keys are same with _attrs of table
        if set(keys)!=set(_attrs): raise Exception('ERROR: Invalid attributes.')

        # Check the values have corresponding type
        i=0
        for attr in _attrs:
            if self.checkType(values[i], _t[attr]['type'])==False:
                raise Exception('ERROR: Invalid types.')
            i=i+1

        with open(database_dir+'/'+table_name+'.csv', 'a', newline='') as f:
            writer=csv.writer(f)
            writer.writerow(values)

            f.close()
        print('PASS: The data is inserted.')

    def checkType(self, value, _type):

        if _type=='CHAR':
            value=str(value)
            return True
        elif _type=='INT':
            try:value=int(value)
            except:raise Exception('ERROR: Invalid types INT.')

            if type(value)!=type(1):
                raise Exception('ERROR: Invalid types INT.')
            return True
        elif _type=='FLOAT':
            try:value=float(value)
            except:raise Exception('ERROR: Invalid types FLOAT.')
            
            if type(value)!=type(1.0):
                raise Exception('ERROR: Invalid types FLOAT.')
            return True
        else:
            raise Exception('ERROR: Invalid types.')
        return False

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
                    if action['table_name'] in db.tables.keys(): raise Exception('ERROR: Table %s is exsited.' %action['table_name'])
                    db = self.createTable(db, action['table_name'], action['info'])
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
            self.insertTable(action['database_name'], action['table_name'], action['info'])
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
            self.show()
            return 'continue', db

