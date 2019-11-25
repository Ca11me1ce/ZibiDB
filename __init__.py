import os
import shutil
import csv
import json
import pandas
from ZibiDB.parser import parse

# database engine
class Engine:

# action functions
    # CREATE DATABASE test;
    def createDatabase(self, name):
        database = name.replace(';', '').lower()
        _database='./ZibiDB/database/'

        # If the database directory is exist, pass
        if not os.path.exists(_database):
            os.makedirs(_database)

        # If the database is exist, ERROR
        if os.path.exists(_database+database):
            raise Exception('ERROR: The database is exist already.')
            
        # If the database is not exist, create and PASS
        elif not os.path.exists(_database+database):
            os.makedirs(_database+database)
            print('PASS: The database is created.')
            return

        else:
            raise Exception('ERROR: Invalid command.')
    '''
    CREATE TABLE database_name.table_name (column_name1 data_type not_null, column_name2 data_type null) primary_key (column_name1);
    CREATE TABLE database_name.table_name (column_name1 data_type not_null, column_name2 data_type null) primary_key (column_name1, column_name2) foreign_key (column_name_f, column_namef1) references database_name.table_name (column_name);
    CREATE TABLE database_name.table_name (column_name1 data_type not_null unique, column_name2 data_type null) primary_key (column_name1, column_name2) foreign_key (column_name_f, column_namef1) references database_name.table_name (column_name);

    '''
    def createTable(self, database_name, table_name, table_info):
        # print(database_name)
        database_dir='./ZibiDB/database/'+database_name
        std_type=['CHAR', 'FLOAT', 'INT']

        # Check database and schema existence
        if not os.path.exists(database_dir):raise Exception('ERROR: '+database_name.upper()+' is invalid database.')
        if os.path.exists(database_dir+'/'+table_name+'.json') or os.path.exists(database_dir+'/'+table_name+'.csv'):raise Exception('ERROR: '+table_name.upper()+' is exist schema.')

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

            # TODO: Parse null, unique, and constrains

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

        # print('null', null_status)
        # print('unique', unique_status)

        # print(table_attrs)
        # print(attrs)
        # print(_type)
        # print(null_status)
        
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
        # Get foreign key
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

        # print(primary_key)
        # print(foreign_key)
        ref_database=[]
        ref_table=[]
        ref_column=[]
        ref_columns=[]
        if table_info:
            while table_info[0].upper()=='REFERENCES':
                table_info.pop(0)   # Pop REFERENCES

                # [ref_database, ref_table]
                ref_name=table_info.pop(0).split('.')


                ref_database.append(ref_name[0])
                ref_table.append(ref_name[1])

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

        # TODO: Each reference should have a on_delete and on_update
        on_delete='NO_ACTION'
        if table_info:
            if table_info[0].upper()=='ON':
                if table_info[1].upper()=='DELETE':
                    table_info.pop(0)    # Pop on
                    table_info.pop(0)    # Pop delete
                    on_delete=table_info.pop(0)

        on_update='NO_ACTION'
        if table_info[0].upper()=='ON':
                if table_info[1].upper()=='UPDATE':
                    table_info.pop(0)    # Pop on
                    table_info.pop(0)    # Pop 'UPDATE'
                    on_update=table_info.pop(0)

        # print(ref_database)
        # print(ref_table)
        # print(ref_columns)
        ref_info=[{'database': ref_database, 'schema': ref_table, 'columns': ref_columns, 'on_delete': on_delete.upper(), 'on_update': on_update.upper()}]

        attrs_ls=[]
        for i in range(len(attrs)):
            if _type[i] not in std_type:
                raise Exception('ERROR: Invalid type.')
            attrs_ls.append({
                attrs[i]:[{
                    'type': _type[i],
                    'not_null': 1 if null_status[i].upper()=='NOT_NULL' else 0,
                    'unique': 1 if unique_status[i].upper()=='UNIQUE' else 0,
                }]
            })

        info=[{
            'Attributes':attrs_ls,
            # 'Types': _type,
            # 'Null_status': null_status,
            'Primary_key': primary_key,
            'Foreign_key': foreign_key,
            'Reference': ref_info
        }]

        # print(info)
        with open(database_dir+'/'+table_name+'.json', 'w') as f:
            json.dump({table_name: info}, f)
            f.close()
        with open(database_dir+'/'+table_name+'.csv', 'w', newline='') as f:
            writer=csv.writer(f)
            writer.writerow(attrs)
            f.close()

        print('PASS: The schema is created.')

    # DROP DATABASE test;
    def dropDatabase(self, name):
        database = name.replace(';', '').lower()
        _database='./ZibiDB/database/'

        # If the database directory is exist, pass
        if not os.path.exists(_database):
            raise Exception('ERROR: No any databases')

        # If the database is exist, drop and PASS
        if os.path.exists(_database+database):
            shutil.rmtree(_database+database)
            print('PASS: The database is dropped.')
            return
            
        # If the database is not exist, ERROR
        elif not os.path.exists(_database+database):
            raise Exception('ERROR: The database is not exist.')

        else:
            raise Exception('ERROR: Invalid command.')

    # DROP TABLE database_name.table_name;
    def dropTable(self, database_name, table_name):
        # print(database_name)
        # print(table_name)

        database_dir='./ZibiDB/database/'+database_name

        # Check database
        if not os.path.exists(database_dir):
            raise Exception('ERROR: '+database_name.upper()+' is invalid database.')

        if not os.path.exists(database_dir+'/'+table_name+'.json') and not os.path.exists(database_dir+'/'+table_name+'.csv'):
            raise Exception('ERROR: '+table_name.upper()+' is not exist schema.')

        os.remove(database_dir+'/'+table_name+'.json')
        os.remove(database_dir+'/'+table_name+'.csv')

        print('PASS: The schema is dropped.')

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
            if checkType(values[i], _t[attr]['type'])==False:
                raise Exception('ERROR: Invalid types.')
            i=i+1

        with open(database_dir+'/'+table_name+'.csv', 'a', newline='') as f:
            writer=csv.writer(f)
            writer.writerow(values)

            f.close()
        print('PASS: The data is inserted.')

    def checkType(value, _type):

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

        attr_key_words=['MAX', 'MIN', 'DISTINCT', 'AVG', 'COUNT', 'SUM']
        where_key_words=['OR', 'AND', 'IN', 'BETWEEN', 'LIKE', 'NOT', 'EXIST']

        key_words=['FROM', 'WHERE', 'ORDER', 'GROUP', 'BY']

        groupBy_key_words=['HAVING']
        orderBy_key_words=['DESC']


        print(info)

        # Get selected attrs
        select_attrs=[]
        count=0
        for i in info:
            print(i)
            if i.upper() in key_words:
                break
            select_attrs.append(i.lower().strip(', '))
            count+=1
        for i in range(count):
            info.pop(0)

        print('attrs: ', select_attrs)

        # Get selected tables' names
        count=0
        select_tables=[]
        if info.pop(0).upper()=='FROM':
            for i in info:
                if i.upper() in key_words:
                    break
                select_tables.append(i.lower().strip(', '))
                count+=1
        else: raise Exception('ERROR: Invalid syntax.')
        for i in range(count):
            info.pop(0)
        print('tables: ', select_tables)

        # Get where clause
        # Where or not where both okay
        where_clause=[]
        count=0
        if info:
            if info[0].upper()=='WHERE':
                info.pop(0) #Pop where
                for i in info:
                    if i.upper() in key_words:
                        break
                    where_clause.append(i.strip(', '))
                    count+=1
                for i in range(count):
                    info.pop(0)

                # TODO: Parse where clause
        print('where, ', where_clause)

        # Get group by clause
        groupBy_clause=[]
        count=0
        group_dict=dict()
        if info:
            if info[0].upper()=='GROUP':
                info.pop(0) #Pop GROUP
                if info.pop(0).upper()!='BY': raise Exception('ERROR: Invalid syntax.') #Pop BY

                for i in info:
                    if i.upper() in key_words:
                        break
                    groupBy_clause.append(i.lower().strip(', '))
                    count+=1
                for i in range(count):
                    info.pop(0)

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
        count=0
        orderBy_dict=dict()
        if info:
            if info[0].upper()=='ORDER':
                info.pop(0) #Pop ORDER
                if info.pop(0).upper()!='BY': raise Exception('ERROR: Invalid syntax.') #Pop BY

                for i in info:
                    if i.upper() in key_words:
                        break
                    orderBy_clause.append(i.lower().strip(', '))
                    count+=1
                for i in range(count):
                    info.pop(0)
                    
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
        # continue running until recieve the exit command.
       while True:
            commandline = input('ZibiDB>')
            if commandline=='':
                continue
            commandline=commandline.replace(';', '')
            try:
                result = self.execute(commandline)
                if result == 'exit':
                    print ('BYE')
                    return

            # print information of exception
            except Exception as err:
                print (err)

    # execution function: send commandline to parser and get an action as return and execute the mached action function.
    def execute(self, commandline):

        # send commandline to parser to get an action
        action = parse(commandline)

        if action['mainact'] == 'exit':
            return 'exit'

        if action['mainact'] == 'create':
            if action['type'] == 'database':
                self.createDatabase(action['name'])
            elif action['type'] == 'table':
                self.createTable(action['database_name'], action['table_name'], action['info'])

        if action['mainact'] == 'drop':
            if action['type'] == 'database':
                print (action['name'])
                self.dropDatabase(action['name'])
            elif action['type'] == 'table':
                self.dropTable(action['database_name'], action['table_name'])

        if action['mainact'] == 'insert':
            self.insertTable(action['database_name'], action['table_name'], action['info'])

        if action['mainact'] == 'select':
            self.selectQuery(action['content'])



