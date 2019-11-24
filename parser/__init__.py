import os
import shutil
import csv
import json
import pandas

# pattern

# action

# symbol

def parse(commandline):

    if commandline.upper() == 'EXIT':
        action = 'exit'
    else:
        action = commandline.split(' ')
        while True:
            if '' not in action:
                break
            action.remove('')

        # print(action)

        # CREATE
        if action[0].upper() == 'CREATE':

            # CREATE DATABASE database_name;
            if action[1].upper() == 'DATABASE':
                createDatabase(action)
            elif action[1].upper() == 'TABLE':
                name=action[2].split('.')

                database_name=name[0]
                table_name=name[1]
                table_info=action[3:]

                createTable(database_name, table_name.lower(), table_info)
            else:
                raise Exception('ERROR: Only accept CREATE DATABASE/TABLE.')
        
        # DROP
        elif action[0].upper() == 'DROP':
            if action[1].upper() == 'DATABASE':
                dropDatabase(action)
            elif action[1].upper() == 'TABLE':
                name=action[2].split('.')
                dropTable(name[0], name[1].lower())
            else:
                raise Exception('ERROR: Only accept DROP DATABASE/TABLE.')
        
        # INSERT
        elif action[0].upper() == 'INSERT':
            if action[1].upper() == 'INTO':
                name=action[2].split('.')
                database_name=name[0].lower()
                table_name=name[1].lower()
                table_info=action[3:]
                insertTable(database_name, table_name, table_info)
            else:
                raise Exception('ERROR: Only accept INSERT TABLE.')

        # SELECT
        elif action[0].upper() == 'SELECT':
            selectQuery(' '.join(action[1:]))
                
        else:
            raise Exception('Only accept exit, drop database/table, and create database/table command now ~~~~ : p')

    return action

def selectQuery(info):
    print(info)
    if 'FROM' in info:
        info=info.split('FROM')
    elif 'from' in info:
        info=info.split('from')
    elif 'From' in info:
        info=info.split('From')
    else: raise Exception('ERROR: Invalid syntax.')

    # Get attrs
    select_attrs=info.pop(0).split(',')
    select_attrs=list(map(str.strip, select_attrs))
    select_attrs=list(map(str.lower, select_attrs))
    print(select_attrs)

    info=' '.join(info)
    if ' WHERE ' in info:
        info=info.split(' WHERE ')
    elif ' where ' in info:
        info=info.split(' where ')
    elif ' Where ' in info:
        info=info.split(' Where ')
    else: info=[info]
    
    # Get table's names
    select_tables=info.pop(0).split(',')
    select_tables=list(map(str.strip, select_tables))
    select_tables=list(map(str.lower, select_tables))
    print(select_tables)

    info=' '.join(info)
    print(info)


# CREATE DATABASE test;
def createDatabase(action):
    database = action[2].replace(';', '').lower()
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
def createTable(database_name, table_name, table_info):
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
def dropDatabase(action):
    database = action[2].replace(';', '').lower()
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
def dropTable(database_name, table_name):
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
def insertTable(database_name, table_name, table_info):
    
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
