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

                createTable(database_name, table_name, table_info)
            else:
                raise Exception('ERROR: Only accept CREATE DATABASE.')
        
        elif action[0].upper() == 'DROP':
            if action[1].upper() == 'DATABASE':
                dropDatabase(action)
            elif action[1].upper() == 'TABLE':
                name=action[2].split('.')
                dropTable(name[0], name[1])
            else:
                raise Exception('ERROR: Only accept DROP DATABASE.')
                
        else:
            raise Exception('Only accept exit, drop database/table, and create database/table command now ~~~~ : p')

    return action

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

# CREATE TABLE database_name.table_name (column_name1 data_type not_null, column_name2 data_type null) primary_key (column_name1);
# CREATE TABLE database_name.table_name (column_name1 data_type not_null, column_name2 data_type null) primary_key (column_name1, column_name2) foreign_key (column_name_f, column_namef1) references database_name.table_name (column_name);
def createTable(database_name, table_name, table_info):
    # print(database_name)
    database_dir='./ZibiDB/database/'+database_name

    # Check database
    if not os.path.exists(database_dir):
        raise Exception('ERROR: '+database_name.upper()+' is invalid database.')
    # print(table_name)

    # Check schema
    if os.path.exists(database_dir+'/'+table_name+'.json') or os.path.exists(database_dir+'/'+table_name+'.csv'):
        raise Exception('ERROR: '+table_name.upper()+' is exist schema.')

    table_attrs=[]
    while True:
        if '(' in table_info[0]:
            table_attrs.append(table_info.pop(0).replace('(', ''))
        elif ')' in table_info[0]:
            table_attrs.append(table_info.pop(0).replace(')', ''))
            break
        else:
            table_attrs.append(table_info.pop(0))
    table_attrs=' '.join(table_attrs).split(',')

    # Get attributes, types and nulls
    attrs=[]
    _type=[]
    null_status=[]
    for i in table_attrs:
        tmp=i.strip().split(' ')
        attrs.append(tmp[0].lower())
        _type.append(tmp[1].upper())
        try:
            null_status.append(tmp[2].upper())
        except:
            null_status.append('')

    # print(table_attrs)
    # print(attrs)
    # print(_type)
    # print(null_status)
    
    # Get primary key
    primary_key=[]
    if table_info:
        if table_info[0].upper()=='PRIMARY_KEY':
            table_info.pop(0)   # Pop PRIMARY_KEY
            if '(' in table_info[0] and ')' in table_info[0]:
                tmp=table_info.pop(0).replace('(', '').replace(')', '')
                primary_key.append(tmp)
            else:
                while True:
                    if '(' in table_info[0]:
                        primary_key.append(table_info.pop(0).replace('(', '').strip().lower())
                    elif ')' in table_info[0]:
                        primary_key.append(table_info.pop(0).replace(')', '').strip().lower())
                        break
                    else:
                        primary_key.append(table_info.pop(0).strip().lower())

    # print(primary_key)
    # Get foreign key
    foreign_key=[]
    if table_info:
        if table_info[0].upper()=='FOREIGN_KEY':
            table_info.pop(0)   # Pop foreign_key
            if '(' in table_info[0] and ')' in table_info[0]:
                tmp=table_info.pop(0).replace('(', '').replace(')', '')
                foreign_key.append(tmp)
            else:
                while True:
                    if '(' in table_info[0]:
                        foreign_key.append(table_info.pop(0).replace('(', '').strip().lower())
                    elif ')' in table_info[0]:
                        foreign_key.append(table_info.pop(0).replace(')', '').strip().lower())
                        break
                    else:
                        foreign_key.append(table_info.pop(0).strip().lower())

    # print(primary_key)
    # print(foreign_key)
    ref_database=''
    ref_table=''
    ref_columns=[]
    if table_info:
        if table_info[0].upper()=='REFERENCES':
            table_info.pop(0)   # Pop REFERENCES
            ref_name=table_info.pop(0).split('.')
            ref_database=ref_name[0]
            ref_table=ref_name[1]

            if '(' in table_info[0] and ')' in table_info[0]:
                tmp=table_info.pop(0).replace('(', '').replace(')', '')
                ref_columns.append(tmp)
            else:
                while True:
                    if '(' in table_info[0]:
                        ref_columns.append(table_info.pop(0).replace('(', '').strip().lower())
                    elif ')' in table_info[0]:
                        ref_columns.append(table_info.pop(0).replace(')', '').strip().lower())
                        break
                    else:
                        ref_columns.append(table_info.pop(0).strip().lower())

    # print(ref_database)
    # print(ref_table)
    # print(ref_columns)
    ref_info=[{'database': ref_database}, {'schema': ref_table}, {'columns': ref_columns}]

    attrs_ls=[]
    for i in range(len(attrs)):
        attrs_ls.append({
            attrs[i]:[_type[i], null_status[i]]
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
    # print(table_info)
    with open(database_dir+'/'+table_name+'.json', 'w') as f:
        json.dump({table_name: info}, f)
        f.close()
    with open(database_dir+'/'+table_name+'.csv', 'w') as f:
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
    print(database_name)
    print(table_name)

    database_dir='./ZibiDB/database/'+database_name

    # Check database
    if not os.path.exists(database_dir):
        raise Exception('ERROR: '+database_name.upper()+' is invalid database.')

    if not os.path.exists(database_dir+'/'+table_name+'.json') and not os.path.exists(database_dir+'/'+table_name+'.csv'):
        raise Exception('ERROR: '+table_name.upper()+' is not exist schema.')

    os.remove(database_dir+'/'+table_name+'.json')
    os.remove(database_dir+'/'+table_name+'.csv')

    print('PASS: The schema is dropped.')

