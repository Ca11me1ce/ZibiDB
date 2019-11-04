import os
import shutil

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
            else:
                raise Exception('ERROR: Only accept DROP DATABASE.')
                
        else:
            raise Exception('Only accept exit, drop database, and create database command now ~~~~ : p')

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

# CREATE TABLE database_name.table_name (column_name1 data_type, column_name2 data_type)
def createTable(database_name, table_name, table_info):
    print(database_name)
    database_dir='./ZibiDB/database/'+database_name

    # Check database
    if not os.path.exists(database_dir):
        raise Exception('ERROR: '+database_name.upper()+' is invalid database.')
    print(table_name)
    print(table_info)

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
