import os

# pattern

# action

# symbol

def parse(commandline):

    if commandline.upper() == 'EXIT':
        action = 'exit'
    else:
        action = commandline.split(' ')

        # CREATE
        if action[0].upper() == 'CREATE':

            # CREATE DATABASE database_name;
            if action[1].upper() == 'DATABASE':
                database = action[2].replace(';', '')
                _database='./ZibiDB/database/'

                # If the database is exist, ERROR
                if os.path.exists(_database+database):
                    raise Exception('ERROR: The file is exist already.')
                    
                # If the database is not exist, create and PASS
                elif not os.path.exists(_database+database):
                    os.makedirs(_database+database)
                    print('PASS: The database is created.')
                    return

                else:
                    raise Exception('ERROR: Invalid command.')
            else:
                    raise Exception('ERROR: Only accept CREATE DATABAS.')
                
        else:
            raise Exception('Only accept exit command now ~~~~ : p')

    return action
