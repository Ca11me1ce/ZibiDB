def exit(action):
    return {
        'mainact' : 'exit'
    }

def create(action):
    if action[1].upper() == 'DATABASE':
        return{
            'mainact' : 'create',
            'type' : 'database',
            'name' : action[2].lower()
        }

    # TODO : remain to be change, since we no need to add database name before table name
    elif action[1].upper() == 'TABLE':
        name=action[2].split('.')
        return{
            'mainact' : 'create',
            'type' : 'table',
            'database_name' : name[0].lower(),
            'table_name' : name[1].lower(),
            'info' : action[3:]
        }

    else:
        raise Exception('Syntax error! Recommend : create table/database ')

def drop(action):
    if action[1].upper() == 'DATABASE':
        return{
            'mainact' : 'drop',
            'type' : 'database',
            'name' : action[2].lower()
        }

    # TODO : remain to be change, since we no need to add database name before table name
    elif action[1].upper() == 'TABLE':
        name=action[2].split('.')
        return{
            'mainact' : 'drop',
            'type' : 'table',
            'database_name' : name[0].lower(),
            'table_name' : name[1].lower()
        }
# TODO : remain to be change, since we no need to add database name before table name
def insert(action):
    if action[1].upper() == 'INTO':
        name=action[2].split('.')
        return{
            'mainact' : 'insert',
            'database_name' : name[0].lower(),
            'table_name' : name[1].lower(),
            'info' : action[3:]
        }

    else:
        raise Exception('Syntax error! Recommend : insert into ')

def select(action):
    return{
        'mainact' : 'select',
        'content' : action[1:]
    }

def save(action):
    return{
        'mainact' : 'save',
        'name' : action[-1]
    }

def use(action):
    return{
        'mainact' : 'use',
        'name' : action[-1]
    }

def show(action):
    return{
        'mainact' : 'show',
    }

main_action = {
    'EXIT' : exit,
    'CREATE' : create,
    'DROP' : drop,
    'INSERT' : insert,
    'SELECT' : select,
    'SAVE' : save,
    'SHOW' : show,
    'USE' : use,
}

def parse(commandline):
    # save the original commandline to use in error messages
    tmpcmdl = commandline

    # split commandline into list and the first word is main action
    action = commandline.split(' ')
    while True:
        if '' not in action:
            break
        action.remove('')

    mainaction = action[0].upper()
    validaction = ""
    for ac in main_action:
        validaction = validaction + " " + ac

    if mainaction not in main_action:
        raise Exception('Syntax error: ' + tmpcmdl + ' Valid action:' + validaction)
        
    act = main_action[mainaction](action)

    return act