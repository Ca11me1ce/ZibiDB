# pattern

# action

# symbol

def parse(commandline):

    if commandline.upper() == 'EXIT':
        action = 'exit'
    else:
        raise Exception('Only accept exit command now ~~~~ : p')

    return action
