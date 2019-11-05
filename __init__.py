from ZibiDB.parser import parse

# database engine
class Engine:

    # lauch function: receieve a command and send to execution function.
    def start(self):

        # continue running until recieve the exit command.
       while True:
            commandline = input('ZibiDB>')
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

        if action == 'exit':
            return 'exit'

    # action functions



