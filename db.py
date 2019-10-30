from parser1 import PARSER

def _db(_sql=None):
    '''
        Function: Get sql from GUI, and pass it to paser
    '''
    print('__Start DBMS__')

    # TODO: Get sql string here

    # Pass sql string to parser
    PARSER.parser(_sql)

    print('__End DBMS__')

    pass


if __name__ == "__main__":
    _db()
    pass