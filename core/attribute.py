# -*- coding: utf-8 -*-
class Attribute:
    # dic = {name:'', type: '', constrain:[], unique: true/false}
    def __init__(self, dic):
        self.name = dic['name']
        self.type = dic['type']
        self.constrain = []
        #self.constrain = dic['constrain']
        self.notnull = dic['notnull']
        self.unique = dic['unique']

    def typecheck(self, value: any) -> bool:
        """
        Check whether value is valid
        TODO:
        -Check type by self.type
        -Check  notnull by self.notnull
        -Check unique by self.unique
        -Use constraincheck to check constrain 
        -Please manage check order wisely
        -Raise error for invalid information
        """
        if self.notnull:
            if value==None:
                raise Exception('ERROR: The value must be not null.')

        if self.type=='CAHR':
            if type(value)!=type('1'):
                raise Exception('ERROR: Invalid type.')
        elif self.type=='INT':
            if type(value)!=type(1):
                raise Exception('ERROR: Invalid type.')
        elif self.type=='FLOAT':
            if type(value)!=type(1.0):
                raise Exception('ERROR: Invalid type.')
        else:
            raise Exception('ERROR: Invalid type.')

        if self.constraincheck(value)==False:
            raise Exception('ERROR: The value do not satisfied to the constrain.')

        # TODO: Check unique
        if self.unique:
            return 'UNIQUE'

        return True


        
    def constraincheck(self, value):
        # constrain = [T/F, None/value, T/F, None/value]
        # True: >=, False >
        if self.type == 'CHAR' or self.constrain is None:
            return True
        return self.con1(value) and self.con2(value)
        
    def con1(self, value):
        if self.constrain[1] is None:
            if value >= float(self.constrain[3]):
                return (self.constrain[2] and value==self.constrain[3])
        return True
            
    def con2(self, value):
        if self.constrain[3] is None:
            if value <= float(self.constrain[1]):
                return (self.constrain[0] and value==self.constrain[1])
        return True
            
            