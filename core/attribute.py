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
        
    def check(self, value):
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
            
            