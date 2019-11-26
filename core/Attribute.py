# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 14:05:07 2019

@author: WXS
"""

class Attribute:
    # dic = {name:'', type: '', constrain:[], unique: true/false}
    def __init__(self, dic):
        self.name = dic['name']
        self.type = dic['type']
        self.constrain = dic['constrain']
        self.unique = dic['unique']
        
    def check(self):
        pass