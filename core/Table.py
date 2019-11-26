# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 13:45:14 2019

@author: WXS
"""

import pandas as pd

class Table:
    # info = {}
    def __init__(self, info):
        self.data = {}
        self.primary = info['primary']
        self.foreign = info['foreign']
        self.name = info['name']

        for attr in info['attrs']:
            self.data[attr] = []
        self.row = pd.DataFrame(self.data)
        
    def serialize(self):
        pass
    
    def deserialize(self):
        pass
    
    def search(self):
        pass