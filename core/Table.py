# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 13:45:14 2019

@author: WXS
"""

import pandas as pd

class Table:
    def __init__(self, dic):
        self.data = dic
        self.primary = False
        self.foreign = []
        self.name = ''
        self.rows = pd.DataFrame(self.data)
        
    def serialize():
        pass
    
    def deserialize():
        pass
    
    def search():
        pass