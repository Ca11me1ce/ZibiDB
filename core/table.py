import pandas as pd
import json
import os
from ZibiDB.core.attribute import Attribute

class Table:
    # info = {}
    # need a load() outside
    def __init__(self, info):
        self.data = {}
        self.name = info['name']
        self.attrs = {}
        self.primary = info['primary']
        self.foreign = info['foreign']

        for attr in info['attrs']:
            self.attrs[attr['name']] = Attribute(attr)

    def insert(self):
        pass

    def display(self):
        pass
        
    def serialize(self):
        pass
    
    def deserialize(self):
        pass
    
    def search(self):
        pass