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
        self.attrs = {} #{name: attributeobj}
        self.primary = info['primary']
        self.foreign = info['foreign']

        for attr in info['attrs']:
            self.attrs[attr['name']] = Attribute(attr)

    def insert(self, attrs: list, data: list) -> None:
        """
        Add data into self.data as a hash table.
        TODO:
        -Put data into self.data{} as hash table. Key is prmkvalue, and value is attvalue = [].
        -Use attribute_object.typecheck to check every value, and if the value is invalid, raise error. If not put into attvalu.
        -Check primary key value, if the value already in prmkvalue, raise error.
        -Print essential information
        """
        prmkvalue = []
        attvalue = []
        self.data[prmkvalue] = attvalue
        
    def serialize(self):
        pass
    
    def deserialize(self):
        pass
    
    def search(self):
        pass