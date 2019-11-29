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
        # TODO: typecheck?
        prmkvalue = []
        attvalue = []
        if attrs==[]:
            # TODO: typecheck
            # Must enter full-attr values by order
            if len(data)!=len(self.attrs):
                raise Exception('ERROR: Full-attr values is needed')
            
            # Get primary-key values
            for _ in range(len(self.primary)):
                prmkvalue.append(data.pop(0))
            # the remaining data is attr data
            attvalue=data

            # Hash data
            self.data[tuple(prmkvalue)]=attvalue
        else:

            # Reorder by the oder of self.attrs
            attrs_dict=dict()
            for name in self.attrs.keys():
                attrs_dict[name]=None
            for i in range(len(attrs)):
                attrs_dict[attrs[i]]=data[i]

            # Get primary-key values
            for name in self.primary:
                if name in prmkvalue:
                    raise Exception('ERROR: The attr is already in hash keys.')
                if attrs_dict[name]==None:
                    raise Exception('ERROR: Primary key cannot be NULL.')
                prmkvalue.append(attrs_dict[name])

                # Pop primary-key value from the full-attr dict
                attrs_dict.pop(name)
            # The remaining data is attr data
            attvalue=list(attrs_dict.values())

            # Hash data
            self.data[prmkvalue] = attvalue
        
    def serialize(self):
        pass
    
    def deserialize(self):
        pass
    
    def search(self):
        pass