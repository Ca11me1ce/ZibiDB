import pandas as pd
import json
import os
import numpy as np
from core.attribute import Attribute

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
    
    def search(self, attr, situation, condition, gb):
        # attr: [] or *
        df = pd.DataFrame(self.data)
        if situation == 0:  # no where
            if attr == '*':
                return self.data
            else:
                res = {}
                for a in attr:
                    if a not in self.attrs:
                        raise Exception('ERROR: Attribute is not exist.')
                    res[a] = self.data[a]
                return res
        if gb:
            temp = self.group_by(condition[2], condition[3], attr, df)
        else:
            temp = df[attr]

        if situation == 1:
            return temp[df[condition[0]] == condition[1]]
        if situation == 2:
            return temp[df[condition[0]] > condition[1]]
        if situation == 3:
            return temp[df[condition[0]] >= condition[1]]
        if situation == 4:
            return temp[df[condition[0]] < condition[1]]
        if situation == 5:
            return temp[df[condition[0]] <= condition[1]]

        if situation == 6:
            return temp[condition[1] in df[condition[0]]]


    def group_by(self, situation, attr_gr, attr, df):
        gb = df.groupby(attr_gr)
        if situation == 0:
            return gb[attr].max()
        if situation == 1:
            return gb[attr].min()
        if situation == 2:
            return gb[attr].mean()
        if situation == 3:
            return gb[attr].sum()
        if situation == 4:
            return gb[attr].value_counts()


# if __name__ == '__main__':
#     df = pd.DataFrame(np.random.random(size=(3,4)))
#     df.columns = list('abcd')
#     print(df[df['c']<0.5])
