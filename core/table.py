import pandas as pd
import json
import os
import numpy as np
from ZibiDB.core.attribute import Attribute

class Table:
    # info = {}
    # need a load() outside
    def __init__(self, attrls, info):
        self.data = {}
        self.name = info['name']
        self.attrls = attrls
        self.attrs = {} #{name: attributeobj}
        self.primary = info['primary']
        self.foreign = info['foreign']
        self.uniqueattr = {} # {attribute_name: {attibute_value: primarykey_value}}

        for attr in info['attrs']:
            temp = Attribute(attr)
            self.attrs[attr['name']] = temp
            if temp.unique:
                self.uniqueattr[attr['name']] = {}

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
            if len(data)!=len(self.attrls):
                raise Exception('ERROR: Full-attr values is needed')

            # TODO: typecheck
            for i in data:
                value = data[i]
                attname = self.attrls[i]
                # typecheck()
                # If false, raise error in typecheck()
                # If true, nothing happens and continue
                # If unique, call self uniquecheck()
                if value in self.uniqueattr[attname].keys():
                    raise Exception('ERROR: Unique attribute values are in conflict' + data)
                    self.attrs[attname].typecheck(self, value)
                    # If it is not unique, raise error in the function
                    # Else, continue
            
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
            for name in self.attrls:
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
            self.data[tuple(prmkvalue)] = attvalue
    def uniquecheck(self, value):
        pass
        
    def serialize(self):
        pass
    
    def deserialize(self):
        pass
    
    def search(self, attr, situation, condition, gb):
        # attr: [] or *
        # situation: number means different conditions
        # gb: true/false have group by
        # condition: [], base on situation
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
        """
        :param situation: calculation of group by
        :param attr_gr: the attrs for group
        :param attr: attrs for calculation
        :param df: dataframe for group by
        :return: a dataframe
        """
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
