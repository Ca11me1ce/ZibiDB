import pandas as pd
import json
import os
import numpy as np
from ZibiDB.core.attribute import Attribute
from BTrees.OOBTree import OOBTree

class Table:
    # info = {}
    # need a load() outside
    def __init__(self, attrls, info):
        self.data = {}
        self.datalist = []
        self.df = pd.DataFrame()
        self.name = info['name']
        self.attrls = attrls
        self.attrs = {} #{name: attributeobj}
        self.primary = info['primary']
        self.foreign = info['foreign']
        self.uniqueattr = {} # {attribute_name: {attibute_value: primarykey_value}}
        self.index={}   #{attr: idex_name}
        self.BTree={}   #{idex_name: BTree}
        self.flag = 0

        for attr in info['attrs']:
            temp = Attribute(attr)
            self.attrs[attr['name']] = temp
            if temp.unique:
                self.uniqueattr[attr['name']] = {}

    def add_index(self, attr, idex_name):
        if attr[0] not in self.uniqueattr.keys():
            raise Exception('ERROR: The attr is not unique and cannot create index')
        # If unique:
        if attr[0] not in self.index:
            self.index[attr[0]]=idex_name
        # Get pairs {v1:p1, v2:p2,...}
        nodes=self.uniqueattr[attr[0]]

        # Create a b tree
        t=OOBTree()
        t.update(nodes)
        self.BTree[idex_name]=t
        return t
    
    def drop_index(self, idex_name):
        # TABLE is a obj
        # index name must in index attrs
        if idex_name in self.index.keys():
            del self.index[idex_name]
            del self.BTree[idex_name]
        else:
            raise Exception('ERROR: The index does not exist')
    def index_search(self, attrs, condition):
        '''
        attrs: [attr1, attr2, ...]
        condition:{attr: , value:, symbol, }
        '''
        attr=condition['attr']
        value=condition['value']
        symbol=condition['symbol']
        idex_name=self.index[attr]
        BTree=self.BTree[idex_name]

        min_key=BTree.minKey()
        max_key=BTree.max_key()

        if symbol=='=':
            pks=[BTree[value]]
            content=[]
            for pk in pks:
                content.append(self.data[pk])
        
        elif symbol=='<':
            pks=list(BTree.values(min=min_key, max=value, excludemin=False, excludemax=True)) #[pk1, pk2,...]
            content=[]
            for pk in pks:
                content.append(self.data[pk])

        elif symbol=='>':
            pks=list(BTree.values(min=value, max=max_key, excludemin=True, excludemax=False)) #[pk1, pk2,...]
            content=[]
            for pk in pks:
                content.append(self.data[pk])

        elif symbol=='<=':
            pks=list(BTree.values(min=min_key, max=value, excludemin=False, excludemax=False)) #[pk1, pk2,...]
            content=[]
            for pk in pks:
                content.append(self.data[pk])

        elif symbol=='>=':
            pks=list(BTree.values(min=value, max=max_key, excludemin=True, excludemax=False)) #[pk1, pk2,...]
            content=[]
            for pk in pks:
                content.append(self.data[pk])

        elif symbol=='<>':
            pk1=list(BTree.values(min=min_key, max=value, excludemin=False, excludemax=True)) #[pk1, pk2,...]
            pk2=list(BTree.values(min=value, max=max_key, excludemin=True, excludemax=False)) #[pk1, pk2,...]
            content=[]
            for pk in pk1:
                content.append(self.data[pk])    
            for pk in pk2:
                content.append(self.data[pk])   
        att=self.attrls[len(pks):]

        df=pd.DataFrame(content, columns=att)
        my_df=pd.DataFrame()
        for i in attrs:
            my_df[i]=df[i]
        return my_df

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

            dat = data[::]
            # Get primary-key values
            for _ in range(len(self.primary)):
                prmkvalue.append(dat.pop(0))
            # the remaining data is attr data
            attvalue=dat

            # TODO: typecheck
            for i in data:
                value = data[i]
                attname = self.attrls[i]
                # typecheck()
                # If false, raise error in typecheck()
                # If true, nothing happens and continue
                # If unique, call self uniquecheck()
                if attname in self.uniqueattr.keys():
                    if value in self.uniqueattr[attname].keys():
                        raise Exception('ERROR: Unique attribute values are in conflict!  ' + attname + " : " + str(value))
                    self.uniqueattr[attname][value] = prmkvalue
                self.attrs[attname].typecheck(value)
                # If it is not unique, raise error in the function
                # Else, continue

            # Hash data
            self.data[tuple(prmkvalue)]=attvalue
        else:
            # Reorder by the oder of self.attrs
            attrs_dict=dict()
            for name in self.attrls:
                attrs_dict[name]=None

            # Get primary-key values
            for name in self.primary:
                if name not in attrs:
                    raise Exception('ERROR: Primary key cannot be NULL.')
                prmkvalue.append(data[attrs.index(name)])

            for i in range(len(attrs)):
                value = data[i]
                attname = attrs[i]

                if attname in self.uniqueattr.keys():
                    if value in self.uniqueattr[attname].keys():
                        raise Exception('ERROR: Unique attribute values are in conflict!  ' + attname + " : " + str(value))
                    self.uniqueattr[attname][value] = prmkvalue
                #self.attrs[attname].typecheck(value)
                self.attrs[attname].typecheck(value)

                attrs_dict[attname] = value
             # Get primary-key values
            for name in self.primary:
                # Pop primary-key value from the full-attr dict
                attrs_dict.pop(name)
            # The remaining data is attr data
            attvalue=list(attrs_dict.values())

            # Hash data
            if tuple(prmkvalue) not in self.data.keys():
                self.datalist = self.datalist + [prmkvalue + attvalue]
                self.data[tuple(prmkvalue)] = attvalue
            else:
                raise Exception('ERROR: Primary key value collision')
        
    def serialize(self):
        pass
    
    def deserialize(self):
        pass
    
    def delete(self, table_name, where):
        if where == []:
            self.data = {}
            self.datalist = []
            for a in self.uniqueattr.keys():
                self.uniqueattr[a] = {}
            #self.BTree
        elif len(where) > 1:
            raise Exception('ERROR: Mutiple where conditions is coming soon')
        elif len(where) == 1:
            if where[0]['attr'] not in self.primary:
                raise Exception('ERROR: You should delete by one of the primary key!')
            else:
                if where[0]['symbol']=='=':
                    value=where[0]['value']
                    try:
                        value=int(value)
                    except:
                        pass

                    del self.data[tuple([value])]
                    self.df=self.df[self.df[where[0]['attr']]!=value]
                elif where[0]['symbol']=='<>':
                    try:
                        value=int(value)
                    except:
                        pass
                    self.data={self.data[value]}
                    self.df=self.df[self.df[where[0]['attr']]==value]

    def search(self, attr, sym, tag, condition, gb):
        # attr: [] or *
        # situation: number means different conditions
        # gb: true/false have group by
        # condition: [], base on situation
        # df = pd.DataFrame(self.datalist, columns = self.attrls)
        if self.flag == 0:
            self.df = pd.DataFrame(self.datalist, columns = self.attrls)
        symbols = {
            '=': 1,
            '>': 2,
            '>=': 3,
            '<': 4,
            '<=': 5,
            'LIKE': 6,
            'NOT LIKE': 7,
            '<>': 8
        }
        if len(sym) == 0:
            situation = 0
        else:
            situation = symbols[sym]
        if gb:
            temp = self.group_by(condition[2], condition[3], attr, df)
        else:
            temp = self.df

        if situation == 0:  # no where
            if attr == ['*']:
                return temp
            else:
                return temp.loc[:, attr]

        if situation == 1:
            if tag:
                if attr == ['*']:
                    return temp.loc[temp[condition[0]] == temp[condition[1]]]
                return temp.loc[temp[condition[0]] == temp[condition[1]], attr]
            if attr == ['*']:
                return temp.loc[temp[condition[0]] == condition[1]]
            return temp.loc[temp[condition[0]] == condition[1], attr]
        if situation == 2:
            if tag:
                if attr == ['*']:
                    return temp.loc[temp[condition[0]] > temp[condition[1]]]
                return temp.loc[temp[condition[0]] > temp[condition[1]], attr]
            if attr == ['*']:
                return temp.loc[temp[condition[0]] > condition[1]]
            return temp.loc[temp[condition[0]] > condition[1], attr]
        if situation == 3:
            if tag:
                if attr == ['*']:
                    return temp.loc[temp[condition[0]] >= temp[condition[1]]]
                return temp.loc[temp[condition[0]] >= temp[condition[1]], attr]
            if attr == ['*']:
                return temp.loc[temp[condition[0]] >= condition[1]]
            return temp.loc[temp[condition[0]] >= condition[1], attr]
        if situation == 4:
            if tag:
                if attr == ['*']:
                    return temp.loc[temp[condition[0]] < temp[condition[1]]]
                return temp.loc[temp[condition[0]] < temp[condition[1]], attr]
            if attr == ['*']:
                return temp.loc[temp[condition[0]] < condition[1]]
            return temp.loc[temp[condition[0]] < condition[1], attr]
        if situation == 5:
            if tag:
                if attr == ['*']:
                    return temp.loc[temp[condition[0]] <= temp[condition[1]]]
                return temp.loc[temp[condition[0]] <= temp[condition[1]], attr]
            if attr == ['*']:
                return temp.loc[temp[condition[0]] <= condition[1]]
            return temp.loc[temp[condition[0]] <= condition[1], attr]
        if situation == 8:
            if tag:
                if attr == ['*']:
                    return temp.loc[temp[condition[0]] != temp[condition[1]]]
                return temp.loc[temp[condition[0]] != temp[condition[1]], attr]
            if attr == ['*']:
                return temp.loc[temp[condition[0]] != condition[1]]
            return temp.loc[temp[condition[0]] != condition[1], attr]

        if situation == 6:
            if tag:
                if attr == ['*']:
                    return temp.loc[temp[condition[0]].str.contains(temp[condition[1]])]
                return temp.loc[temp[condition[0]].str.contains(temp[condition[1]]), attr]
            if attr == ['*']:
                return temp.loc[temp[condition[0]].str.contains(condition[1])]
            return temp.loc[temp[condition[0]].str.contains(condition[1]), attr]
        if situation == 7:
            if tag:
                if attr == ['*']:
                    return temp.loc[~temp[condition[0]].str.contains(temp[condition[1]])]
                return temp.loc[~temp[condition[0]].str.contains(temp[condition[1]]), attr]
            if attr == ['*']:
                return temp.loc[~temp[condition[0]].str.contains(condition[1]), attr]
            return temp.loc[~temp[condition[0]].str.contains(condition[1]), attr]


    def group_by(self, agg, attr_gr, attr, df):
        """
        :param situation: calculation of group by
        :param attr_gr: the attrs for group
        :param attr: attrs for calculation
        :param df: dataframe for group by
        :return: a dataframe
        """
        agg_funcs = {
            'MAX': 0,
            'MIN': 1,
            'AVG': 2,
            'SUM': 3,
            'COUNT': 4
        }
        situation = agg_funcs[agg]

        if attr == '*':
            raise Exception('ERROR: Invalid search.')
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

    def table_join(self, table, attr):
        df1 = pd.DataFrame(self.data)
        df2 = pd.DataFrame(table.data)
        return pd.merge(df1, df2, on=attr)

"""
if __name__ == '__main__':
    data = []
    for i in range(100):
        if i > 50:
            data.append([i,2])
        else:
            data.append([i,1])
    attr1 = {'name': 'id', 'type': 'INT', 'notnull': False, 'unique': False}
    attr2 = {'name': 'num', 'type': 'INT', 'notnull': False, 'unique': False}
    info = {'name': 'test', 'attrs': [attr1, attr2], 'primary': '', 'foreign': []}
    table = Table(['id', 'num'], info)
    table.df = pd.DataFrame(data, columns=['id', 'num'])
    res = table.search('*', '=', False, ['id', 5], False)
    print(res)
"""