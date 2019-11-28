#import pandas as pd
import json
import os
from ZibiDB.core.attribute import Attribute

class Table:
    # info = {}
    # need a load() outside
    def __init__(self, info):
        self.data = {}
        self.name = info['name']
        self.attrs = info['attrs']
        self.primary = info['primary']
        self.foreign = info['foreign']

        for attr in info['attrs']:
            Attr = Attribute(attr)
            self.data[Attr.name] = []
        self.row = pd.DataFrame(self.data)
        self.length = self.row.shape[0]
        
    def add_data(self, row_data):
        for k, d in zip(self.data.keys(), row_data):
            self.data[k].append(d)
        self.length += 1
        self.row.loc[self.length] = row_data
    
    def save(self, info):
        database_dir='./ZibiDB/database/'+self.database
        with open(database_dir+'/'+self.name+'.json', 'w') as f:
            json.dump({self.name: info}, f) # need more info
            f.close()
        name = database_dir+'/'+self.name+'.csv'
        self.row.to_csv(name)

        print('PASS: The schema is created.')
    
    """
    def delete(self):
        database_dir='./ZibiDB/database/'+self.database

        # Check database
        if not os.path.exists(database_dir):
            raise Exception('ERROR: '+self.database.upper()+' is invalid database.')

        if not os.path.exists(database_dir+'/'+self.name+'.json') and not os.path.exists(database_dir+'/'+table_name+'.csv'):
            raise Exception('ERROR: '+self.name.upper()+' is not exist schema.')

        os.remove(database_dir+'/'+self.name+'.json')
        os.remove(database_dir+'/'+self.name+'.csv')

        print('PASS: The schema is dropped.')
    """
        
    def serialize(self):
        pass
    
    def deserialize(self):
        pass
    
    def search(self):
        pass
