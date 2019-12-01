import pandas as pd

l1 = [[1, 'a'], [2, 'bb']]
l2 = ['id', 'name']
df = pd.DataFrame(l1, columns=l2)
print(df.loc[df['name'].str.contains('b')])