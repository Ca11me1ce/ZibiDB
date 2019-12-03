import pandas as pd

l1 = [[1, 'a'], [2, 'bb'], [3, 'a'], [5, 'a']]
l2 = ['id', 'name']
df = pd.DataFrame(l1, columns=l2)
print(df.columns.tolist())