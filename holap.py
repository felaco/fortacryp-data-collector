import pandas as pd

df = pd.read_csv('bitcoin.csv', index_col=0)
res = pd.to_datetime(df.index, unit='s')
df.set_index(res, inplace=True)
i= 0