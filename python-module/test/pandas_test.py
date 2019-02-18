import pandas as pd
import json

df1 = pd.read_csv("text", header=0)
df2 = df1.fillna(0)
cols = df1.columns.values.tolist()

for idx, df in df2.iterrows():
    data = {}
    for col in cols:
        data[col] = df[col]
    js = json.dumps(data)
    print js
