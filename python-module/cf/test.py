# 3 39 92 float
# print(3/(39*92)*1.0)
# print(2*3/(39+92)*1.0)
import pandas as pd

# data_file = '../../data/data.txt'
data_file = '../../data/rating_mini'

df = pd.read_csv(data_file,
                 sep='\t',
                 # nrows=3,
                 names=['user_id', 'item_id', 'rating'])

d = dict()
for _, records in df.iterrows():
    user_id = str(records['user_id'])
    item_id = str(records['item_id'])
    rating = records['rating']
    if user_id not in d.keys():
        d[user_id] = {item_id: rating}
    else:
        d[user_id][item_id] = rating
for i in d:
    print(i, '===', d[i])
# {item_id : {user_id : rating}}
w = dict()
for i, items in d.items():
    for j in items.keys():
        if w.get(j, -1) == -1:
            w[j] = dict()
        w[j][i] = items[j]
for i in w:
    print(i, '===', w[i])

# 生成正排表： {user_id: {item_id: rating}}
def user_similarity(d):
    size = 0
    w = dict()
    for u in d.keys():
        for v in d.keys():
            if u == v:
                continue
            if u not in w.keys():
                w[u] = dict()
            seta = set(d[u])
            setb = set(d[v])
            w[u][v] = 2.0 * len(seta & setb) / (len(seta) + len(setb))

    for i in w.keys():
        print(i, '==', w[i])
