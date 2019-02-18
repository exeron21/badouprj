import math
import pandas as pd

df = pd.read_csv("../data/u.data",
                 sep='\t',
                 # nrows=10000,
                 names=['user_id', 'item_id', 'rating', 'timestamp'])

d = dict()
for _, row in df.iterrows():
    user_id = str(row['user_id'])
    item_id = str(row['item_id'])
    rating = row['rating']
    # timestamp = row['timestamp']
    if d.get(user_id, -1) == -1:
        d[user_id] = {item_id: rating}
    else:
        d[user_id][item_id] = rating

# 1. 计算物品与物品之间的相似度矩阵
N = dict()
C = dict()
for u, items in d.items():
    for i in items:
        # item拥有的user数据量
        if N.get(i, -1) == -1:
            N[i] = 0
        N[i] += 1
        if C.get(i, -1) == -1:
            C[i] = dict()
        for j in items:
            if i <= j:
                continue
            if C[i].get(j, -1) == -1:
                C[i][j] = 0
            C[i][j] += 1

for i, related_items in C.items():
    for j, cij in related_items.items():
        C[i][j] += 2 * cij / ((N[i] + N[j]) * 1.0)

user_id = "196"
Ru = d[user_id]
rank = dict()
# print('与', user_id, '打分过的物品', Ru)
for i, rating in Ru.items():
    # print(i, '相似的物品集合top10: ', sorted(C[i].items(), key=lambda x:x[1], reverse=True)[0:10])
    for j, sim in sorted(C[i].items(), key=lambda x:x[1], reverse=True)[0:10]:
        # 过滤这个user已经打过分的item
        if Ru.get(j, -1) != -1:
            continue
        elif rank.get(j, -1) == -1:
            rank[j] = 0
        rank[j] += sim * rating

print(user_id, '用户基于物品相似度推荐list:')
print(sorted(rank.items(), key=lambda x: x[1], reverse=True)[0:10])
