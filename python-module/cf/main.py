import pandas as pd
from cf.user_base_new import *
'''
About movielens u.data file:
100000 ratings by 943 users on 1682 items.
Each user has rated at least 20 movies.
Users and items are numbered consecutively from 1.
The data is randomly ordered.
This is a tab separated list of user id | item id | rating | timestamp. p
'''
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
    if user_id not in d:
        d[user_id] = {item_id: rating}
    else:
        d[user_id][item_id] = rating

C = sim_user(d)
k = 10
user = '196'
sim_user_dict = sim_user(d)
# 相似用户倒排表，结构：{item_id: set(user_id)}
rank = recommend(user, d, sim_user_dict, k)
print(len(rank))
print(rank)
item_recommend = sorted(rank.items(),
                        key=operator.itemgetter(1),
                        reverse=True)[0:k]

print(item_recommend)
