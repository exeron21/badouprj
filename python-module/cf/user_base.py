import pandas as pd
import math

data_file = '../../data/u.data'
df = pd.read_csv(data_file,
                 sep='\t',
                 # nrows=100000,
                 names=['user_id', 'item_id', 'rating', 'timestamp'])
# 先将数据读到字典中，数据结构：{user_id: {item_id: rating}} ，也就是每个人给每件商品的打分
d = dict()
for _, row in df.iterrows(): # _ 是pandas的dataframe中的序号
    user_id = str(row['user_id'])
    item_id = str(row['item_id'])
    rating = row['rating']
    if user_id not in d.keys():
        d[user_id] = {item_id: rating}  # 可以用这种方式生成字典
    else:
        d[user_id][item_id] = rating
# print(len(d))


'''
用户相似度如何定义？给相同的item打过分，这两个用户就是相似用户
用户a给39个item打过分，b给58个item打过分，两者交集有5个
用户ab的相似度就是2.0 * 5 / (39 + 58)
'''


# 这个函数生成的w是一个大表，包含了每个用户与其它用户的相似度，数据量和计算量会很大，难以处理，因此要优化为倒排结构
def user_normal_similarity(d):
    w = dict()
    f = open("../../data/sim_user.txt", "w")
    for u in d.keys():
        for v in d.keys():
            if u not in w:  # w[u]不存在时，必须初始化一下，否则w[u][v]会报错
                w[u] = dict()
            if u == v:
                continue
            seta = set(d[u])  # 把dict转换成set，是对keys进行转换，会丢失values
            setb = set(d[v])
            w[u][v] = 2.0 * len(seta & setb) / (len(seta) + len(setb))  # 如果这里w[u]不存在，会报错
        f.write(u + ' ::: ' + str(w[u]) + '\n')
    f.close()
    return w


# 将user->item的结构转换为item->user
# item_users的结构: {item_id : {user_id : rating}}
def user_sim(d):
    item_users = dict()
    for u, items in d.items():
        for i in items.keys():
            if i not in item_users:
                item_users[i] = set()
            item_users[i].add(u)
    # 商品热度
    i_pop = 1/ math.log(1 + len(item_users[i]))
    # print(item_users['257'])
    # print(len(item_users['257']))

    # 计算用户共同item的数量
    C = dict()  # 存放统计用户与用户共同item数量
    N = dict()  # 存放用户下的item数量

    # item_users的结构: {item_id : {user_id : rating}}
    for i, users in item_users.items():
        users2 = users
        for u in users:
            if N.get(u, -1) == -1:
                N[u] = 0
            N[u] += 1  # 此处是+=，之前必须要初始化否则报错，如果是N[u] = 1则不需要初始化
            if C.get(u, -1) == -1:
                C[u] = dict()
            for v in users2:
                if u == v:
                    continue
                if C[u].get(v, -1) == -1:
                    C[u][v] = 0
                C[u][v] += 1
                # C[u][v] += 1/math.log(1+len(item_users[i]))
    del item_users

    # print('777777777')
    for u, sim_users in C.items():
        for v, cuv in sim_users.items():
            C[u][v] = cuv / math.sqrt(N[u] * N[v])

    # print('8222222222')
    print(C['22'])
    print('all user cnt: ', len(C.keys()))
    print('user_186 sim user cnt: ', len(C['186']))
    return C

# 怎么解决热门商品问题?
# 比如“可乐”这个商品非常热门，有90%的用户都购买了可乐，“网球拍”就不那么热门，只有1%的用户购买了
# 那么对于同时购买了可乐和网球拍的两个用户，可乐和网球拍的权重就不应该一样
# 如何调整可乐和网球拍的权重（热度处理）？
# 有一种方式就是：权重 = 1/log(1+ 商品热度)


user = '196'
items = list()
rank = dict()
# 用户评论过的电影
interacted_iterms = d[user].keys()


def recommend():
    return ""

