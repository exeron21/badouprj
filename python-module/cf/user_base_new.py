import math
import operator


#  1. 获得用户和用户之间的相似度
#  1.1 正常逻辑的用户相似度计算（计算量大，需要优化）
def user_normal_similarity(d):
    w = dict()
    for u in d.keys():
        for v in d.keys():
            if u == v:
                continue
            if u not in w:
                w[u] = dict()
            w[u][v] = len(set(d[u]) & set(d[v]))
            w[u][v] = 2.0 * len(set(d[u]) & set(d[v])) / (len(set(d[u])) + len(set(d[v])))

    print(w[196])
    print('all user cnt: ', len(w.keys()))  # 这句话打印出来用户个数943
    print('user 196 sim user cnt: ', len(w['196']))  # 打印和某个用户相关联的用户数量942
    # 意思就是所有的用户两两相计算了一次，大大浪费时间
    # 所以这种计算方式不行，要优化。
    # 优化成倒排索引的方式：{item_id: {user_id: rating}}这种方式


def sim_user(user_item_dict):
    '''
    :param user_item_dict: 用户:商品:打分数据，结构为{user_id:{item_id:rating}}
    :return: user_user_sim_dict: 商品:用户:相似度，结构为{user_id:{user_id:sim}}
    '''
    # 1.2 优化计算用户与用户之间的相似度,数据结构改成{item_id: set(user_id)}这种方式
    # d的结构是：{user_id: {item_id: rating}}
    item_users = dict()
    for u, items in user_item_dict.items():
        for i in items.keys():  # 因为这里不需要rating，所以遍历items.keys而不是items
            if i not in item_users:
                item_users[i] = set()
            item_users[i].add(u)

    # print(item_users['257'])
    # print(len(item_users['257']))

    i_pop = 1/math.log(1+len(item_users[i]))
    # 计算用户共同的items数量
    C = dict()  # 两个用户相同的items数量
    N = dict()  # 用户的items数量
    for item_id, users in item_users.items():
        for user in users:
            if N.get(user, -1) == -1:
                N[user] = 0
            N[user] += 1
            if C.get(user, -1) == -1:
                C[user] = dict()
            for v in users:
                if user == v:
                    continue
                if C[user].get(v, -1) == -1:
                    C[user][v] = 0
                C[user][v] += 1
                '''
                有些物品很热门，如果不处理一下的话推荐会不合理
                使用下面这种方式，可以将热门商品的权重降低，不那么热门的商品权重提高
                '''
                # C[user][v] += 1/math.log(1+len(item_users[i]))
    # W的结构是:{user_id : {user_id: sim}} 用户与用户间的相似度
    for u, users in C.items():
        # if u not in W:
        #     W[u] = dict()
        for v, cuv in users.items():
            # 两种不同的计算相似度的方法
            C[u][v] = 2.0 * cuv / (N[u] + N[v])
            # W[u][v] = cuv/math.sqrt(N[u] * N[v]) * 1.0
    # user_ = '473'
    # print(W[user_])
    # print('all user cnt : ', len(W.keys()))
    # print('user ', user_, ' sim user cnt : ', len(W[user_]))
    return C


# 给new_user推荐物品
def recommend(user, d, sim_user_dict, k):
    # 获得用户相似度dict，结构：{user_id:{user_id:sim_score}}
    rank = dict()
    # d 的结构：{user_id: {item_id: rating}}
    # 用户评论过的电影
    interacted_items = d[user].keys()
    # 取出和new_user最相似的前10个用户:
    for v, cuv in sorted(sim_user_dict[user].items(),
                         key=operator.itemgetter(1),
                         reverse=True)[0:k]:
        for i, rating in d[v].items():
            if i in interacted_items:
                continue
            elif rank.get(i, -1) == -1:
                rank[i] = 0
            rank[i] += cuv * rating
    return rank
