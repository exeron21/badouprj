import operator


def item_sim(d):
    # 1. 计算物品与物品相似度矩阵
    C = dict()
    N = dict()
    for u, items in d.items():
        for i in items:
            # item拥有的user数据量
            if N.get(i, -1) == -1:
                N[i] = 0
            N[i] += 1
            if C.get(i, -1) == -1:
                C[i] = dict()
            for j in items:
                if i == j:
                    continue
                elif C[i].get(j, -1) == -1:
                    C[i][j] = 0
                C[i][j] += 1
                # break

    # print(C)
    # 计算最终相似度矩阵
    W = dict()
    for i, related_items in C.items():
        if W.get(i,-1)==-1:
            W[i] = dict()
        for j, cij in related_items.items():
            if W[i].get(j,-1)==-1:
                W[i][j] = 0
            W[i][j] += 2 * cij / ((N[i] + N[j]) * 1.0)
    return W


def recommendation(d, user_id, C, k):
    rank = dict()
    Ru = d[user_id]
    # print('196用户打分过的物品：',Ru)
    for i, rating in Ru.items():
        # print(i,'相似的物品集合top10：',sorted(C[i].items(),key=lambda x:x[1],reverse=True)[0:10])
        # break
        for j, sim in sorted(C[i].items(),
                             key=operator.itemgetter(1), reverse=True)[0:k]:
            # 过滤这个user已经打分过的item
            if j in Ru:
                continue
            elif rank.get(j, -1) == -1:
                rank[j] = 0
            rank[j] += sim * rating
    return rank

