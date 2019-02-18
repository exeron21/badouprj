import recall.item_base as ib
import recall.user_base_new as ub
import operator
import recall.config as conf

user_user_sim_file = conf.user_user_sim_file
item_item_sim_file = conf.item_item_sim_file
cf_rec_lst_outfile = conf.cf_rec_lst_outfile
ICF_PREFIX = conf.UCF_PREFIX
UCF_PREFIX = conf.ICF_PREFIX
# load cf train data
train = {}
with open(conf.cf_train_data_path, 'r', encoding='utf-8') as f:
    train = eval(f.read())
print('CF train data have loaded! Start compute user similarity ...')
print(train)

reclst = dict()

'''
user base
'''
# 计算用户与用户的相似度矩阵并存储
user_user_sim = ub.sim_user(train)
print('User-user similarity compute done! saving matrix ... ')
with open(user_user_sim_file, 'w', encoding='utf-8') as uuf:
    uuf.write(str(user_user_sim))

'''
对每个用户计算推荐物品集合
'''
k = 10
print('similarity matrix have saved! computing user base recommend list ... ')
for user_id in train.keys():
    rec_item_list = ub.recommend(user_id, train, user_user_sim, k)
    user_id_pre = UCF_PREFIX + user_id
    reclst[user_id_pre] = sorted(rec_item_list.items(), key=operator.itemgetter(1), reverse=True)[0:20]
print('user base done! item base starting ... ')

'''
item base
'''
item_item_sim = ib.item_sim(train)
with open(item_item_sim_file, 'w', encoding='utf-8') as iif:
    iif.write(str(item_item_sim))
for user_id in train.keys():
    rec_item_list = ib.recommendation(train, user_id, item_item_sim, k)
    user_id_pre = ICF_PREFIX + user_id
    reclst[user_id_pre] = sorted(rec_item_list.items(), key=operator.itemgetter(1), reverse=True)[0:20]


with open(cf_rec_lst_outfile, 'w', encoding='utf-8') as rcf:
    rcf.write(str(reclst))
