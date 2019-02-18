import recall.config as conf
import math

user_id = '123'
# step1: 载入特征处理
# load user and item category feature
with open(conf.user_feat_map_file, 'r', encoding='utf-8') as f:
    category_feat_dict = eval(f.read())

# load cross feature
with open(conf.cross_file, 'r', encoding='utf-8') as cross_file:
    cross_feat_dict = eval(f.read())

# step 2: 载入model
# load LR model
with open(conf.model_file, 'r', encoding='utf-8') as model_file:
    model_dict = eval(f.read())
w = model_dict['w']
b = model_dict['b']

# step 3: match/recall(协同过滤，召回候选集)
rec_item_all = dict()
# 3.1 CF
# 3.1.1 user base recall
with open(conf.cf_rec_lst_outfile, 'r', encoding='utf-8') as f:
    cf_rec_lst = eval(f.read())
key = conf.UCF_PREFIX + user_id
ucf_rec_lst = cf_rec_lst[key]

for item, score in ucf_rec_lst:
    rec_item_all[item] = float(score)

