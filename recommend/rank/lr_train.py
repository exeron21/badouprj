import recall.gen_cf_data as gcd
import recall.config as conf
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
import numpy as np

cross_file = conf.cross_file
user_feat_map_file = conf.user_feat_map_file
model_file = conf.model_file

data = gcd.user_music_score(10000)
# 定义 label 0/1规则：听完就算喜欢
data['label'] = data['score'].apply(lambda x: 1 if x >= 1.0 else 0)

'''
user_id, item_id, label
加入用户和item的信息
'''

# user信息
user_profile = conf.gen_user_profile()
# item信息
music_meta = conf.gen_music_meta()
# 关联用户和item的信息到data中
data = data.merge(user_profile, how='inner', on='user_id').merge(music_meta, how='inner', on='music_id')
print('data merge: ', data[['user_id', 'music_id', 'score', 'label']].head())

'''
特征种类
'''
user_feat = ['gender', 'age', 'salary', 'province']
item_feat = ['duration', 'location']
item_text_feat = ['music_name', 'tag']
watch_feat = ['hour', 'stay_secs', 'score']

category_feat = user_feat + ['music_loc']
continuous_feat = ['score']

labels = data['label']
del data['label']

# 特征处理
# 1.离散特征one-hot处理（word2vec -> embedding[continuous]）
# get_dummies pandas的离散化处理
df = pd.get_dummies(data[category_feat])
one_hot_columns = df.columns
# print(data[category_feat].head())
# print(df.head())

# 2.连续特征不处理直接带入，也可以使用GBDT(xgboost)叶子节点做离散化GBDT+LR
# cross feat save 交叉特征处理
data['ui-key'] = data['user_id'].astype(str) + '_' + data['music_id'].astype(str)
cross_feat_map = dict()
for _, row in data[['ui-key', 'score']].iterrows():
    cross_feat_map[row['ui-key']] = row['score']
with open(cross_file, 'w') as cf:
    cf.write(str(cross_feat_map))

# train test split[0.7, 0.3] 随机划分数据集,0.7作为训练，0.3作为预测
x_train, x_test, y_train, y_test = train_test_split(df.values, labels, test_size=0.3, random_state=2019)
lr = LogisticRegression(penalty='l2', dual=False, tol=1e-4, C=1.0,
                        fit_intercept=True, intercept_scaling=1, class_weight=None,
                        random_state=None, solver='liblinear', max_iter=100,
                        multi_class='ovr', verbose=0, warm_start=False, n_jobs=1)
model = lr.fit(x_train, y_train)
print("w:%s,b:%s" % (lr.coef_, lr.intercept_))
print("Residual sum of squares: %.2f" % np.mean((lr.predict(x_test) - y_test) ** 2))
print("score: %.2f" % (lr.score(x_test, y_test)))

'''
one-host: 
'''
feat_map = {}
for i in range(len(one_hot_columns)):
    key = one_hot_columns[i]
    feat_map[key] = i

with open(user_feat_map_file, 'w', encoding='utf-8') as f:
    f.write(str(feat_map))

# 存储模型
model_dict = {'w': lr.coef_.tolist()[0],
              'b': lr.intercept_.tolist()[0]}

with open(model_file, 'w', encoding='utf-8') as f:
    f.write(str(model_dict))
