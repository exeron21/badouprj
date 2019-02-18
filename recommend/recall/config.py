import pandas as pd
import os

data_path = '../raw_data'
music_meta = os.path.join(data_path, 'music_meta')
user_profile = os.path.join(data_path, 'user_profile.data')
user_watch = os.path.join(data_path, 'user_watch_pref.sml')

cross_file = '../data/map/cross_file'

UCF_PREFIX = 'UCF_'
ICF_PREFIX = 'ICF_'
'''
middle data save path
'''
cf_train_data_path = '../data/cf_train.data'
sim_mid_data_path = '../data/sim_m_data'
user_user_sim_file = os.path.join(sim_mid_data_path, 'user_sim.data')
item_item_sim_file = os.path.join(sim_mid_data_path, 'item_sim.data')
user_feat_map_file = '../data/map/user_feat_map'
cf_rec_lst_outfile = '../data/cf_reclst.data'
model_file = '../data/map/model_file'


def gen_music_meta(nrows=None):
    return pd.read_csv(music_meta,
                       sep='\001',
                       nrows=nrows,
                       names=['music_id', 'music_name', 'music_desc', 'duration', 'music_loc', 'tags'])


def gen_user_profile(nrows=None):
    return pd.read_csv(user_profile,
                       sep=',',
                       nrows=nrows,
                       names=['user_id', 'gender', 'age', 'salary', 'province'])


# 用户id，音乐id，收听时长（单位秒），收听时间点（hour）
def gen_user_watch(nrows=None):
    return pd.read_csv(user_watch,
                       sep='\001',
                       nrows=nrows,
                       names=['user_id', 'music_id', 'stay_secs', 'hours'])
