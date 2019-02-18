import recall.config as conf
train_file = conf.cf_train_data_path


def user_music_score(nrows=100):
    '''
    将原始数据处理成cf的输入数据，类似udata中的user_id, item_id, rating
    :return: data(DataFrame)[user_id, music_id, score]
    '''
    df_user_watch = conf.gen_user_watch(nrows)
    # print(df_user_watch.head(10))
    # exit(0)
    # 取music_meta中音乐的总时长
    df_music_meta = conf.gen_music_meta()
    # pandas里面的merge和sql的join一样
    data = df_user_watch.merge(df_music_meta, how="inner", on="music_id")
    # join之后清除读进来不再用的数据
    del df_user_watch
    del df_music_meta
    # apply相当于spark rdd map操作
    data['score'] = data.apply(lambda x: float(x['stay_secs'])/float(x['duration']), axis=1)
    data = data[['user_id', 'music_id', 'score']]
    data = data.groupby(['user_id', 'music_id']).score.sum().reset_index()
    # user_avg_df = data.groupby('user_id').score.avg().reset_index()
    # 对应列降序排列
    # data = data.sort_values(by='score', ascending=False)
    # print(data.head())
    # df_cnt = df_user_watch.groupby(['user_id', 'music_id'])['staysecs'].count().reset_index()
    # df_cnt.columns = ['user_id', 'music_id', 'cnt']
    # df_cnt.sort('cnt', ascending=False).head(10)

    # music_user_data.sort('score', ascending=False).head(10)
    return data


def train_from_df(df, col_name=['user_id', 'music_id', 'score']):
    '''
    将DataFrame数据处理成cf输入的数据形式（dict）
    :param df: DataFrame数据
    :param col_name: 对应所需要取得的列名数组
    :return: 最终dict数据
    '''
    d = dict()
    for _, row in df.iterrows():
        user_id = str(row[col_name[0]])
        item_id = str(row[col_name[1]])
        rating = row[col_name[2]]
        # timestamp = row['timestamp']
        if user_id not in d:
            d[user_id] = {item_id: rating}
        else:
            d[user_id][item_id] = rating
    return d


# 主函数
if __name__ == '__main__':
    data = user_music_score()
    print(data.size)
    train = train_from_df(data)
    # 将训练数据存起来，下次使用时直接用即可
    with open(train_file, 'w', encoding='utf-8') as f:
        f.write(str(train))
