import pandas as pd

music_meta_data = pd.read_csv('music_meta',
            sep='\001',
            nrows=10,
            names=['music_id', 'music_name', 'music_desc', 'music_duration', 'location', 'tags'])
user_profile_data = pd.read_csv('user_profile.data',
            sep=',',
            nrows=10,
            names=['user_id', 'gender', 'age', 'salary', 'location'])
# 00ea9a2fe9c6810aab440c4d8c050000,女,26-35,20000-100000,江苏p
print(music_meta_data.head())
print(user_profile_data.head())
