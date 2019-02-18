import os
import random

file_path = './raw_data'
# 训练集和测试集的输出文件路径
TrainOutFilePath = './mid_data/data.train'
TestOutFilePath = './mid_data/data.test'

TrainingPercent = 0.8  # 划分数据集的概率，0.8为训练，0.2为test
# 打开文件，往里面写数据
train_out_file = open(TrainOutFilePath, 'w', encoding='utf-8')
test_out_file = open(TestOutFilePath, 'w', encoding='utf-8')
# 类别定义字典进行编码
label_dict = {'business': 0, 'yule': 1, 'it': 2, 'sports': 3, 'auto': 4}

WordIDDic = dict()  # {'八斗':1,'bayes':2}
WordList = []  # ['八斗'，'bayes'] len(WordList)作为新加入词的编码


def convert_data():
    i = 0
    tag = 0

    for filename in os.listdir(file_path):
        if filename.find('business') != -1:
            tag = label_dict['business']  # 0
        elif filename.find('yule') != -1:
            tag = label_dict['yule']
        elif filename.find('it') != -1:
            tag = label_dict['it']
        elif filename.find('sports') != -1:
            tag = label_dict['sports']
        else:
            tag = label_dict['auto']

        i += 1
        rd = random.random()
        outfile = test_out_file  # 默认输出文件，最终rd在0.8~1.0 test

        if rd < TrainingPercent:
            outfile = train_out_file  # rd在0~0.8 train
        if i % 100 == 0:
            print(i, 'files processed!')

        infile = open(os.path.join(file_path, filename), 'r', encoding='utf-8')
        outfile.write(str(tag) + ' ')

        content = infile.read().strip()
        words = content.replace('\n', ' ').split(' ')
        for word in words:
            if len(word.strip()) < 1:
                continue
            # 当词不在字典中，将词编码加入字典
            if word not in WordIDDic:
                WordList.append(word)
                WordIDDic[word] = len(WordList)
            outfile.write(str(WordIDDic[word]) + ' ')
        outfile.write('#' + filename + '\n')
        infile.close()

        print(i, 'files loaded!')
        print(len(WordList), 'unique words found!')


if __name__ == '__main__':
    convert_data()
    train_out_file.close()
    test_out_file.close()
