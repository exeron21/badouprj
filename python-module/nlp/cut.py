# encoding=utf-8

import jieba
from sklearn.feature_extraction.text import CountVectorizer
import token

''' 新词发现
 将句子中出现次数为2次或以上的词识别出来(在词库中不存在的词)
 然后可以将这些词写入一个自定义词典中，即完成了在很多篇文章中识别新词的目的
 参数解释：
 ngram_range(2,2) 只用二元模型匹配，即用词库中已存在的词进行两两匹配，前面一个参数若改为1，会将字典中的词也保留下来。
 后面一个参数改成3就过于复杂，变成3元模型
 min_df=1.0/len(s_list)+0.01 s_list是文章数量，后面加了一个很小的数，某个词要出现过2次或以上才能匹配
 token_pattern=r'\b\w+\b' 保留单个字
'''

s_list = ['做中文分词中文计算', '大数据', '云计算', '用结巴分词做中文分词', '云计算大数据']
print(len(s_list))
ss = [' '.join(jieba.cut(x)) for x in s_list] # ngram_vec.fit_transform接收的是字符串，不是generator数组，所以要用join
print(ss)

ngram_vec = CountVectorizer(ngram_range=(2, 2), min_df=1.0/len(s_list)+0.01, token_pattern=r'\b\w+\b')
xx = ngram_vec.fit_transform(ss)
print(xx)
print(ngram_vec.vocabulary_)

# ngram_range : tuple (min_n, max_n), default=(1, 1)
#         The lower and upper boundary of the range of n-values for different
#         n-grams to be extracted. All values of n such that min_n <= n <= max_n
#         will be used.





