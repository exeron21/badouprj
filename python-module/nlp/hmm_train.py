import math
data_path = '../data/allfiles.txt'
mod_path = '../data/model_file.txt'


def get_word_ch(word):
    ch_lst = []
    i = 0
    word_len = len(word)
    while i < word_len:
        ch_lst.append(word[i])
        i += 1
    return ch_lst


# 一、初始化模型参数
# 其中S状态为:B,M,E,S  S状态大小M=4
STATUS_NUM = 4

# 1.初始概率
pi = [0.0 for col in range(STATUS_NUM)]
pi_sum = 0.0

# 2.状态转移概率 alpha ：M*M矩阵
A = [[0.0 for col in range(STATUS_NUM)] for row in range(STATUS_NUM)]
# print(A)
A_sum = [0.0 for col in range(STATUS_NUM)]

# 3.发射概率 b
# [B:{'我'：cnt}]
B = [dict() for col in range(STATUS_NUM)]
B_sum = [0.0 for col in range(STATUS_NUM)]

# 打开文件，读取每一行
f_txt = open(data_path, 'r', encoding='utf-8')

while True:
    line = f_txt.readline()
    # print(line)
    if not line:
        break
    line = line.strip()
    if len(line) < 1:
        continue

    words = line.split()
    # print(words)
    # break
    ch_lst = []
    status_lst = []
    # 获取一句话中每个字符对应的B,M,E,S [0,1,2,3]状态
    # word = words[0]
    # cur_ch_lst = get_word_ch(word)
    # print(cur_ch_lst)
    # break
    for word in words:  # words是每一行的词的列表
        cur_ch_lst = get_word_ch(word)  # 截取词中的每个字
        cur_ch_num = len(cur_ch_lst)  # 词中有多少个字

        # 初始化字符状态0
        cur_status_lst = [0 for ch in range(cur_ch_num)]  # 生成一个和字数相同长度的列表
        # S
        if cur_ch_num == 1:
            cur_status_lst[0] = 3  # 如果列表长度为1，说明是单个字符S: 3
        else:
            # 否则都是词,首个字为B，尾部为E，其他全为M
            # 标识B：0
            cur_status_lst[0] = 0
            # 标识E：2
            cur_status_lst[-1] = 2
            # 中间全是M：1
            for i in range(1, cur_ch_num-1):
                cur_status_lst[i] = 1
        # 一行的所有word放到ch_lst，状态放到status_lst
        ch_lst.extend(cur_ch_lst)
        status_lst.extend(cur_status_lst)
    # 总结：
    # ch_lst 中文字符序列 ['中','国','人','物','火','锅']
    # status_lst 字符序列对应的状态序列 [0,2,0,2,0,2]
    # print(ch_lst)
    # print(status_lst)
    # break

    for i in range(len(ch_lst)):
        cur_status = status_lst[i]
        cur_ch = ch_lst[i]
        # 存储初始量 Pi
        if i == 0:  # 每句话的第一个字的状态就是初始量
            pi[cur_status] += 1.0  # pi[0] = 1.0
            pi_sum += 1.0
        # 存储发射统计量 B
        if cur_ch in B[cur_status]:
            B[cur_status][cur_ch] += 1.0
        else:
            B[cur_status][cur_ch] = 1.0
        B_sum[cur_status] += 1.0

        # 存储状态转移统计量 A
        if i+1 < len(ch_lst)-1:
            A[cur_status][status_lst[i+1]] += 1.0
            A_sum[cur_status] += 1.0

f_txt.close()

# 将统计结果转化成概率形式
for i in range(STATUS_NUM):
    # pi
    pi[i] /= pi_sum  # [1/10,3/10,2/10,4/10] sum=10
    # A
    for j in range(STATUS_NUM):
        A[i][j] /= A_sum[i]
    # B
    for ch in B[i]:
        B[i][ch] /= B_sum[i]

# 存储模型-> 模型文件：将概率转化成log形式
f_mod = open(mod_path, 'wb')

# pi向量转化成log之后写入文件
for i in range(STATUS_NUM):
    if pi[i] != 0.0:
        log_p = math.log(pi[i])
    else:
        log_p = 0.0
    f_mod.write(str(log_p).encode()+' '.encode())
f_mod.write('\n'.encode())

# A转移矩阵
for i in range(STATUS_NUM):
    for j in range(STATUS_NUM):
        if A[i][j] != 0.0:
            log_p = math.log(A[i][j])
        else:
            log_p = 0.0
        f_mod.write((str(log_p)+' ').encode())
    f_mod.write('\n'.encode())

# 发射概率
for i in range(STATUS_NUM):
    for ch in B[i]:
        if B[i][ch] != 0.0:
            log_p = math.log(B[i][ch])
        else:
            log_p = 0.0
        f_mod.write((str(ch)+' '+str(log_p) + ' ').encode())
    f_mod.write('\n'.encode())
f_mod.close()
