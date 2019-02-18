# encoding=utf-8

data_path = "../data/allfiles.txt"

STATUS_NUM = 4
pi = [0.0] * STATUS_NUM
pi_sum = 0.0

A = [[0.0] * STATUS_NUM] * STATUS_NUM
A_sum = [0.0] * STATUS_NUM

B = [dict()] * STATUS_NUM
B_sum = [0.0] * STATUS_NUM

file = open(data_path, 'r', encoding='utf-8')
while True:
    line = file.readline()
    if not line:
        break
    chr_list_all = []
    chr_status_all = []
    lines = line.strip().split(" ")
    for l in lines:
        chr_size = len(l)
        chr_list = [0] * chr_size
        if chr_size == 1:  # 如果词的长度为1，说明是单字词，类型为S/3
            chr_list[0] = 3
        else:
            chr_list[0] = 0
            chr_list[-1] = 2
            for i in range(1, chr_size - 1):
                chr_list[i] = 1
        chr_status_all.extend(chr_list)
        chr_list_all.extend(l)
    print(chr_list_all)
    print(chr_status_all)

    for i in range(len(chr_list_all)):
        cur_chr = chr_list_all[i]
        cur_status = chr_status_all[i]
        # 存储初始量Pi
        if i == 0:
            pi[cur_status] += 1.0
            pi_sum += 1.0
        # 存储发射统计量 B
        if cur_chr in B[cur_status]:
            B[cur_status][cur_chr] += 1.0
        else:
            B[cur_status][cur_chr] = 1.0
        B_sum[cur_status] += 1.0

        # 存储转移概率 A
        if i < len(chr_list_all) - 1:
            A[cur_status][chr_status_all[i + 1]] += 1.0
            A_sum[cur_status] += 1.0

    break  # 只读一行，然后退出循环
file.close()
