path = "../data/model_file.txt"


STATUS_NUM = 4
# 初始概率
pi = [0.0 for col in range(STATUS_NUM)]
# 发射概率
A = [[0.0 for col in range(STATUS_NUM)] for row in range(STATUS_NUM)]
# 转移概率
B = [dict() for col in range(STATUS_NUM)]

f = open(path, 'r', encoding="utf-8")

line = f.readline()
tokens = line.strip().split()
for i in range(STATUS_NUM):
    pi[i] = float(tokens[i])

for i in range(STATUS_NUM):
    line = f.readline()
    tokens = line.strip().split()
    for j in range(len(tokens)):
        A[i][j] = float(tokens[j])

for i in range(STATUS_NUM):
    line = f.readline()
    tokens = line.strip().split()
    len_ = len(tokens)
    j = 0
    while j < len_ - 1:
        # print(tokens[j], tokens[j + 1])
        B[i][tokens[j]] = float(tokens[j + 1])
        j += 2

f.close()
# 待处理句子的字符个数
ch_num = 10
status_matrix = [[[0.0, 0] for i in range(ch_num)] for j in range(STATUS_NUM)]
for i in range(len(status_matrix)):
    print(status_matrix[i])

print(pi)
print(A)
for i in range(len(B)):
    print(B[i])
