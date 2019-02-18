from numpy import *
import matplotlib.pyplot as plt
# 数据形式
'''
-0.017612	14.053064	0
-1.395634	4.662541	1
-0.752157	6.538620	0
-1.322371	7.152853	0
'''
data = []
label = []
fr = open("../data/testSet.txt")
for line in fr.readlines():
    line = line.strip()
    if not line:
        continue
    words = line.split("\t")
    data.append([1.0, float(words[0]), float(words[1])])
    label.append(int(words[2]))
# print('data: ', data)
# print('label: ', label)
fr.close()


def plot_function(data, label, weights=None):
    n = shape(data)[0]  # shape: (100, 3)
    x1 = []
    x2 = []
    y1 = []
    y2 = []
    for i in range(n):
        if int(label[i]) == 1:
            x1.append(data[i][1])
            y1.append(data[i][2])
        else:
            x2.append(data[i][1])
            y2.append(data[i][2])
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.scatter(x1, y1, s=30, c='red', marker='s')
    ax.scatter(x2, y2, s=30, c='green', marker='o')

    if weights != None:
        x = arange(-3.0, 3.0, 0.1)
        y = -(weights[0] + weights[1] * x) / weights[2]
    ax.plot(x, y)
    plt.xlabel("x1")
    plt.ylabel("x2")
    plt.show()


def sigmoid(t):
    return 1.0/(1 + exp(-t))


data = mat(data)
label = mat(label).transpose()  # 矩阵转置
m, n = shape(data)
alpha = 0.02  # 步长
maxIter = 1000  # 最大迭代次数
weights = ones((n, 1))
loss = None
i = 1

for k in range(maxIter):
    y = sigmoid(data * weights)
    l_wx = -label.T * log(y) - (ones((m, 1)).T - label.T) * log(1 - y)
    if loss is None:
        loss = l_wx
    elif abs(loss - l_wx) < 1e-3:
        break
    else:
        loss = l_wx
    print('step ', i, ',loss: ', l_wx)
    i += 1
    error = label - y
    grad = data.T * error
    weights += alpha * grad
print(weights.T.tolist()[0])

plot_function(data.tolist(), label, weights.T.tolist()[0])