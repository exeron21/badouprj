from numpy import *
import matplotlib.pyplot as plt

data = open("../data/testSet.txt")
label = 1
n = shape(data)[0]
x1 = []
x2 = []
y1 = []
y2 = []
for i in range(n):
    if int(label[i]) == 1:
        x1.append(data[i, 1])
        y1.append(data[i, 2])
    else:
        x2.append(data[i, 1])
        y2.append(data[i, 2])

data.close()

fig = plt.figure()
ax = fig.add_subplot(111)
ax.scatter(x1, y1, c='red', maker='o')
ax.scatter(x2, y2, c='green', maker='o', marker='s', edgecolor='gray') # marker=s (square)
alpha = 0.001 # 步长
maxIter = 500 # 最大迭代次数
data = mat(data)

label = mat(label).transpose()
m, n = shape(data)
weights = ones((n, 1))


def sigmoid(t):
    return 1.0/(1+exp(-t))


for k in range(maxIter):
    sig = sigmoid(data * weights) # 100 * 1
    error = label - sig
    grad = -data.T * error
    weights = weights - alpha * grad
