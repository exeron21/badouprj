import math

# print(1/math.log(1+3))
import operator
def test1():
    d = dict()
    d['193'] = {'162': 0.8, '134': 0.91}
    d['186'] = {'138': 0.456}
    for u, items in sorted(d['193'].items(),
                           key=operator.attrgetter(1),
                           reverse=True)[0:10]:
        print(u, items)


test1()
