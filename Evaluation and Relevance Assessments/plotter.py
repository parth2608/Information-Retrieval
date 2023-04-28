import pickle
from collections import OrderedDict
from operator import itemgetter

import matplotlib.pylab as plt

handle = open('graph_details.pickle', 'rb')
data = pickle.load(handle)
handle.close()

recall_values = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
plot_data = OrderedDict()
for q, vals in data.items():
    plot_data[q] = OrderedDict()
    for i in recall_values:
        plot_data[q][i] = -1

for q, vals in data.items():
    for r, p in vals.items():
        r_r = round(r, 1)
        plot_data[q][r_r] = p

interpolated = OrderedDict()
for q, vals in plot_data.items():
    l = list(vals.items())
    for i in range(len(l)):
        n = len(l) - i
        l[i] = (l[i][0], max(l[-n:], key=itemgetter(1))[1])
    interpolated[q] = l

avgs = OrderedDict()
num_queries = 4
for each in recall_values:
    avgs[each] = 0
for q, vals in interpolated.items():
    for each in vals:
        r = each[0]
        avgs[r] += each[1]
for rec, prec in avgs.items():
    avgs[rec] = prec / num_queries

d = avgs.items()
x, y = zip(*d)
plt.plot(x, y, "-o", color='purple')
plt.xlabel("Recall")
plt.ylabel("Precision")
plt.title("Precision-Recall Plot")
plt.rcParams["figure.figsize"] = (10, 7)
plt.rcParams.update({'font.size': 24})
plt.savefig('precision-recall.png', facecolor='w')
plt.show()
