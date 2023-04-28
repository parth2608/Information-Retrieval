import math


def calculate_perplexity(pr_scores):
    e = 0
    for l, rank in pr_scores.items():
        e += rank * math.log(rank, 2)
    return 2 ** (-e)


def ranking(inlinks, outlinks):
    p = list(inlinks.keys())
    n = len(p)
    s = set([])
    for each in p:
        if len(outlinks[each]) == 0:
            s.add(each)
    pr = {}
    for each in p:
        pr[each] = 1 / n
    d = 0.85
    old_pp = calculate_perplexity(pr)
    j = 0
    while j < 4:
        sink_pr = 0
        for each in s:
            sink_pr += pr[each]
        updated_pr = {}
        for each in p:
            updated_pr[each] = (1 - d) / n
            updated_pr[each] += d * (sink_pr / n)
            for q in inlinks[each]:
                try:
                    updated_pr[each] += d * (pr[q] / len(outlinks[q]))
                except:
                    pass
        for each in p:
            pr[each] = updated_pr[each]
        new_pp = calculate_perplexity(pr)
        if abs(old_pp - new_pp) < 1:
            j += 1
        old_pp = new_pp
    return pr


def get_data(file):
    data = []
    for line in open(file, encoding="ISO-8859-1", errors='ignore'):
        data += [str(line)]
    return data


def link_graph(data_list):
    inlinks = {}
    outlinks = {}
    for link in data_list:
        link_list = link.split()
        inlinks[link_list[0]] = list(link_list[1:])
    for key, value in inlinks.items():
        for each in value:
            if each not in outlinks.keys():
                outlinks[each] = [key]
            else:
                outlinks[each].append(key)
    for key, value in inlinks.items():
        if key not in outlinks.keys():
            outlinks[key] = []
    for key, value in inlinks.items():
        inlinks[key] = list(set(value))
    for key, value in outlinks.items():
        outlinks[key] = list(set(value))
    return inlinks, outlinks


filepath = "wt2g_inlinks.txt"
raw_data = get_data(filepath)
in_links_wt2g, out_links_wt2g = link_graph(raw_data)
page_rankings_w = ranking(in_links_wt2g, out_links_wt2g)
sorted_page_rankings_w = sorted(page_rankings_w.items(), key=lambda kv: kv[1], reverse=True)
file_name = "wt2g_scores.txt"
wt2g_pr = open(file_name, "w")
for i in range(500):
    wt2g_pr.write(sorted_page_rankings_w[i][0] + "\t" + str(sorted_page_rankings_w[i][1]) + "\toutlinks: " + str(
        len(out_links_wt2g[sorted_page_rankings_w[i][0]])) + "\tinlinks: " + str(
        len(in_links_wt2g[sorted_page_rankings_w[i][0]])) + "\n")
wt2g_pr.close()
