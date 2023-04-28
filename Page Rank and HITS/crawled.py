import math

from elasticsearch7 import Elasticsearch
from elasticsearch7 import helpers


class ES:

    def __init__(self):
        self.cloud_id = "e95d3890bef24fef862afd449d36201c:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQwNGQ0MjUwNmQyYjU0N2FhYTZiNDRlMmFjODEzN2ZlNCQ4NGNkN2FkOGIwMWI0MzM4YTk0YzU1NmNjM2JiNTZmYQ=="
        self.index = 'hurricane_index_2'
        self.es = Elasticsearch(request_timeout=20000, cloud_id=self.cloud_id,
                                http_auth=('elastic', 'kdV7ROr42cZbPS4HvhdPMg41'), timeout=30)
        self.in_links = {}
        self.out_links = {}

    def get_in_links(self):
        all_docs = helpers.scan(self.es,
                                index=self.index,
                                query={
                                    "query": {
                                        "match_all": {}
                                    },
                                    "_source": ["inlinks"]
                                },
                                size=2000,
                                request_timeout=30)
        for i in all_docs:
            url = i["_id"]
            if "inlinks" in i["_source"]:
                in_links = i["_source"]["inlinks"]
                self.in_links[url] = in_links
            else:
                self.in_links[url] = []

    def write_in_links(self):
        with open("in_links.txt", "a") as f:
            for url in self.in_links:
                line = "{} ".format(url)
                for l in self.in_links[url]:
                    line += "{} ".format(l)
                f.write(line)
                f.write("\n")

    def get_out_links(self):
        all_docs = helpers.scan(self.es,
                                index=self.index,
                                query={
                                    "query": {
                                        "match_all": {}
                                    },
                                    "_source": ["outlinks"]
                                },
                                size=2000,
                                request_timeout=30)
        for i in all_docs:
            url = i["_id"]
            if "outlinks" in i["_source"]:
                out_links = i["_source"]["outlinks"]
                self.out_links[url] = out_links
            else:
                self.out_links[url] = []

    def write_out_links(self):
        with open("out_links.txt", "a", encoding="utf-8") as f:
            for url in self.out_links:
                line = "{} ".format(url)
                for l in self.out_links[url]:
                    line += "{} ".format(l)
                f.write(line)
                f.write("\n")


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


my_es = ES()
my_es.get_in_links()
my_es.write_in_links()
my_es.get_out_links()
my_es.write_out_links()

in_links = {}
with open("in_links.txt", "r", encoding="utf-8") as f:
    for line in f.readlines():
        new_line = line.replace(" \n", "")
        new_line = new_line.replace("\n", "")
        new_line = new_line.split(" ")
        if len(new_line) == 1:
            in_links[new_line[0]] = []
        else:
            in_links[new_line[0]] = new_line[1:]

out_links = {}
with open("out_links.txt", "r", encoding="utf-8") as f:
    for line in f.readlines():
        new_line = line.replace(" \n", "")
        new_line = new_line.replace("\n", "")
        new_line = new_line.split(" ")
        if len(new_line) == 1:
            out_links[new_line[0]] = []
        else:
            out_links[new_line[0]] = new_line[1:]

page_rankings = ranking(in_links, out_links)
sorted_page_rankings = sorted(page_rankings.items(), key=lambda kv: kv[1], reverse=True)

file_name = "crawled_scores.txt"
crawled_pr = open(file_name, "w")
for i in range(500):
    crawled_pr.write(sorted_page_rankings[i][0] + "\t" + str(sorted_page_rankings[i][1]) + "\toutlinks: " + str(
        len(out_links[sorted_page_rankings[i][0]])) + "\tinlinks: " + str(
        len(in_links[sorted_page_rankings[i][0]])) + "\n")
crawled_pr.close()
