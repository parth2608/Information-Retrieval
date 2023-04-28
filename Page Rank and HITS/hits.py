import math
import os
import random
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient

cloud_id = "e95d3890bef24fef862afd449d36201c:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQwNGQ0MjUwNmQyYjU0N2FhYTZiNDRlMmFjODEzN2ZlNCQ4NGNkN2FkOGIwMWI0MzM4YTk0YzU1NmNjM2JiNTZmYQ=="
index = 'hurricane_index_2'
es = Elasticsearch(cloud_id=cloud_id, http_auth=('elastic', 'kdV7ROr42cZbPS4HvhdPMg41'), timeout=60)
ic = IndicesClient(es)

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


def preprocess_query(query):
    body = {
        "tokenizer": "standard",
        "filter": ["lowercase"],
        "text": query
    }
    response = ic.analyze(body=body, index=index)
    cleaned_queries = [list["token"] for list in response["tokens"]]
    return cleaned_queries


def write_es_scores(response_dict, name):
    file_name = name + ".txt"
    if os.path.exists(file_name):
        os.remove(file_name)
    output = open(file_name, "w")
    for q_id, response in response_dict.items():
        query_number = q_id
        rank = 1
        for doc in response["hits"]["hits"]:
            docno = doc["_id"]
            score = doc["_score"]
            new_line = query_number + " Q0 " + docno + " " + str(rank) + " " + str(score) + " Exp\n"
            output.write(new_line)
            rank += 1
    output.close()


def es_scores(query_dict):
    responses = {}
    for _id, query in query_dict.items():
        query = " ".join(query)
        query_body = {
            "size": 1000,
            "query": {
                "match": {
                    "content": query
                }
            }
        }
        response = es.search(index=index, body=query_body)
        responses[_id] = response
    return responses


def read_roots(path):
    data = []
    for line in open(path, encoding="ISO-8859-1", errors='ignore'):
        line_list = line.split()
        data += [line_list[2]]
    return data


def expand_roots(r, inlinks, outlinks):
    expanded_set = set(r)
    d = 200
    new_set = set(r)
    while len(expanded_set) < 10000:
        to_add = set([])
        for each in new_set:
            if each in outlinks:
                outs = outlinks[each]
                to_add.update(outs)
                ins = set(inlinks[each])
                if len(ins) <= d:
                    to_add.update(ins)
                else:
                    to_add.update(random.sample(ins, d))
                if len(expanded_set) + len(to_add) >= 10000:
                    break
        expanded_set.update(to_add)
        new_set = to_add
    return list(expanded_set)


def calculate_perplexity(pr_scores):
    e = 0
    for l, rank in pr_scores.items():
        e += rank * math.log(rank, 2)
    return 2 ** (-e)


def calculate_hits(b, inlinks, outlinks):
    check_set = set(b)
    hub_scores = {}
    authority_scores = {}
    for each in b:
        hub_scores[each] = 1
        authority_scores[each] = 1
    old_pp_a = calculate_perplexity(authority_scores)
    old_pp_h = calculate_perplexity(hub_scores)
    j = 0
    while j < 1:
        norm = 0
        new_authority_scores = {}
        for each in b:
            new_authority_scores[each] = 0
            if inlinks.get(each, 0):
                ins = inlinks[each]
                for q in ins:
                    if q in check_set:
                        new_authority_scores[each] += hub_scores[q]
            if new_authority_scores[each] == 0:
                new_authority_scores[each] = 1
            norm += (new_authority_scores[each]) ** 2
        norm = math.sqrt(norm)
        for key, value in new_authority_scores.items():
            new_authority_scores[key] = value / norm
        norm = 0
        new_hub_scores = {}
        for each in b:
            new_hub_scores[each] = 0
            if outlinks.get(each, 0):
                outs = outlinks[each]
                for q in outs:
                    if q in check_set:
                        new_hub_scores[each] += authority_scores[q]
            if new_hub_scores[each] == 0:
                new_hub_scores[each] = 1
            norm += (new_hub_scores[each]) ** 2
        norm = math.sqrt(norm)
        for key, value in new_hub_scores.items():
            new_hub_scores[key] = value / norm
        for each in b:
            hub_scores[each] = new_hub_scores[each]
            authority_scores[each] = new_authority_scores[each]
        new_pp_a = calculate_perplexity(authority_scores)
        new_pp_h = calculate_perplexity(hub_scores)
        if abs(old_pp_a - new_pp_a) < 1 and abs(old_pp_h - new_pp_h) < 1:
            j += 1
        old_pp_a = new_pp_a
        old_pp_h = new_pp_h
    return hub_scores, authority_scores


q = 'hurricane sandy'
query_clean = preprocess_query(q)
q_dict = {"1": query_clean}
r = es_scores(q_dict)
write_es_scores(r, "es_built_in_results")
root = read_roots("es_built_in_results.txt")
base = expand_roots(root, in_links, out_links)
h, a = calculate_hits(base, in_links, out_links)
sorted_h = sorted(h.items(), key=lambda kv: kv[1], reverse=True)
sorted_a = sorted(a.items(), key=lambda kv: kv[1], reverse=True)
file_name = "hub_scores.txt"
if os.path.exists(file_name):
    os.remove(file_name)
hub_out = open(file_name, "w")
file_name = "auth_scores.txt"
if os.path.exists(file_name):
    os.remove(file_name)
auth_out = open(file_name, "w")
for i in range(500):
    hub_out.write(sorted_h[i][0] + "\t" + str(sorted_h[i][1]) + "\n")
    auth_out.write(sorted_a[i][0] + "\t" + str(sorted_a[i][1]) + "\n")
hub_out.close()
auth_out.close()
