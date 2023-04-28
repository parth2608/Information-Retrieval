import math
import pickle
import re
from collections import Counter

from elasticsearch import Elasticsearch
from nltk import word_tokenize

es = Elasticsearch()

with open("stoplist.txt", "r") as file:
    stopwords = word_tokenize(file.read())

with open("queries_parsed.txt", "r") as query_file:
    queries = {match.group(1): word_tokenize(match.group(2)) for match in
               (re.match("(\d+)\.(.*)", line) for line in query_file)}

with open("term_vectors_new.pickle", "rb") as handle:
    return_term_vectors = pickle.load(handle)

terms_stats_dict = {}
for doc_id, tv in return_term_vectors.items():
    try:
        for term, info in tv["term_vectors"]["text"]["terms"].items():
            term = term.strip()
            if term not in terms_stats_dict:
                df = info["doc_freq"]
                ttf = info["ttf"]
                terms_stats_dict[term] = {"doc_freq": df, "ttf": ttf}
    except KeyError:
        continue

terms_to_add = {q_id: Counter() for q_id in queries}
for q_id, query_list in queries.items():
    for word in query_list:
        response = es.search(index="ap_dataset_new", body={
            "query": {
                "terms": {"text": [word]}
            },
            "aggregations": {
                "significantCrimeTypes": {
                    "significant_terms": {
                        "field": "text"
                    }
                }
            },
            "size": 0
        })
        terms = response["aggregations"]["significantCrimeTypes"]["buckets"]
        for term in terms:
            if term["key"] != word:
                terms_to_add[q_id][term["key"]] += 1

common_terms = {q_id: c.most_common(2) for q_id, c in terms_to_add.items()}

d = 84678

idf_scores = {q_id: {term: math.log(d / (terms_stats_dict.get(term[0], {"doc_freq": 100})["doc_freq"]))
                     for term in t if term[0] not in stopwords} for q_id, t in common_terms.items()}

most_common_terms = {q_id: Counter(idf).most_common(1) for q_id, idf in idf_scores.items()}

updated_queries = {q_id: [word[0]] + query_list for q_id, query_list in queries.items() for word in
                   most_common_terms[q_id] if word[0] not in query_list}

with open("queries_updated_aggs.txt", "w") as output_file:
    for key, value in updated_queries.items():
        terms = [word[0] if type(word) == tuple else word for word in value]
        output_file.write(f"{key}. {' '.join(terms)}\n")
