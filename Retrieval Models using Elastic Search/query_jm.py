import math
import pickle
import re
from operator import itemgetter

from elasticsearch import Elasticsearch
from nltk import word_tokenize

es = Elasticsearch()

req = {
    'aggs': {
        "vocab_size": {
            "cardinality": {
                "field": "text"
            }
        }
    },
    "size": 0
}
resp = es.search(index="ap_dataset_new", body=req)
vocab_size = resp["aggregations"]["vocab_size"]["value"]

with open("queries_updated_aggs.txt", "r") as query_file:
    queries = {match.group(1): word_tokenize(match.group(2)) for match in
               (re.match("(\d+)\.(.*)", line) for line in query_file)}

with open('term_vectors_new.pickle', 'rb') as handle:
    return_term_vectors = pickle.load(handle)

terms_stats_dict = {
    term: {"doc_freq": info["doc_freq"], "ttf": info["ttf"]}
    for tv in return_term_vectors.values()
    for term, info in tv.get("term_vectors", {}).get("text", {}).get("terms", {}).items()
}

scores = {q_id: {} for q_id, query in queries.items()}

for doc_id, term_vector in return_term_vectors.items():
    terms = term_vector["term_vectors"]["text"]["terms"]
    doc_len = sum(term['term_freq'] for term in terms.values())
    for q_id, query in queries.items():
        for word in query:
            tf = terms.get(word, {'term_freq': 0})['term_freq']
            ttf = terms_stats_dict.get(word, {'ttf': 100})['ttf']
            lambda_val = 1.1
            calc1 = lambda_val * (tf / doc_len) if tf else 0
            calc2 = (lambda_val - 1) * (ttf / vocab_size)
            p_jm_score = calc1 + calc2
            score = math.log(p_jm_score)
            scores[q_id][doc_id] = scores[q_id].get(doc_id, 0) + score

sorted_scores = {query_id: dict(sorted(document_scores.items(), key=itemgetter(1), reverse=True)) for
                 query_id, document_scores in scores.items()}

with open("results_jm_aggs.txt", "w") as output_file:
    for query_id, score_dict in sorted_scores.items():
        for rank, (doc_id, score) in enumerate(score_dict.items(), start=1):
            if rank > 1000:
                break
            output_file.write(f"{query_id} Q0 {doc_id} {rank} {score} Exp\n")
