import math
import operator
import pickle
import re

from elasticsearch import Elasticsearch
from nltk.tokenize import word_tokenize

es = Elasticsearch()

req = {
    'aggs': {
        "vocabSize": {
            "cardinality": {
                "field": "text"
            }
        }
    },
    "size": 0
}
resp = es.search(index="ap_dataset_new", body=req)
vocab_size = resp["aggregations"]["vocabSize"]["value"]

with open("queries_updated_aggs.txt", "r") as query_file:
    queries = {match.group(1): word_tokenize(match.group(2)) for match in
               (re.match("(\d+)\.(.*)", line) for line in query_file)}

with open('term_vectors_new.pickle', 'rb') as handle:
    return_term_vectors = pickle.load(handle)

scores = {q_id: {} for q_id, query in queries.items()}

for doc_id, term_vector in return_term_vectors.items():
    doc_len = sum(dlt['term_freq'] for dlt in term_vector["term_vectors"]["text"]["terms"].values())
    for q_id, query in queries.items():
        for word in query:
            term_info = term_vector["term_vectors"]["text"]["terms"].get(word, {})
            tf_value = term_info.get("term_freq", 0)
            p_laplace_score = (tf_value + 1) / (doc_len + vocab_size)
            score = math.log(p_laplace_score)
            scores[q_id][doc_id] = scores[q_id].get(doc_id, 0) + score

sorted_scores = {query_id: dict(sorted(document_scores.items(), key=operator.itemgetter(1), reverse=True))
                 for query_id, document_scores in scores.items()}

with open("results_laplace_aggs.txt", "w") as output_file:
    for query_id, score_dict in sorted_scores.items():
        for rank, (doc_id, score) in enumerate(score_dict.items(), start=1):
            if rank > 1000:
                break
            output_file.write(f"{query_id} Q0 {doc_id} {rank} {score} Exp\n")
