import math
import operator
import pickle
import re

from nltk import word_tokenize

with open("queries_updated_aggs.txt", "r") as query_file:
    queries = {match.group(1): word_tokenize(match.group(2)) for match in
               (re.match("(\d+)\.(.*)", line) for line in query_file)}

with open('term_vectors_new.pickle', 'rb') as handle:
    return_term_vectors = pickle.load(handle)

scores = {q_id: {} for q_id, query in queries.items()}

for doc_id, term_vector in return_term_vectors.items():
    for q_id, query in queries.items():
        for word in query:
            if word in term_vector["term_vectors"]["text"]["terms"]:
                tf = term_vector["term_vectors"]["text"]["terms"][word]
                term_frequency = tf['term_freq']
                doc_len = sum(term['term_freq'] for term in term_vector["term_vectors"]["text"]["terms"].values())
                avg_doc_len = term_vector["term_vectors"]["text"]["field_statistics"]["sum_ttf"] / \
                              term_vector["term_vectors"]["text"]["field_statistics"]["doc_count"]
                okapi_tf_score = term_frequency / (term_frequency + 0.5 + 1.5 * (doc_len / avg_doc_len))
                total_docs = len(return_term_vectors)
                doc_frequency = tf['doc_freq']
                tf_idf_score = okapi_tf_score * math.log((total_docs / doc_frequency), 2)
                scores[q_id][doc_id] = scores[q_id].get(doc_id, 0) + tf_idf_score

sorted_scores = {query_id: dict(sorted(document_scores.items(), key=operator.itemgetter(1), reverse=True))
                 for query_id, document_scores in scores.items()}

with open("results_tfidf_aggs.txt", "w") as output_file:
    for query_id, score_dict in sorted_scores.items():
        for rank, (doc_id, score) in enumerate(score_dict.items(), start=1):
            if rank > 1000:
                break
            output_file.write(f"{query_id} Q0 {doc_id} {rank} {score} Exp\n")
