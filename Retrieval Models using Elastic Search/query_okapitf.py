import operator
import pickle
import re

from nltk.tokenize import word_tokenize

with open("queries_updated_aggs.txt", "r") as query_file:
    queries = {match.group(1): word_tokenize(match.group(2)) for match in
               (re.match("(\d+)\.(.*)", line) for line in query_file)}

with open('term_vectors_new.pickle', 'rb') as handle:
    return_term_vectors = pickle.load(handle)

scores = {q_id: {} for q_id, query in queries.items()}

for doc_id, term_vector in return_term_vectors.items():
    for query_id, query in queries.items():
        for word in query:
            if word not in term_vector["term_vectors"]["text"]["terms"]:
                continue
            tf = term_vector["term_vectors"]["text"]["terms"].get(word)
            term_frequency = tf['term_freq']
            doc_length = sum(term['term_freq'] for term in term_vector["term_vectors"]["text"]["terms"].values())
            avg_doc_len = term_vector["term_vectors"]["text"]["field_statistics"]["sum_ttf"] / \
                          term_vector["term_vectors"]["text"]["field_statistics"]["doc_count"]
            okapi_tf = term_frequency / (term_frequency + 0.5 + 1.5 * (doc_length / avg_doc_len))
            scores[query_id][doc_id] = scores.get(query_id, {}).get(doc_id, 0) + okapi_tf

sorted_scores = {query_id: dict(sorted(document_scores.items(), key=operator.itemgetter(1), reverse=True))
                 for query_id, document_scores in scores.items()}

with open("results_okapitf_aggs.txt", "w") as output_file:
    for query_id, score_dict in sorted_scores.items():
        for rank, (doc_id, score) in enumerate(score_dict.items(), start=1):
            if rank > 1000:
                break
            output_file.write(f"{query_id} Q0 {doc_id} {rank} {score} Exp\n")
