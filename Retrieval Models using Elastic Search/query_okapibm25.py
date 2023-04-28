import math
import operator
import pickle
import re
from collections import Counter, defaultdict

from nltk import word_tokenize

with open("queries_updated_aggs.txt", "r") as query_file:
    queries = {match.group(1): word_tokenize(match.group(2)) for match in
               (re.match("(\d+)\.(.*)", line) for line in query_file)}

with open('term_vectors_new.pickle', 'rb') as handle:
    return_term_vectors = pickle.load(handle)

scores = defaultdict(dict)
queries_counter = defaultdict(Counter)
for q_id, query in queries.items():
    for word in query:
        queries_counter[q_id][word] += 1

for doc_id, term_vector in return_term_vectors.items():
    for q_id, query in queries.items():
        for word in query:
            try:
                if word in term_vector["term_vectors"]["text"]["terms"].keys():
                    term_frequency = term_vector["term_vectors"]["text"]["terms"].get(word, 0)
                    total_docs = len(return_term_vectors)
                    document_frequency = term_frequency['doc_freq']
                    tf_value = term_frequency['term_freq']
                    doc_len = sum(map(lambda doc_length_term: doc_length_term['term_freq'],
                                      term_vector["term_vectors"]["text"]["terms"].values()))
                    avg_doc_len = term_vector["term_vectors"]["text"]["field_statistics"]["sum_ttf"] / \
                                  term_vector["term_vectors"]["text"]["field_statistics"]["doc_count"]
                    tf_query = queries_counter[q_id][word]
                    k_1 = 1.2
                    k_2 = 1.2
                    b = 0.75
                    calc1_num = total_docs + 0.5
                    calc1_den = document_frequency + 0.5
                    calc1 = math.log((calc1_num / calc1_den), 2)
                    calc2_num = tf_value + (k_1 * tf_value)
                    cal2_den = tf_value + k_1 * ((1 - b) + (b * (doc_len / avg_doc_len)))
                    calc2 = calc2_num / cal2_den
                    calc3_num = tf_query + (k_2 * tf_query)
                    calc3_den = tf_query + k_2
                    calc3 = calc3_num / calc3_den
                    okapi_BM25_score = calc1 * calc2 * calc3
                    if doc_id not in scores[q_id].keys():
                        scores[q_id][doc_id] = okapi_BM25_score
                    else:
                        scores[q_id][doc_id] += okapi_BM25_score
            except KeyError:
                continue

sorted_scores = {query_id: dict(sorted(document_scores.items(), key=operator.itemgetter(1), reverse=True))
                 for query_id, document_scores in scores.items()}

with open("results_okapibm25_aggs_new.txt", "w") as output_file:
    for query_id, score_dict in sorted_scores.items():
        for rank, (doc_id, score) in enumerate(score_dict.items(), start=1):
            if rank > 1000:
                break
            output_file.write(f"{query_id} Q0 {doc_id} {rank} {score} Exp\n")
