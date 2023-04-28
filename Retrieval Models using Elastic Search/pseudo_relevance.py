import math
import pickle
import re
from collections import Counter

from nltk import word_tokenize

with open("queries_parsed.txt", "r") as query_file:
    queries = {match.group(1): word_tokenize(match.group(2)) for match in
               (re.match("(\d+)\.(.*)", line) for line in query_file)}

with open('term_vectors_new.pickle', 'rb') as handle:
    return_term_vectors = pickle.load(handle)

top_docs = {}
with open("results_okapibm25.txt", encoding="ISO-8859-1", errors='ignore') as results:
    for line in results:
        q_id, _, doc_id, rank, _, _ = line.split(" ")
        rank = int(rank)
        if rank <= 1:
            top_docs.setdefault(q_id, []).append(doc_id)

query_term_calcs = {}
d = 84678
for q_id, docs in top_docs.items():
    term_calcs = {}
    for doc in docs:
        terms = return_term_vectors[doc]["term_vectors"]["text"]["terms"]
        for term, info in terms.items():
            tf = info['term_freq']
            df = info['doc_freq']
            score = math.log(1 + tf, 2) * math.log(d / df)
            term_calcs[term] = max(term_calcs.get(term, -float('inf')), score)
    query_term_calcs[q_id] = term_calcs

most_common_terms = {q_id: Counter(d).most_common(2) for q_id, d in query_term_calcs.items()}

updated_queries = {}
for q_id, query_words in queries.items():
    new_words = [word for word, _ in most_common_terms[q_id] if word not in query_words]
    updated_queries[q_id] = new_words + query_words

with open("queries_updated_pr.txt", "w") as output_file:
    for key, value in updated_queries.items():
        output_file.write(f"{key}. {' '.join(value)}\n")
