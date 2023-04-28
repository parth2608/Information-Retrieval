import math
import re
import zlib
from collections import OrderedDict, Counter

from nltk.stem import PorterStemmer

STOPWORDS_FILE_PATH = "stoplist.txt"
QUERIES_FILE_PATH_MODIFIED = "queries_modified.txt"
QUERIES_FILE_PATH_UNMODIFIED = "queries_unmodified.txt"
CATALOG_FILE_PATH = "full_catalog.txt"
INVERTED_INDEX_FILE_PATH = "full_index.txt"
STEMMING_ENABLED = True
if STEMMING_ENABLED:
    DOC_HASHES_FILE_PATH = "stemmed_hashes_file.txt"
    stemmer = PorterStemmer()
else:
    DOC_HASHES_FILE_PATH = "hashes_file.txt"


def get_stopwords(stopwords_file_path):
    with open(stopwords_file_path, encoding="ISO-8859-1", errors='ignore') as f:
        stopwords = [line.strip() for line in f]
    return stopwords


def analyze_query(query, stopwords):
    cleaned_terms = []
    terms = re.findall(r"\w+|[^\w\s]|[\n\r]+", query, re.UNICODE)
    for term in terms:
        if term.isalnum() and term not in stopwords:
            if STEMMING_ENABLED:
                term = stemmer.stem(term)
            cleaned_terms.append(term)
    return cleaned_terms


def read_queries(queries_file_path, stopwords):
    queries = {}
    with open(queries_file_path, encoding="ISO-8859-1", errors='ignore') as f:
        for line in f:
            query_id, query_text = line.split(".", 1)
            query_id = query_id.strip()
            query_text = query_text.strip()
            cleaned_query = analyze_query(query_text, stopwords)
            queries[query_id] = cleaned_query
    return queries


def read_catalog(catalog_path):
    catalog = OrderedDict()
    with open(catalog_path, encoding="ISO-8859-1", errors='ignore') as f:
        for line in f:
            key, offset, length = map(str.strip, line.split(':'))
            catalog[key] = (offset, length)
    return catalog


def parser(doc_info):
    doc_dict = {}
    for doc in doc_info.split('|'):
        if not doc:
            continue
        doc_id, positions = doc.split(':')
        doc_dict[doc_id] = positions.split(',') if positions else positions
    return doc_dict


def read_hashes(doc_hashes_path):
    doc_hashes = OrderedDict()
    with open(doc_hashes_path, encoding="ISO-8859-1", errors='ignore') as f:
        for line in f:
            fields = line.split()
            doc_id, doc_hash, doc_length = fields[1].split('|')
            doc_hashes[doc_id] = (doc_hash.strip(), doc_length.strip())
    return doc_hashes


def avg_len(doc_lengths, n_docs):
    return sum(int(length) for length in doc_lengths.values()) / n_docs


def sort_scores(scores_dict):
    sorted_scores = {}
    for query_id, doc_scores in scores_dict.items():
        sorted_doc_scores = dict(sorted(doc_scores.items(), key=lambda x: x[1], reverse=True))
        sorted_scores[query_id] = sorted_doc_scores
    return sorted_scores


def write_scores(scores_dict, file_name):
    with open(file_name + ".txt", "w") as f:
        for query_id, doc_scores in scores_dict.items():
            rank = 1
            for doc_id, score in doc_scores.items():
                if rank > 1000:
                    break
                line = f"{query_id} Q0 {doc_id} {rank} {score} Exp\n"
                f.write(line)
                rank += 1


stop_words_list = get_stopwords(STOPWORDS_FILE_PATH)
queries_dict = read_queries(QUERIES_FILE_PATH_MODIFIED, stop_words_list)
catalog_dict = read_catalog(CATALOG_FILE_PATH)
doc_hashes_dict = read_hashes(DOC_HASHES_FILE_PATH)
avg_doc_length = avg_len(doc_hashes_dict, len(doc_hashes_dict))


def okapi_tf(catalog_dict, doc_names, query_dict, avg_corp_len):
    scores = {}
    for q_id, query in query_dict.items():
        scores[q_id] = {}
        for doc in doc_names:
            scores[q_id][doc[0]] = 0.0
    for q_id, query in query_dict.items():
        for word in query:
            index_placement = catalog_dict.get(word, 0)
            if index_placement != 0:
                offset = index_placement[0]
                length = index_placement[1]
                with open(INVERTED_INDEX_FILE_PATH, "r") as f:
                    f.seek(int(offset))
                    doc_details = f.read(int(length))
                    doc_details_dict = parser(doc_details)
                for doc, positions in doc_details_dict.items():
                    if doc in doc_names:
                        tf = len(positions)
                        len_d = int(doc_names[doc][1])
                        temp_score = tf / (tf + 0.5 + (1.5 * (len_d / avg_corp_len)))
                        scores[q_id][doc_names[doc][0]] += temp_score
    return scores


doc_scores = okapi_tf(catalog_dict, doc_hashes_dict, queries_dict, avg_doc_length)
doc_scores_sorted = sort_scores(doc_scores)
write_scores(doc_scores_sorted, "okapitf_results")


def okapi_BM25(catalog_dict, doc_names, query_dict):
    scores = {}
    k_1 = 1.5
    k_2 = 1.2
    b = 0.5
    total_docs = len(doc_names)
    avg_len = avg_doc_length
    for q_id, query in query_dict.items():
        scores[q_id] = {}
        queries_counter = Counter(query)
        for word, tf_query in queries_counter.items():
            index_placement = catalog_dict.get(word, 0)
            if index_placement != 0:
                offset, length = index_placement
                with open(INVERTED_INDEX_FILE_PATH, "r") as f:
                    f.seek(int(offset))
                    doc_details_dict = parser(f.read(int(length)))
                for doc, positions in doc_details_dict.items():
                    doc_info = doc_names.get(doc)
                    if doc_info:
                        df = len(doc_details_dict)
                        tf = len(positions)
                        len_d = int(doc_info[1])
                        calc1_num = total_docs - df + 0.5
                        calc1_den = df + 0.5
                        idf = math.log((calc1_num / calc1_den), 2)
                        calc2_num = tf * (k_1 + 1)
                        cal2_den = tf + k_1 * (1 - b + b * (len_d / avg_len))
                        bm25_tf = calc2_num / cal2_den
                        calc3_num = tf_query * (k_2 + 1)
                        calc3_den = tf_query + k_2
                        query_weight = calc3_num / calc3_den
                        temp_score = idf * bm25_tf * query_weight
                        doc_name = doc_info[0]
                        scores[q_id][doc_name] = scores[q_id].get(doc_name, 0) + temp_score
    return scores


doc_scores = okapi_BM25(catalog_dict, doc_hashes_dict, queries_dict)
doc_scores_sorted = sort_scores(doc_scores)
write_scores(doc_scores_sorted, "okapibm25_results")


def unigram_laplace(catalog_dict, doc_names, query_dict):
    scores = {}
    for q_id, query in query_dict.items():
        scores[q_id] = {}
    v = len(catalog_dict)
    with open(INVERTED_INDEX_FILE_PATH, "r") as f:
        for q_id, query in query_dict.items():
            for word in query:
                index_placement = catalog_dict.get(word, 0)
                doc_details_dict = {}
                if index_placement != 0:
                    offset, length = index_placement
                    f.seek(int(offset))
                    doc_details = f.read(int(length))
                    doc_details_dict = parser(doc_details)
                len_d = int(doc_names[hash_val][1])
                for hash_val, info in doc_names.items():
                    if hash_val in doc_details_dict:
                        positions = doc_details_dict[hash_val]
                        tf = len(positions)
                        temp_score = (tf + 1) / (len_d + v)
                    else:
                        temp_score = 1 / (len_d + v)
                    doc_name = info[0]
                    score = math.log(temp_score)
                    scores[q_id][doc_name] = scores[q_id].get(doc_name, 0) + score
    return scores


doc_scores = unigram_laplace(catalog_dict, doc_hashes_dict, queries_dict)
doc_scores_sorted = sort_scores(doc_scores)
write_scores(doc_scores_sorted, "laplace_results")


def okapi_tf_compressed(catalog_dict, doc_names, query_dict):
    scores = {}
    for q_id, query in query_dict.items():
        scores[q_id] = {}
    for q_id, query in query_dict.items():
        for word in query:
            index_placement = catalog_dict.get(word, 0)
            if index_placement != 0:
                offset = index_placement[0]
                length = index_placement[1]
                f = open(INVERTED_INDEX_FILE_PATH, "rb")
                f.seek(int(offset))
                doc_details = f.read(int(length))
                doc_details_decompressed = zlib.decompress(doc_details)
                doc_details_string = str(doc_details_decompressed, 'utf-8')
                doc_details_dict = parser(doc_details_string)
                f.close()
                for doc, positions in doc_details_dict.items():
                    if doc in doc_names:
                        tf = len(positions)
                        len_d = int(doc_names[doc][1])
                        avg_len = avg_doc_length
                        temp_score = tf / (tf + 0.5 + (1.5 * (len_d / avg_len)))
                        doc_name = doc_names[doc][0]
                        if doc_name not in scores[q_id].keys():
                            scores[q_id][doc_name] = temp_score
                        else:
                            scores[q_id][doc_name] += temp_score
                    else:
                        continue
    return scores


doc_scores = okapi_tf_compressed(catalog_dict, doc_hashes_dict, queries_dict)
doc_scores_sorted = sort_scores(doc_scores)
write_scores(doc_scores_sorted, "okapitf_comp_results")


def accumulator(td, d, wrd, w_positions_dict):
    curr_positions = w_positions_dict.get(wrd)
    acc = 0
    for w, p in w_positions_dict.items():
        if w == wrd:
            continue
        p1 = 0
        p2 = 0
        term_j_pos = p
        while p1 < len(curr_positions) and p2 < len(term_j_pos):
            window = abs(int(curr_positions[p1]) - int(term_j_pos[p2]))
            if window <= d:
                tf = len(term_j_pos)
                temp_acc = (math.log((td - tf + 1) / (tf + 0.5), 2)) / d ** 2
                acc += temp_acc
            if int(curr_positions[p1]) < int(term_j_pos[p2]):
                p1 += 1
            else:
                p2 += 1
        while p1 < len(curr_positions):
            window = abs(int(curr_positions[p1]) - int(term_j_pos[p2 - 1]))
            if window <= d:
                tf = len(term_j_pos)
                temp_acc = (math.log((td - tf + 1) / (tf + 0.5), 2)) / d ** 2
                acc += temp_acc
            p1 += 1
        while p2 < len(term_j_pos):
            window = abs(int(curr_positions[p1 - 1]) - int(term_j_pos[p2]))
            if window <= d:
                tf = len(term_j_pos)
                temp_acc = (math.log((td - tf + 1) / (tf + 0.5), 2)) / d ** 2
                acc += temp_acc
            p2 += 1
    return acc


def proximity(num_docs, tf, acc_t, len_d, avg_doc_len, k1, b):
    ief = math.log((num_docs - tf + 1) / (tf + 0.5), 2)
    calc2 = (acc_t * (k1 + 1)) / (acc_t + (k1 * ((1 - b) + (b * (len_d / avg_doc_len)))))
    prox = min(ief, 1) * calc2
    return prox


def proximity_search(catalog_dict, doc_names, query_dict):
    k_1 = 1.5
    b = 0.4
    d = 2
    scores = okapi_BM25(catalog_dict, doc_names, query_dict)
    for q_id, query in query_dict.items():
        query_info = {}
        for word in query:
            index_placement = catalog_dict.get(word, 0)
            if index_placement != 0:
                offset = index_placement[0]
                length = index_placement[1]
                f = open(INVERTED_INDEX_FILE_PATH, "r")
                f.seek(int(offset))
                doc_details = f.read(int(length))
                doc_details_dict = parser(doc_details)
                f.close()
                for doc, pos in doc_details_dict.items():
                    if query_info.get(doc):
                        query_info[doc][word] = pos
                    else:
                        query_info[doc] = {word: pos}
        for doc, words in query_info.items():
            for word, pos in words.items():
                tf = len(pos)
                curr_acc = accumulator(len(doc_names), d, word, words)
                prox_score = proximity(len(doc_names), tf, curr_acc, int(doc_names[doc][1]), avg_doc_length, k_1, b)
                doc_name = doc_names[doc][0]
                scores[q_id][doc_name] += prox_score
    return scores


queries_dict = read_queries(QUERIES_FILE_PATH_UNMODIFIED, stop_words_list)
doc_scores = proximity_search(catalog_dict, doc_hashes_dict, queries_dict)
doc_scores_sorted = sort_scores(doc_scores)
write_scores(doc_scores_sorted, "proximity_results.txt")
