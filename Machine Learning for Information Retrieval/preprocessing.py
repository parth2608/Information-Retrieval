import pandas as pd


def read_qrel_file(file_path):
    data = {}
    with open(file_path, encoding="ISO-8859-1", errors='ignore') as f:
        for line in f:
            line_list = line.split()
            q_id = line_list[0]
            doc_id = line_list[2]
            rel = line_list[3]
            if q_id in queries:
                if q_id not in data:
                    data[q_id] = {'rel': set([]), 'non-rel': set([])}
                if rel == '1':
                    data[q_id]['rel'].add(doc_id)
                elif rel == '0':
                    data[q_id]['non-rel'].add(doc_id)
    f.close()
    return data


def expand_qrel_data(file_path):
    with open(file_path, encoding="ISO-8859-1", errors='ignore') as f:
        for line in f:
            line_list = line.split()
            q_id = line_list[0]
            doc_id = line_list[2]
            if len(qrel_data[q_id]['non-rel']) < 1000:
                if doc_id not in qrel_data[q_id]['non-rel']:
                    qrel_data[q_id]['non-rel'].add(doc_id)
    f.close()


def create_dataframe():
    df = pd.DataFrame(columns=('query-docid', 'q_id', 'label'))
    k = 0
    for key, value in qrel_data.items():
        rels = list(value['rel'])
        nonrels = list(value['non-rel'])
        for i in range(len(rels)):
            values_to_add = {'query-docid': str(key + "-" + rels[i]), 'q_id': str(key), 'label': 1}
            row_to_add = pd.Series(values_to_add, name=k)
            df = df.append(row_to_add)
            k += 1
        for j in range(len(nonrels)):
            values_to_add = {'query-docid': str(key + "-" + nonrels[j]), 'q_id': str(key), 'label': 0}
            row_to_add = pd.Series(values_to_add, name=k)
            df = df.append(row_to_add)
            k += 1
    return df


def create_score_dict(file_path):
    feature_dict = {}
    with open(file_path, encoding="ISO-8859-1", errors='ignore') as f:
        for line in f:
            line_list = line.split()
            q_id = line_list[0]
            doc_id = line_list[2]
            score = line_list[4]
            pair = q_id + "-" + doc_id
            feature_dict[pair] = score
    f.close()
    return feature_dict


queries = {'85', '59', '56', '71', '64', '62', '93', '99', '58', '77', '54', '87', '94', '100', '89', '61', '95', '68',
           '57', '97', '98', '60', '80', '63', '91'}
qrel_data = read_qrel_file('qrels.adhoc.51-100.AP89.txt')
expand_qrel_data('es_results.txt')
bm25_scores = create_score_dict('results_okapibm25.txt')
laplace_scores = create_score_dict('results_laplace.txt')
jm_scores = create_score_dict('results_jm.txt')
tf_idf_scores = create_score_dict('results_tfidf.txt')
okapi_scores = create_score_dict('results_okapitf.txt')
features = create_dataframe()
pairs = features['query-docid'].tolist()
bm25_col = []
laplace_col = []
jm_col = []
tfidf_col = []
okapi_col = []
for i in range(len(pairs)):
    bm25_col.append(bm25_scores.get(pairs[i], 0))
    laplace_col.append(laplace_scores.get(pairs[i], -5000))
    jm_col.append(jm_scores.get(pairs[i], -5000))
    tfidf_col.append(tf_idf_scores.get(pairs[i], 0))
    okapi_col.append(okapi_scores.get(pairs[i], 0))
features['bm25'] = bm25_col
features['laplace'] = laplace_col
features['jm'] = jm_col
features['tfidf'] = tfidf_col
features['okapi'] = okapi_col
features.to_csv('features.csv')
