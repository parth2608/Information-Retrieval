import numpy as np
import pandas as pd
from sklearn.preprocessing import RobustScaler
from sklearn.svm import SVC

PTH_DATA = 'features.csv'
features = pd.read_csv(PTH_DATA)
features.set_index('query-docid', inplace=True, drop=True)
features.drop(['Unnamed: 0'], axis=1, inplace=True)

rs = RobustScaler()
columns = ['bm25', 'laplace', 'jm', 'tfidf', 'okapi']
features[columns] = rs.fit_transform(features[columns])


def get_query_ids():
    q = np.array([85, 59, 56, 71, 64, 62, 93, 99, 58, 77, 54, 87, 94,
                  100, 89, 61, 95, 68, 57, 97, 98, 60, 80, 63, 91])
    return q


def get_train_test_q_ids(q, i):
    tst = q[i * 5:i * 5 + 5]
    trn = np.setdiff1d(q, tst)
    return trn, tst


def get_train_test_features(train_ids, test_ids):
    tst = features[features['q_id'].isin(test_ids)]
    trn = features[features['q_id'].isin(train_ids)]
    return trn, tst


def create_list_from_df(tr_x, tr_y, tst_x, tst_y):
    tr_x_list = []
    for rows in tr_x.itertuples():
        tmp_list = [rows.bm25, rows.laplace, rows.jm, rows.tfidf, rows.okapi]
        tr_x_list.append(tmp_list)
    tr_y_list = []
    for rows in tr_y.itertuples():
        tr_y_list.append(rows.label)
    tst_x_list = []
    for rows in tst_x.itertuples():
        tmp_list = [rows.bm25, rows.laplace, rows.jm, rows.tfidf, rows.okapi]
        tst_x_list.append(tmp_list)
    tst_y_list = []
    for rows in tst_y.itertuples():
        tst_y_list.append(rows.label)
    return tr_x_list, tr_y_list, tst_x_list, tst_y_list


def write_results(scores, i, tst):
    name = 'test_scores_' + str(i) + '.txt'
    o = open(name, "w")
    rank = 1
    j = 0
    pair = tst.index[0]
    last_q_id_end = pair.find("-")
    last_q_id = str(pair[:last_q_id_end]).strip()
    for i, row in tst.iterrows():
        pair = str(i)
        q_id_end = pair.find("-")
        q_id = str(pair[:q_id_end]).strip()
        if q_id != last_q_id:
            rank = 1
            last_q_id = q_id
        docid_start = pair.find("A")
        docid = str(pair[docid_start:]).strip()
        score = str(scores[j])
        r = str(rank)
        o.write(q_id + " " + "Q0" + " " + docid + " " + r + " " + score + " " + "Exp\n")
        rank += 1
        j += 1
    o.close()
    return


def write_results_train(scores, i, tr):
    name = 'train_scores_' + str(i) + '.txt'
    o = open(name, "w")
    rank = 1
    j = 0
    pair = tr.index[0]
    last_q_id_end = pair.find("-")
    last_q_id = str(pair[:last_q_id_end]).strip()
    for i, row in tr.iterrows():
        pair = str(i)
        q_id_end = pair.find("-")
        q_id = str(pair[:q_id_end]).strip()
        if q_id != last_q_id:
            rank = 1
            last_q_id = q_id
        docid_start = pair.find("A")
        docid = str(pair[docid_start:]).strip()
        score = str(scores[j])
        r = str(rank)
        o.write(q_id + " " + "Q0" + " " + docid + " " + r + " " + score + " " + "Exp\n")
        rank += 1
        j += 1
    o.close()
    return


def train_cross_validation():
    q_ids = get_query_ids()
    for i in range(5):
        train_q_ids, test_q_ids = get_train_test_q_ids(q_ids, i)
        train_features, test_features = get_train_test_features(train_q_ids, test_q_ids)
        x_train, y_train, x_test, y_test = train_features.iloc[:, 2:], train_features.iloc[:, 1:2], \
            test_features.iloc[:, 2:], test_features.iloc[:, 1:2]
        x_train_l, y_train_l, x_test_l, y_test_l = create_list_from_df(x_train, y_train, x_test, y_test)
        sv = SVC(C=1, gamma=0.4, kernel='rbf', probability=True)
        sv.fit(x_train_l, y_train_l)
        write_results(sv.predict_proba(x_test)[:, 1], i, y_test)
        write_results_train(sv.predict_proba(x_train)[:, 1], i, y_train)


train_cross_validation()
