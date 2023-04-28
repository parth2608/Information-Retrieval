import numpy as np
import pandas as pd
from sklearn.datasets import dump_svmlight_file
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import MultinomialNB
from sklearn.tree import DecisionTreeClassifier

corpus_df = pd.read_csv('corpus_df_clean.csv')
corpus_df.drop(['Unnamed: 0'], axis=1, inplace=True)
all_data = corpus_df.iloc[:, [3, 4]]
all_data.loc[all_data['label'] == 'spam', 'label'] = 1
all_data.loc[all_data['label'] == 'ham', 'label'] = 0
all_data['clean_doc'] = all_data['clean_doc'].astype('U')
train_x, test_x, train_y, test_y = train_test_split(all_data['clean_doc'], all_data['label'], test_size=0.2)
train_x = train_x.astype('str')
train_y = train_y.astype('int')
test_y = test_y.astype('int')
trial_a_vocab = ['free', 'click here', 'winner', 'won', 'prize', 'money', 'cash', 'porn', 'access now', 'apply now',
                 'credit', 'buy now', 'limited time', 'call now', 'congratulations', 'earn money', 'give away']
vectorizer = CountVectorizer(analyzer='word', min_df=0.005, max_df=0.995, vocabulary=trial_a_vocab)
fitted_x_train_A = vectorizer.fit_transform(train_x)
transformed_x_test_A = vectorizer.transform(test_x)
dump_svmlight_file(fitted_x_train_A, train_y, 'trainA_dump.txt')
trial_b_vocab = ['free', 'spam', 'click', 'buy', 'clearance', 'shopper', 'order', 'earn', 'cash', 'extra', 'money',
                 'double', 'collect', 'credit', 'check', 'affordable', 'fast', 'price', 'loans', 'profit', 'refinance',
                 'hidden', 'freedom', 'chance', 'miracle', 'lose', 'home', 'remove', 'success', 'virus', 'malware',
                 'ad', 'subscribe', 'sales', 'performance', 'valium', 'medicine', 'diagnostics', 'million', 'join',
                 'deal', 'unsolicited', 'trial', 'prize', 'now', 'legal', 'bonus', 'limited', 'instant', 'luxury',
                 'celebrity', 'only', 'compare', 'win', 'viagra', '$$$', '$discount', 'click here', 'meet singles',
                 'incredible deal', 'lose weight', 'act now', '100% free', 'fast cash', 'million dollars',
                 'lower interest rate', 'visit our website', 'no credit check']
vectorizer2 = CountVectorizer(analyzer='word', min_df=0.005, max_df=0.995, vocabulary=trial_b_vocab)
fitted_x_train_B = vectorizer2.fit_transform(train_x)
transformed_x_test_B = vectorizer2.transform(test_x)
dump_svmlight_file(fitted_x_train_B, train_y, 'trainB_dump.txt')
vectorizer3 = CountVectorizer(analyzer='word', min_df=0.005, max_df=0.995)
fitted_x_train = vectorizer3.fit_transform(train_x)
trial_c_vocab = list(vectorizer3.vocabulary_.keys())
transformed_x_test = vectorizer3.transform(test_x)
dump_svmlight_file(fitted_x_train, train_y, 'trainC_dump.txt')


def write_results(scores, trial, y):
    y_list = y.tolist()
    x_list = test_x.tolist()
    name = 'trial_' + str(trial) + '.txt'
    o = open('' + name, "w")
    for i in range(10):
        o.write("Preidctied probability of being spam: " + str(scores[i]) + "\n")
        if y_list[i] == 1:
            o.write("Is actually: Spam \n")
        else:
            o.write("Is actually: Ham \n")
        o.write("Text: " + x_list[i] + "\n")
    o.close()
    return


def classify_documents(x_train_data, x_test_data, y_train_labels, y_test_labels, trial):
    lr = LogisticRegression(max_iter=1000, C=0.01, penalty='l1', solver='liblinear')
    lr.fit(x_train_data, y_train_labels)
    lr_probs = lr.predict_proba(x_test_data)[:, 1]
    lr_score = roc_auc_score(np.array(y_test_labels), lr_probs)
    write_results(lr_probs, trial, y_test_labels)
    print('ROC AUC score for Logistic Regression: ', lr_score)
    print('Most important features: ')
    importance_lr = lr.coef_[0]
    features_lr = {}
    for i, v in enumerate(importance_lr):
        if trial == 'A':
            features_lr[trial_a_vocab[i]] = v
        elif trial == 'B':
            features_lr[trial_b_vocab[i]] = v
        elif trial == 'C':
            features_lr[trial_c_vocab[i]] = v
    features_lr = {k: v for k, v in sorted(features_lr.items(), key=lambda item: item[1], reverse=True)}
    i = 0
    for feat, sc in features_lr.items():
        if i > 5:
            break
        print(feat, "\t", round(sc, 2))
        i += 1
    dt = DecisionTreeClassifier()
    dt.fit(x_train_data, y_train_labels)
    dt_probs = dt.predict_proba(x_test_data)[:, 1]
    dt_score = roc_auc_score(np.array(y_test_labels), dt_probs)
    write_results(dt_probs, trial, y_test_labels)
    print('ROC AUC score for Decision Tree: ', dt_score)
    print('Most important features: ')
    importance_dt = dt.feature_importances_
    features_dt = {}
    for i, v in enumerate(importance_dt):
        if trial == 'A':
            features_dt[trial_a_vocab[i]] = v
        elif trial == 'B':
            features_dt[trial_b_vocab[i]] = v
        elif trial == 'C':
            features_dt[trial_c_vocab[i]] = v
    features_dt = {k: v for k, v in sorted(features_dt.items(), key=lambda item: item[1], reverse=True)}
    i = 0
    for feat, sc in features_dt.items():
        if i > 5:
            break
        print(feat, "\t", round(sc, 2))
        i += 1
    nb = MultinomialNB()
    nb.fit(x_train_data, y_train_labels)
    nb_probs = nb.predict_proba(x_test_data)[:, 1]
    nb_score = roc_auc_score(np.array(y_test_labels), nb_probs)
    write_results(nb_probs, trial, y_test_labels)
    print('ROC AUC score for Naive Bayes: ', nb_score)
    importance_nb = nb.coef_[0]
    features_nb = {}
    for i, v in enumerate(importance_nb):
        if trial == 'A':
            features_nb[trial_a_vocab[i]] = v
        elif trial == 'B':
            features_nb[trial_b_vocab[i]] = v
        elif trial == 'C':
            features_nb[trial_c_vocab[i]] = v
    features_nb = {k: v for k, v in sorted(features_nb.items(), key=lambda item: item[1], reverse=True)}
    i = 0
    for feat, sc in features_nb.items():
        if i > 5:
            break
        print(feat, "\t", round(sc, 2))
        i += 1


classify_documents(fitted_x_train_A, transformed_x_test_A, train_y, test_y, "A")
classify_documents(fitted_x_train_B, transformed_x_test_B, train_y, test_y, "B")
classify_documents(fitted_x_train, transformed_x_test, train_y, test_y, "C")
