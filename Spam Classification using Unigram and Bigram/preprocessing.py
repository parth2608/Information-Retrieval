import email

import pandas as pd
from bs4 import BeautifulSoup
from nltk.tokenize import word_tokenize
from tqdm import tqdm


def read_index(file_path):
    ind = []
    with open(file_path, 'r', encoding='ISO-8859-1') as f:
        lines = f.readlines()
        for line in lines:
            ind.append(line)
    return ind


def gather_email_content(c):
    collected_content = []
    if type(c) == str:
        collected_content.append(c)
    elif type(c) == list:
        for each in c:
            if each.is_multipart():
                collected_content += gather_email_content(each.get_payload())
            else:
                collected_content += gather_email_content(each)
    elif c.get_content_type().split(' ')[0] == 'text/plain':
        if type(c.get_payload()) == str:
            collected_content.append(c.get_payload())
    elif c.get_content_type().split(' ')[0] == 'text/html':
        soup = BeautifulSoup(c.get_payload(), 'html.parser')
        txt = soup.get_text()
        if type(txt) == str and not txt == None:
            collected_content.append(txt)
    return ''.join(collected_content)


def extract_document(m):
    content = m.get_payload()
    body = gather_email_content(content)
    return m['Subject'], body


def clean_document(s):
    toks = word_tokenize(s)
    clean_toks = []
    for each in toks:
        if each.isalpha():
            clean_toks.append(each)
    return " ".join(clean_toks)


def create_dataset(file_path):
    index = read_index(file_path)
    data = {}
    for line in tqdm(index):
        line_list = line.split()
        label = line_list[0]
        doc_path = 'trec07p' + line_list[1][2:]
        doc_id_s = doc_path.find('inmail.') + len('inmail.')
        doc_id = doc_path[doc_id_s:]
        with open(doc_path, 'r', encoding='ISO-8859-1') as f:
            raw_doc = f.read()
            msg = email.message_from_string(raw_doc)
            subject, doc = extract_document(msg)
            clean_doc = clean_document(doc)
            data[doc_id] = {'doc_path': doc_path, 'raw_doc': raw_doc, 'subject': subject,
                            'clean_doc': clean_doc, 'label': label}
            f.close()
    df = pd.DataFrame.from_dict(data, orient='index')
    return df


PTH = "trec07p/full/index"
corpus_df = create_dataset(PTH)
corpus_df.to_csv("corpus_df_clean.csv", header=True)
