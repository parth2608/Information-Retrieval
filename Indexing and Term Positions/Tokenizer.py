import os
import pickle
import re
from collections import OrderedDict

from nltk.stem import PorterStemmer

STOPWORDS_PATH = "stoplist.txt"
STEMMING_ENABLED = True
if STEMMING_ENABLED:
    stemmer = PorterStemmer()

document_dict = OrderedDict()
document_hashes = {}
stopwords = [line.strip() for line in open(STOPWORDS_PATH, encoding="ISO-8859-1", errors='ignore')]


def get_files(folder_path):
    return [os.path.join(folder_path, file) for file in os.listdir(folder_path)]


def get_text(file):
    with open(file, encoding="ISO-8859-1", errors='ignore') as f:
        return f.readlines()


def tokenizer(text):
    tokens = []
    text_list = re.findall(r"\w+|[^\w\s]|[\n\r]+", text, re.UNICODE)
    for pos, token in enumerate(text_list):
        if token.isalnum() and token not in stopwords:
            if STEMMING_ENABLED:
                token = stemmer.stem(token)
            token_tuple = (token, pos)
            tokens.append(token_tuple)
    return tokens


def parser(files):
    doc_hash = 1
    for count, _file in enumerate(files):
        file_name = os.path.basename(_file)
        if file_name != 'readme':
            data = get_text(_file)
            docs = []
            current = []
            for element in data:
                doc_end_tag = "</DOC>"
                if doc_end_tag not in element:
                    current.append(element)
                else:
                    current.append(element)
                    docs.append(current)
                    current = []
            for doc in docs:
                doc = " ".join(doc)
                docend = doc.find("</DOC>")
                doc_sub = doc[:docend]
                docno_s = doc_sub.find("<DOCNO>") + len("<DOCNO>")
                docno_e = doc_sub.find("</DOCNO>")
                docno = doc_sub[docno_s:docno_e].strip()
                text_s = doc_sub.find("<TEXT>") + len("<TEXT>")
                text_e = doc_sub.find("</TEXT")
                text = doc_sub[text_s:text_e].strip() + "\\ln"
                text = text.lower()
                tokens = tokenizer(text)
                document_dict[doc_hash] = tokens
                document_hashes[docno] = (doc_hash, len(tokens))
                doc_hash += 1


def write_document_hashes_to_file(save_path):
    with open(save_path, "w") as outF:
        for docid, doc_tuple in document_hashes.items():
            outF.write(f"{docid} {doc_tuple[0]}|{doc_tuple[1]}\n")


def write_document_dict_to_pickle(save_path):
    with open(f"{save_path}dictionary.pickle", 'wb') as handle:
        pickle.dump(document_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)


all_files_paths = get_files("ap89_collection/")
parser(all_files_paths)
write_document_hashes_to_file("hashes_file.txt")
write_document_dict_to_pickle("")
