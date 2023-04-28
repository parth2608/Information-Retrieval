import pickle
from collections import OrderedDict
from itertools import islice

STEMMING = True
if STEMMING:
    HASHES_PATH = "stemmed_hashes_file.txt"
    PICKLE = 'stemmed_dictionary.pickle'
else:
    HASHES_PATH = "hashes_file.txt"
    PICKLE = 'dictionary.pickle'


def indexer(d, tokens):
    for doc, doc_tokens in d.items():
        for token in doc_tokens:
            if token[0] not in tokens:
                tokens[token[0]] = {}
            if doc not in tokens[token[0]]:
                tokens[token[0]][doc] = []
            tokens[token[0]][doc].append(token[1])


def sort_tokens(tokens):
    return OrderedDict(sorted(tokens.items(), key=lambda t: t[0]))


def write_index(tokens, save_path, group_no):
    catalog_name = "catalog" + str(group_no) + ".txt"
    inverted_index_name = "inverted_index" + str(group_no) + ".txt"
    with open(save_path + catalog_name, "w") as catalog, open(save_path + inverted_index_name, "w") as inverted_index:
        offset = 0
        for token, docs in tokens.items():
            index_string = []
            for doc, pos_list in docs.items():
                pos_string = ",".join(str(pos).strip() for pos in pos_list)
                index_string.append(f"{doc}:{pos_string}|")
            index_string = "".join(index_string)
            inverted_index.write(index_string)
            catalog.write(f"{token.strip()}:{offset},{len(index_string)}\n")
            offset += len(index_string)


def split(d, n):
    for i in range(0, len(d), n):
        yield OrderedDict(islice(d.items(), i, i + n))


with open(PICKLE, 'rb') as handle:
    doc_dict = pickle.load(handle)
split_docs_list = list(split(doc_dict, 1000))
for i, split_doc in enumerate(split_docs_list):
    tokens = {}
    indexer(split_doc, tokens)
    tokens = sort_tokens(tokens)
    write_index(tokens, "", i + 1)
