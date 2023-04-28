import os
import re

import nltk
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from nltk.tokenize import RegexpTokenizer

tokenizer = RegexpTokenizer(r'\w+')
stemmer = nltk.PorterStemmer()

doc_start_pattern = re.compile(r'<DOC>')
doc_end_pattern = re.compile(r'</DOC>')

with open('stoplist.txt', 'r') as file:
    stopwords = file.read()

es = Elasticsearch()

request_body = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 1,
        "analysis": {
            "filter": {
                "english_stop": {
                    "type": "stop",
                    "stopwords_path": 'my_stoplist.txt'
                },
                "english_stemmer": {
                    "type": "stemmer",
                    "language": "english"
                }

            },
            "analyzer": {
                "stopped": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "english_stop",
                        "english_stemmer"
                    ]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "text": {
                "type": "text",
                "fielddata": True,
                "analyzer": "stopped",
                "index_options": "positions"
            }
        }
    }
}

response = es.indices.create(index='ap_dataset_new', body=request_body, ignore=400)


def yield_docs():
    file_name = os.listdir("./ap89_collection/")
    file_name = ["./ap89_collection/" + i for i in file_name]
    for f in file_name:
        with open(f, 'r', encoding="ISO-8859-1") as corpus_file:
            doc_start = False
            text = ""
            docno = ""
            for line in corpus_file:
                if doc_start_pattern.match(line):
                    doc_start = True
                    text = ""
                    docno = ""
                elif doc_end_pattern.match(line):
                    text = text.lower()
                    text = tokenizer.tokenize(text)
                    text = [word for word in text if word not in stopwords]
                    text = [stemmer.stem(word) for word in text]
                    text = " ".join(text)
                    doc_start = False
                    doc_source = {
                        "text": text
                    }
                    yield {
                        "_index": "ap_dataset_new",
                        "_id": docno.strip(),
                        "_source": doc_source
                    }
                elif doc_start:
                    if line.startswith("<DOCNO>"):
                        docno = line.strip().replace("<DOCNO>", "").replace("</DOCNO>", "")
                    elif line.startswith("<TEXT>"):
                        text = line.strip().replace("<TEXT>", "").replace("</TEXT>", "")
                    else:
                        text = text + " " + line.strip()


yield_docs()
resp = helpers.bulk(es, yield_docs())
