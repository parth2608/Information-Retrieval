import re

import nltk
from nltk.tokenize import RegexpTokenizer

with open('stoplist.txt', 'r') as file:
    stopwords = set(file.read().splitlines())

tokenizer = RegexpTokenizer(r'\w+')
stemmer = nltk.PorterStemmer()

with open("query_desc.51-100.short.txt", "r", encoding="ISO-8859-1") as query_file, \
        open("queries_parsed.txt", "w") as output_file:
    for line in query_file:
        if not line.strip():
            break
        query_id, text = re.match(r"(\d+)\.(.*)", line).groups()
        words = [stemmer.stem(word) for word in tokenizer.tokenize(text.lower()) if word not in stopwords]
        output_file.write(f"{query_id}. {' '.join(words)}\n")
