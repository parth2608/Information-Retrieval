import json
import re


class Parser:

    def __init__(self):
        self.path = ""
        self.doc = "documents.txt"
        self.in_links = "in_links.json"
        self.out_links = "out_links.json"
        self.count = 40000

    def parse_docs(self):
        docs = {}
        doc = list()
        add_file_flag = 0
        txt_flag = 0
        with open(self.path + self.doc, "r", encoding="utf-8") as f:
            count = 0
            for line in f:
                line = line.strip()
                if re.search("</DOC>", line):
                    add_file_flag = 0
                    docs[data_id] = ' '.join(doc)
                    doc = list()
                    count += 1
                    if count == self.count:
                        return docs
                if add_file_flag == 1:
                    if re.search("</DOCNO>", line):
                        data_id = re.sub("(<DOCNO>)|(</DOCNO>)", "", line)
                    if re.search("</TEXT>", line):
                        txt_flag = 0
                    if txt_flag == 1:
                        doc.append(line)
                    if re.search("<TEXT>", line):
                        if re.search("[A-Z|a-z]*[a-z]", line):
                            doc.append(line[6:])
                        txt_flag = 1
                if re.search("<DOC>", line):
                    add_file_flag = 1
        return docs

    def parse_links(self):
        in_links = {}
        out_links = {}
        count = 0
        with open(self.path + self.in_links, "r", encoding="utf-8") as fi:
            for line in fi:
                in_links.update(json.loads(line))
                count += 1
        count = 0
        with open(self.path + self.out_links, "r", encoding="utf-8") as fo:
            for line in fo:
                out_links.update(json.loads(line))
                count += 1
        return in_links, out_links
