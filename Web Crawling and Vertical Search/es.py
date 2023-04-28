from urllib.parse import urlparse, urlunparse

from elasticsearch import Elasticsearch
from elasticsearch import helpers

from parser import Parser


def query_preprocess(url):
    parsed_url = urlparse(url)
    parsed_url = parsed_url._replace(scheme=parsed_url.scheme.lower(), netloc=parsed_url.netloc.lower())
    if parsed_url.port:
        parsed_url = parsed_url._replace(netloc=parsed_url.hostname)
    parsed_url = parsed_url._replace(fragment="")
    path = parsed_url.path.replace('//', '/')
    if path:
        clean_path = path if path[0] == '/' else '/' + path
        clean_parsed = parsed_url._replace(path=clean_path)
        segments = clean_parsed.path.split('/')
        segments = [s for s in segments if s != '.']
        i = 0
        while i < len(segments):
            if segments[i] == '..':
                if i > 0:
                    del segments[i - 1:i + 1]
                    i -= 1
            else:
                i += 1
        path = '/'.join(segments)
        new_parts = (
            clean_parsed.scheme, clean_parsed.netloc, path, clean_parsed.params, clean_parsed.query,
            clean_parsed.fragment)
        return urlunparse(new_parts)
    else:
        return urlunparse(parsed_url)


class ES:
    def __init__(self):
        self.cloud_id = "e95d3890bef24fef862afd449d36201c:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQwNGQ0MjUwNmQyYjU0N2FhYTZiNDRlMmFjODEzN2ZlNCQ4NGNkN2FkOGIwMWI0MzM4YTk0YzU1NmNjM2JiNTZmYQ=="
        self.index = 'hurricane_index'
        self.es = Elasticsearch(request_timeout=20000, cloud_id=self.cloud_id,
                                http_auth=('elastic', 'kdV7ROr42cZbPS4HvhdPMg41'), timeout=30)
        print(self.es.ping())
        self.parser = Parser()

    def indexer(self):
        docs = self.parser.parse_docs()
        in_links, out_links = self.parser.parse_links()

        for id in docs:
            search_query = {
                "query": {
                    "term": {
                        "_id": query_preprocess(id)
                    }
                }
            }
            response = self.es.search(index=self.index, body=search_query)

            author = "Parth Shah"

            if response['hits']['total']['value'] > 0:
                existing_doc = response['hits']['hits'][0]['_source']
                if existing_doc['author'] != author:
                    existing_doc['author'] += f", {author}"
            else:
                existing_doc = {
                    "author": author,
                    "inlinks": [],
                    "outlinks": []
                }

            for inlink in in_links[id]:
                if inlink not in existing_doc["inlinks"]:
                    existing_doc["inlinks"].append(inlink)

            for outlink in out_links[id]:
                if outlink not in existing_doc["outlinks"]:
                    existing_doc["outlinks"].append(outlink)

            actions = [
                {
                    "_op_type": "update",
                    "_index": self.index,
                    "_id": query_preprocess(id),
                    "doc": {
                        "author": existing_doc['author'],
                        "content": docs[id],
                        "inlinks": existing_doc["inlinks"],
                        "outlinks": existing_doc["outlinks"]
                    },
                    "doc_as_upsert": True
                }
            ]
            helpers.bulk(self.es, actions=actions)


my_es = ES()
my_es.indexer()
