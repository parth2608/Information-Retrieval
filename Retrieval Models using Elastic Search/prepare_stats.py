import pickle

from elasticsearch import Elasticsearch

es = Elasticsearch()

query_body = {
    "size": 10000,
    "query": {
        "match_all": {}
    }
}
response = es.search(index="ap_dataset_new", body=query_body, scroll='3m')
scroll_id = response['_scroll_id']

doc_ids = [doc['_id'] for doc in response['hits']['hits']]

while len(response['hits']['hits']):
    response = es.scroll(scroll_id=scroll_id, scroll='3m')
    scroll_id = response['_scroll_id']
    doc_ids.extend([doc['_id'] for doc in response['hits']['hits']])

vector_per_doc = {}
for doc in doc_ids:
    term_vector = es.termvectors(index="ap_dataset_new", id=doc, fields="text", term_statistics=True,
                                 field_statistics=True, positions=False, offsets=False, payloads=False)
    vector_per_doc[doc] = term_vector

with open('term_vectors_new.pickle', 'wb') as handle:
    pickle.dump(vector_per_doc, handle, protocol=pickle.HIGHEST_PROTOCOL)
