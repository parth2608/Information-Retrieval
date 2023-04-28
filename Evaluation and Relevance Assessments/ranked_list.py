from elasticsearch7 import Elasticsearch


class ES:

    def __init__(self):
        self.cloud_id = "e95d3890bef24fef862afd449d36201c:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQwNGQ0MjUwNmQyYjU0N2FhYTZiNDRlMmFjODEzN2ZlNCQ4NGNkN2FkOGIwMWI0MzM4YTk0YzU1NmNjM2JiNTZmYQ=="
        self.index = 'hurricane_index_2'
        self.es = Elasticsearch(request_timeout=20000, cloud_id=self.cloud_id,
                                http_auth=('elastic', 'kdV7ROr42cZbPS4HvhdPMg41'), timeout=30)
        self.rank_list = {"150301": [], "150302": [], "150303": [], "150304": []}
        self.query = ["Hurricane Sandy", "hurricane katrina damage", "hurricane Ike", "hurricane Maria"]
        self.query_id = ["150301", "150302", "150303", "150304"]

    def get_rank_list(self):
        for idx, q in enumerate(self.query):
            print("Reading ranked list: " + str(idx + 1))
            q_id = self.query_id[idx]
            temp = self.es.search(index="hurricane_index_2",
                                  body={
                                      "size": 150,
                                      "query": {
                                          "match": {
                                              "content": q
                                          }
                                      },
                                      "_source": ""
                                  })['hits']['hits']
            for item in temp:
                self.rank_list[q_id].append({item['_id']: item['_score']})

    def output_rank_list(self):
        with open("ranked_list.txt", "a", encoding="utf-8") as f:
            for q_id in self.rank_list:
                for idx, item in enumerate(self.rank_list[q_id]):
                    for url in item:
                        line = '{0} Q0 {1} {2} {3} Exp\n'.format(q_id, url, idx + 1, str(item[url]))
                        f.write(line)


my_es = ES()
my_es.get_rank_list()
my_es.output_rank_list()
