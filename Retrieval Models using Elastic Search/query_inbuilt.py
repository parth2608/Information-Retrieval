import json
import re

import requests

endpoint = "http://localhost:9200"
index = "ap_dataset_new"
doc_type = "_doc"

with open("queries_updated_aggs.txt", "r") as f:
    with open("results_inbuilt_aggs.txt", "w") as output_file:
        for line in f:
            query_id, text = re.match("(\d+)\.(.*)", line).groups()
            query = {
                "query": {
                    "match": {
                        "text": text.strip()
                    }
                },
                "size": 1000,
                "sort": [{"_score": {"order": "desc"}}]
            }
            results = json.loads(requests.get(f"{endpoint}/{index}/{doc_type}/_search", json=query).text)["hits"][
                "hits"]
            for rank, r in enumerate(results):
                docno = r["_id"]
                score = r["_score"]
                output_file.write(f"{query_id} Q0 {docno} {rank + 1} {score} Exp\n")
