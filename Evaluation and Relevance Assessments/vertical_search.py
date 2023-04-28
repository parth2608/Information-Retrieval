from urllib.parse import urlparse

import streamlit as st
from elasticsearch7 import Elasticsearch


def domain_retrieval(url):
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    return domain


def generate_qrel_file(records, filename):
    with open(filename, 'a') as f:
        for record in records:
            f.write(' '.join(record) + '\n')


qrel_records = []
if "qrel_records" not in st.session_state:
    st.session_state.qrel_records = []
st.markdown(f'<h1 style="color:white;">{"Vertical Search Page HW5"}</h1>', unsafe_allow_html=True)
with st.form("my_form"):
    query_id = st.text_input("Enter Query ID")
    text_query = st.text_input("Enter your Query")
    assessor_id = st.text_input("Enter Assessor ID (Your Name)")
    submitted = st.form_submit_button("Submit")
    if submitted:
        INDEX = 'hurricane_index_2'
        cloud_id = 'e95d3890bef24fef862afd449d36201c:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQwNGQ0MjUwNmQyYjU0N2FhYTZiNDRlMmFjODEzN2ZlNCQ4NGNkN2FkOGIwMWI0MzM4YTk0YzU1NmNjM2JiNTZmYQ=='
        es = Elasticsearch(request_timeout=10000, cloud_id=cloud_id, http_auth=('elastic', 'kdV7ROr42cZbPS4HvhdPMg41'))
        doc_count = 0
        response = es.search(
            index=INDEX,
            body={
                "query": {
                    "match": {
                        "content": text_query
                    }
                }
            }, size=150
        )
        st.session_state.displayed_docs = response['hits']['hits']

if "displayed_docs" in st.session_state:
    grade_values = []
    doc_urls = []

    for doc_count, hit in enumerate(st.session_state.displayed_docs, start=1):
        doc_url = hit["_id"]
        doc_author = hit["_source"]["author"]
        doc_content = hit["_source"]["content"]
        domain = domain_retrieval(doc_url)
        st.write(f"{doc_count}. {domain} - {doc_url}")
        with st.expander("Click Here for article text"):
            st.write("Author:", doc_author)
            st.write("Text:", doc_content)
        grade = st.radio(f"Grade for document {doc_count}", ('non-relevant', 'relevant', 'very relevant'),
                         key=f"doc{doc_count}")
        grade_values.append(grade)
        doc_urls.append(doc_url)

if st.button("Save Grades"):
    grade_map = {'non-relevant': 0, 'relevant': 1, 'very relevant': 2}
    grade_values = [st.session_state[f"doc{i + 1}"] for i in range(len(doc_urls))]
    qrel_records = [[query_id, assessor_id.replace(" ", "_"), doc_url, str(grade_map[grade])] for doc_url, grade
                    in zip(doc_urls, grade_values)]
    st.session_state.qrel_records = qrel_records

if st.button("Save QREL Records"):
    if st.session_state.qrel_records:
        filename = 'qrel_' + query_id + '.txt'
        generate_qrel_file(st.session_state.qrel_records, filename)
        st.success("QREL Records saved to qrel_records.txt")
    else:
        st.warning("No QREL records to save. Please submit a query first.")
