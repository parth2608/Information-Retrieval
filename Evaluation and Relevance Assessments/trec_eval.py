import math
import pickle
import sys
from collections import OrderedDict

RELEVANT_DOCS = OrderedDict()
RETURNED_DOCS = OrderedDict()


def parse_qrel(q_path):
    with open(q_path, 'r') as f:
        for line in f:
            l = line.split(" ")
            query = l[0]
            doc = l[2]
            score = int(l[3])
            if score != 0:
                if query in RELEVANT_DOCS:
                    RELEVANT_DOCS[query].append(doc)
                else:
                    RELEVANT_DOCS[query] = [doc]
    f.close()


def parse_results(r_path):
    with open(r_path, 'r') as f:
        for line in f:
            l = line.split(" ")
            query = l[0]
            doc = l[2]
            if query in RETURNED_DOCS:
                RETURNED_DOCS[query].append(doc)
            else:
                RETURNED_DOCS[query] = [doc]
    f.close()


def evaluate(q):
    num_queries = len(RETURNED_DOCS)
    metrics = {}
    k_values = [5, 10, 20, 50, 100]
    k_overall_metrics = OrderedDict()
    graph_details = {}
    for q_id, docs in RETURNED_DOCS.items():
        query_metrics = {}
        relevant_docs = RELEVANT_DOCS[q_id]
        n = len(relevant_docs)
        rank = 1
        retrieved_and_relevant = 0
        k_query_metrics = OrderedDict()
        for each in k_values:
            k_query_metrics[each] = {}
        precision_sum = 0
        graph_details[q_id] = OrderedDict()
        r = 0
        relevance = []
        for doc in docs:
            k_metrics = OrderedDict()
            is_relevant = 0
            if doc in relevant_docs:
                is_relevant = 1
                retrieved_and_relevant += 1
            relevance.append(is_relevant)
            precision = retrieved_and_relevant / rank
            if n == 0:
                recall = 0
            else:
                recall = retrieved_and_relevant / n
            if retrieved_and_relevant == 0:
                f1 = 0
            else:
                f1 = (2 * precision * recall) / (precision + recall)
            if rank in k_values:
                k_query_metrics[rank]['Precision'] = precision
                k_query_metrics[rank]['Recall'] = recall
                k_query_metrics[rank]['F1'] = f1
            if is_relevant:
                precision_sum += precision
                graph_details[q_id][recall] = precision
            if rank == n:
                r = retrieved_and_relevant
            rank += 1
        k_overall_metrics[q_id] = k_query_metrics
        if n == 0:
            avg_precision = 0
        else:
            avg_precision = precision_sum / n
        query_metrics['Average Precision'] = avg_precision
        r_precision = r / n
        query_metrics['R-Precision'] = r_precision
        dcg = 0
        i = 1
        for score in relevance:
            if i == 1:
                dcg += score
            else:
                dcg += score / math.log(i)
            i += 1
        idcg = 0
        i = 1
        sorted_relevance = sorted(relevance, reverse=True)
        for score in sorted_relevance:
            if i == 1:
                idcg += score
            else:
                idcg += score / math.log(i)
            i += 1
        if idcg == 0:
            nDCG = 0
        else:
            nDCG = dcg / idcg
        query_metrics['nDCG'] = nDCG
        metrics[q_id] = query_metrics
        if q:
            print("Metrics for Query #: ", q_id)
            print("\n")
            print("R-Precision is: {:.4f}".format(r_precision))
            print("Average Precision is: {:.4f}".format(avg_precision))
            print("dDCG is: {:.4f}".format(nDCG))
            print("\n")
            print("Precision@ values: ")
            print("Precision at 5: {:.4f}".format(k_query_metrics[5]['Precision']))
            print("Precision at 10: {:.4f}".format(k_query_metrics[10]['Precision']))
            print("Precision at 20: {:.4f}".format(k_query_metrics[20]['Precision']))
            print("Precision at 50: {:.4f}".format(k_query_metrics[50]['Precision']))
            print("Precision at 100: {:.4f}".format(k_query_metrics[100]['Precision']))
            print("\n")
            print("Recall@ values: ")
            print("Recall at 5: {:.4f}".format(k_query_metrics[5]['Recall']))
            print("Recall at 10: {:.4f}".format(k_query_metrics[10]['Recall']))
            print("Recall at 20: {:.4f}".format(k_query_metrics[20]['Recall']))
            print("Recall at 50: {:.4f}".format(k_query_metrics[50]['Recall']))
            print("Recall at 100: {:.4f}".format(k_query_metrics[100]['Recall']))
            print("\n")
            print("F1@ values: ")
            print("F1 at 5: {:.4f}".format(k_query_metrics[5]['F1']))
            print("F1 at 10: {:.4f}".format(k_query_metrics[10]['F1']))
            print("F1 at 20: {:.4f}".format(k_query_metrics[20]['F1']))
            print("F1 at 50: {:.4f}".format(k_query_metrics[50]['F1']))
            print("F1 at 100: {:.4f}".format(k_query_metrics[100]['F1']))
            print("\n")
            print("*******************************************************************")
    with open('graph_details.pickle', 'wb') as handle:
        pickle.dump(graph_details, handle, protocol=pickle.HIGHEST_PROTOCOL)
    total_avg_precision = 0
    total_r_precision = 0
    total_dDCG = 0
    total_k_metrics = OrderedDict()
    for qu, m in metrics.items():
        total_avg_precision += m['Average Precision']
        total_r_precision += m['R-Precision']
        total_dDCG += m['nDCG']
    total_avg_precision = total_avg_precision / num_queries
    total_r_precision = total_r_precision / num_queries
    total_dDCG = total_dDCG / num_queries
    total_k_metrics = OrderedDict()
    for each in k_values:
        total_k_metrics[each] = {'Precision': 0, 'Recall': 0, 'F1': 0}
    for qu, m in k_overall_metrics.items():
        for rnk, m_rank in m.items():
            total_k_metrics[rnk]['Precision'] += m_rank['Precision']
            total_k_metrics[rnk]['Recall'] += m_rank['Recall']
            total_k_metrics[rnk]['F1'] += m_rank['F1']
    print("Metrics for all queries (averaged): ")
    print("\n")
    print("R-Precision is: {:.4f}".format(total_r_precision))
    print("Average Precision is: {:.4f}".format(total_avg_precision))
    print("dDCG is: {:.4f}".format(total_dDCG))
    print("\n")
    print("Precision@ values: ")
    print("Precision at 5: {:.4f}".format(total_k_metrics[5]['Precision'] / num_queries))
    print("Precision at 10: {:.4f}".format(total_k_metrics[10]['Precision'] / num_queries))
    print("Precision at 20: {:.4f}".format(total_k_metrics[20]['Precision'] / num_queries))
    print("Precision at 50: {:.4f}".format(total_k_metrics[50]['Precision'] / num_queries))
    print("Precision at 100: {:.4f}".format(total_k_metrics[100]['Precision'] / num_queries))
    print("\n")
    print("Recall@ values: ")
    print("Recall at 5: {:.4f}".format(total_k_metrics[5]['Recall'] / num_queries))
    print("Recall at 10: {:.4f}".format(total_k_metrics[10]['Recall'] / num_queries))
    print("Recall at 20: {:.4f}".format(total_k_metrics[20]['Recall'] / num_queries))
    print("Recall at 50: {:.4f}".format(total_k_metrics[50]['Recall'] / num_queries))
    print("Recall at 100: {:.4f}".format(total_k_metrics[100]['Recall'] / num_queries))
    print("\n")
    print("F1@ values: ")
    print("F1 at 5: {:.4f}".format(total_k_metrics[5]['F1'] / num_queries))
    print("F1 at 10: {:.4f}".format(total_k_metrics[10]['F1'] / num_queries))
    print("F1 at 20: {:.4f}".format(total_k_metrics[20]['F1'] / num_queries))
    print("F1 at 50: {:.4f}".format(total_k_metrics[50]['F1'] / num_queries))
    print("F1 at 100: {:.4f}".format(total_k_metrics[100]['F1'] / num_queries))
    print("\n")
    print("*******************************************************************")


def main(argv):
    try:
        if len(sys.argv) == 3:
            option = 0
            qrel_path = sys.argv[1]
            results_path = sys.argv[2]
        else:
            option = 1
            qrel_path = sys.argv[2]
            results_path = sys.argv[3]

        parse_qrel(qrel_path)
        parse_results(results_path)
        evaluate(option)

    except:
        print("Input the qrel file and the ranked list file.")


if __name__ == "__main__":
    main(sys.argv)
