import os
import re
import zlib
from collections import OrderedDict


def list_files(path):
    return [os.path.join(path, f) for f in os.listdir(path)]


def load_catalog(filename):
    catalog = OrderedDict()
    with open(filename, encoding="ISO-8859-1", errors='ignore') as f:
        for line in f:
            line_list = re.split(r'[:,]', line)
            catalog[line_list[0].strip()] = (line_list[1].strip(), line_list[2].strip())
    return catalog


def merge_indices(t, merge_path, m_no, cat1_list, ind1_path, cat2_list, ind2_path):
    merged_cat = open(merge_path + "merged" + str(m_no) + "_catalog.txt", "x")
    merged_ind = open(merge_path + "merged" + str(m_no) + "_index.txt", "wb")
    p1 = 0
    p2 = 0
    new_offset = 0
    while p1 < len(cat1_list) and p2 < len(cat2_list):
        if cat1_list[p1][0] == cat2_list[p2][0]:
            term = cat1_list[p1][0].strip()
            cat1_start = cat1_list[p1][1][0]
            cat1_len = cat1_list[p1][1][1]
            cat2_start = cat2_list[p2][1][0]
            cat2_len = cat2_list[p2][1][1]
            if t == 1:
                f1 = open(ind1_path, "r")
            else:
                f1 = open(ind1_path, "rb")
            f2 = open(ind2_path, "r")
            f1.seek(int(cat1_start))
            f2.seek(int(cat2_start))
            p1_doc_info = f1.read(int(cat1_len))
            p2_doc_info = f2.read(int(cat2_len))
            f1.close()
            f2.close()
            if t != 1:
                p1_doc_info = zlib.decompress(p1_doc_info)
                p1_doc_info = str(p1_doc_info, 'utf-8')
            new_info = p1_doc_info + p2_doc_info
            new_info_c = zlib.compress(new_info.encode('utf-8'), 6)
            merged_cat.write(term + ":" + str(new_offset).strip() + "," + str(len(new_info_c)) + "\n")
            merged_ind.write(new_info_c)
            p1 += 1
            p2 += 1
            new_offset += len(new_info_c)
        elif cat1_list[p1][0] > cat2_list[p2][0]:
            term = cat2_list[p2][0].strip()
            cat2_start = cat2_list[p2][1][0]
            cat2_len = cat2_list[p2][1][1]
            f2 = open(ind2_path, "r")
            f2.seek(int(cat2_start))
            p2_doc_info = f2.read(int(cat2_len))
            f2.close()
            p2_doc_info_c = zlib.compress(p2_doc_info.encode('utf-8'), 6)
            merged_cat.write(term + ":" + str(new_offset).strip() + "," + str(len(p2_doc_info_c)) + "\n")
            merged_ind.write(p2_doc_info_c)
            p2 += 1
            new_offset += len(p2_doc_info_c)
        else:
            term = cat1_list[p1][0].strip()
            cat1_start = cat1_list[p1][1][0]
            cat1_len = cat1_list[p1][1][1]
            if t == 1:
                f1 = open(ind1_path, "r")
            else:
                f1 = open(ind1_path, "rb")
            f1.seek(int(cat1_start))
            p1_doc_info = f1.read(int(cat1_len))
            f1.close()
            if t == 1:
                p1_doc_info = zlib.compress(p1_doc_info.encode('utf-8'), 6)
            merged_cat.write(term + ":" + str(new_offset).strip() + "," + str(len(p1_doc_info)) + "\n")
            merged_ind.write(p1_doc_info)
            p1 += 1
            new_offset += len(p1_doc_info)
    while p1 < len(cat1_list):
        term = cat1_list[p1][0].strip()
        cat1_start = cat1_list[p1][1][0]
        cat1_len = cat1_list[p1][1][1]
        if t == 1:
            f1 = open(ind1_path, "r")
        else:
            f1 = open(ind1_path, "rb")
        f1.seek(int(cat1_start))
        p1_doc_info = f1.read(int(cat1_len))
        f1.close()
        if t == 1:
            p1_doc_info = zlib.compress(p1_doc_info.encode('utf-8'), 6)
        merged_cat.write(term + ":" + str(new_offset).strip() + "," + str(len(p1_doc_info)) + "\n")
        merged_ind.write(p1_doc_info)
        p1 += 1
        new_offset += len(p1_doc_info)
    while p2 < len(cat2_list):
        term = cat2_list[p2][0].strip()
        cat2_start = cat2_list[p2][1][0]
        cat2_len = cat2_list[p2][1][1]
        f2 = open(ind2_path, "r")
        f2.seek(int(cat2_start))
        p2_doc_info = f2.read(int(cat2_len))
        f2.close()
        p2_doc_info_c = zlib.compress(p2_doc_info.encode('utf-8'), 6)
        merged_cat.write(term + ":" + str(new_offset).strip() + "," + str(len(p2_doc_info_c)) + "\n")
        merged_ind.write(p2_doc_info_c)
        p2 += 1
        new_offset += len(p2_doc_info_c)
    merged_cat.close()
    merged_ind.close()


catalogs_path = 'catalogs/'
indices_path = 'indices/'
merging_path = 'merged/'
catalog_files = list_files(catalogs_path)
index_files = list_files(indices_path)
full_catalog_file = catalog_files[0]
full_catalog = load_catalog(full_catalog_file)
full_catalog_items = list(full_catalog.items())
full_index_file = index_files[0]
merge_count = 1
t = 1
for i in range(1, len(catalog_files)):
    partial_catalog_file = catalog_files[i]
    partial_catalog = load_catalog(partial_catalog_file)
    partial_catalog_items = list(partial_catalog.items())
    partial_index_file = index_files[i]
    merge_indices(t, merging_path, merge_count, full_catalog_items, full_index_file, partial_catalog_items,
                  partial_index_file)
    full_catalog_file = merging_path + "merged" + str(merge_count) + "_catalog.txt"
    full_catalog = load_catalog(full_catalog_file)
    full_catalog_items = list(full_catalog.items())
    full_index_file = merging_path + "merged" + str(merge_count) + "_index.txt"
    merge_count += 1
    t += 1
