import os
import re
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


def merge_indices(merge_path, merge_no, cat1, ind1_path, cat2, ind2_path):
    merged_cat_path = f"{merge_path}merged{merge_no}_catalog.txt"
    merged_ind_path = f"{merge_path}merged{merge_no}_index.txt"
    with open(merged_cat_path, "x") as merged_cat, open(merged_ind_path, "x") as merged_ind:
        i, j = 0, 0
        offset = 0
        while i < len(cat1) and j < len(cat2):
            term1, (start1, len1) = cat1[i]
            term2, (start2, len2) = cat2[j]
            if term1 == term2:
                with open(ind1_path, "r") as f1, open(ind2_path, "r") as f2:
                    f1.seek(int(start1))
                    f2.seek(int(start2))
                    info1 = f1.read(int(len1))
                    info2 = f2.read(int(len2))
                new_info = info1 + info2
                merged_cat.write(f"{term1}:{offset},{len(new_info)}\n")
                merged_ind.write(new_info)
                i += 1
                j += 1
                offset += len(new_info)
            elif term1 > term2:
                with open(ind2_path, "r") as f2:
                    f2.seek(int(start2))
                    info2 = f2.read(int(len2))
                merged_cat.write(f"{term2}:{offset},{len(info2)}\n")
                merged_ind.write(info2)
                j += 1
                offset += len(info2)
            else:
                with open(ind1_path, "r") as f1:
                    f1.seek(int(start1))
                    info1 = f1.read(int(len1))
                merged_cat.write(f"{term1}:{offset},{len(info1)}\n")
                merged_ind.write(info1)
                i += 1
                offset += len(info1)
        while i < len(cat1):
            term, (start, len_) = cat1[i]
            with open(ind1_path, "r") as f1:
                f1.seek(int(start))
                info = f1.read(int(len_))
            merged_cat.write(f"{term}:{offset},{len(info)}\n")
            merged_ind.write(info)
            i += 1
            offset += len(info)
        while j < len(cat2):
            term, (start, len_) = cat2[j]
            with open(ind2_path, "r") as f2:
                f2.seek(int(start))
                info = f2.read(int(len_))
            merged_cat.write(f"{term}:{offset},{len(info)}\n")
            merged_ind.write(info)
            j += 1
            offset += len(info)


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

for i in range(1, len(catalog_files)):
    partial_catalog_file = catalog_files[i]
    partial_catalog = load_catalog(partial_catalog_file)
    partial_catalog_items = list(partial_catalog.items())
    partial_index_file = index_files[i]
    merge_indices(merging_path, merge_count, full_catalog_items, full_index_file, partial_catalog_items,
                  partial_index_file)
    full_catalog_file = merging_path + "merged" + str(merge_count) + "_catalog.txt"
    full_catalog = load_catalog(full_catalog_file)
    full_catalog_items = list(full_catalog.items())
    full_index_file = merging_path + "merged" + str(merge_count) + "_index.txt"
    merge_count += 1
