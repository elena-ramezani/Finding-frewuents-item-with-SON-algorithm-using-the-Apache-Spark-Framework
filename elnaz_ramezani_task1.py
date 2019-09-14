from collections import Counter
from itertools import combinations
import findspark

findspark.init("/usr/local/spark", edit_rc=True)
import json
import argparse
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from operator import add
import pdb
import time



def candidate_pair(s):
    candidate_size2 = []
    s = set(s)
    for item in combinations(s, 2): # take first one and combine with rest and so on
        item = list(item)
        item.sort()
        candidate_size2.append(item)
    return candidate_size2


def count_candidate_pair(baskets, pairs, threshold):
    freq_item = {}
    freq_set = list()
    baskets = list(baskets)

    for basket in baskets:
        for pair in pairs:
            candidate_pair
    for item in freq_item:
        if freq_item[item] > threshold:
            freq_set.append(item)

    freq_set = sorted(freq_set)

    return freq_set



def generate_more_candidate(freq_pair ,length_pair):

    subset_more_pair = list()
    lengthA = len(freq_pair) - 1
    lengthB = lengthA + 1
    for i in range(lengthA):
        for j in range(i + 1, lengthB):
            partA = freq_pair[i]
            partB = freq_pair[j]
            if partA[0:(length_pair - 2)] == partB[0:(length_pair - 2)]:
                subset_more_pair.append(list(set(partA) | set(partB)))
            else:
                break

    return subset_more_pair


def check_freq_more(candidate , data ,threshold):
    candidate = list(candidate)
    baskets = list(data)
    freq_count = dict()
    freq_more_item = list()

    for basket in baskets:
        for pair in candidate:

            pair = set(pair)
            newpair_sort = sorted(pair)
            pair_tuple = tuple(newpair_sort)

            if pair.issubset(basket):
                freq_count[pair_tuple] = freq_count.get(pair_tuple, 0) + 1


    for item in freq_count:
        if freq_count[item] > threshold:
            freq_more_item.append(item)

    return freq_more_item





def candidateCount(baskets, candidates):
    item_count = {}
    baskets = list(baskets)
    for candidate in candidates:

        if type(candidate) is str:


            candidate = [candidate]

            key = tuple(sorted(candidate))


        else:

            key = candidate

        candidate = set(candidate)

        for basket in baskets:
            if candidate.issubset(basket):
                item_count[key] = item_count.get(key, 0) + 1

    return item_count.items()


def apriori(chunk, support, total_num):
    num_item = 0
    single_freq = []
    chunk = list(chunk)
    firstItemCounts = Counter()  # count the number that each value is repeated
    for item in chunk:
        num_item += 1
        firstItemCounts.update(item)

    threshold = support * (float(num_item) / float(total_num))
    for x in firstItemCounts:

        if firstItemCounts[x] > threshold:
            single_freq.append(x)

    single_freq = sorted(single_freq)
    final_frequent_item= list()
    final_frequent_item.extend(single_freq)
    #print((final_frequent_item))

    candidate_item2 = candidate_pair(single_freq)

    # create candidate pair
    final_pair = count_candidate_pair(chunk, candidate_item2, threshold)
    final_frequent_item.extend(sorted(final_pair))
    #print(final_frequent_item)
    length_pair = 3
    freq_item = final_pair

    while len(freq_item) != 0:
        candidate_more_item = generate_more_candidate(freq_item, length_pair)

        freq_item = check_freq_more(candidate_more_item, chunk,threshold)
        final_frequent_item.extend(sorted(freq_item))


        length_pair = length_pair + 1
    print(final_frequent_item)

    return final_frequent_item


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='input')
    parser.add_argument('case', nargs='+', help='case')
    parser.add_argument('support', nargs='+', help='support')
    parser.add_argument('input_file', nargs='+', help='input')
    parser.add_argument('output_file', nargs='+',  help='output')


    args = parser.parse_args()
    case = args.case
    support = args.support
    input_file_name = args.input_file
    output_file_name = args.output_file


    case = int(case[0])
    support = int(support[0])



    sc = SparkContext("local[*]", "Elnaz App")
    spark = SparkSession(sc)
    start = time.time()
    rdd_csv = sc.textFile(input_file_name[0])



    if case == 1:
        mapped_rdd_by_busid = rdd_csv.map(lambda x: x.split(",")).map(lambda x: (x[0], x[1])).groupByKey()

    elif case == 2:
        mapped_rdd_by_busid = rdd_csv.map(lambda x: x.split(",")).map(lambda x: (x[1], x[0])).groupByKey()


    all_baskets = mapped_rdd_by_busid.map(lambda x: set(x[1])) #take all the values, remove keys
    number_chunk = all_baskets.count()

    output_phase_one = all_baskets.mapPartitions(lambda basket: apriori(basket, support,  number_chunk)).map(lambda x: (x, 1))
    output = output_phase_one.reduceByKey(lambda x, y: (1)).keys().collect()
   # cc = sorted(output, key=lambda x: (-x[1], x[0]))

    output_phase_two = all_baskets.mapPartitions(lambda baskets: candidateCount(baskets, output))
    output_two = output_phase_two.reduceByKey(lambda x, y: (x + y))
    out = output_two.sortBy(lambda x: (len(x[0]), x[0])).map(lambda x: x[0]).collect() # candidate

    final = output_two.filter(lambda x: x[1] >= support).sortBy(lambda x: (len(x[0]), x[0])).map(lambda x: x[0]).collect()

    len_candidate = len(out[0])
    file_open = open(output_file_name[0], 'w')

    if (len(out[0]) == 1):
        file_open.write('Candidates:\n')
        file_open.write('(' + str(out[0][0]) + ')')
    elif (len(out[0]) > 1):
        file_open.write('Candidates:\n')
        file_open.write(str(out[0][0]))

    for x in final[1:]:
        if len(x) == len_candidate:
            if len(x) == 1:
                temp = str(x[0])
                temp = temp.replace('\'', '')
                temp = temp.replace(' ', '')
                file_open.write(',(' + temp + ')')
            else:
                temp = str(x)
                temp = temp.replace('\'', '')
                temp = temp.replace(' ', '')
                file_open.write(',' + temp)
        else:
            temp = str(x)
            temp = temp.replace('\'', '')
            temp = temp.replace(' ', '')
            file_open.write('\n\n')
            file_open.write(temp)
            len_candidate = len(x)

    file_open.write('\n\n')
    len_final = len(final[0])



    if (len(final[0]) == 1):
        file_open.write('Frequent Itemsets:\n')
        file_open.write('(' + str(final[0][0]) + ')')
    elif (len(final[0]) > 1):
        file_open.write('Frequent Itemsets:\n')
        file_open.write(str(final[0][0]))

    for x in final[1:]:
        if len(x) == len_final:
            if len(x) == 1:
                temp = str(x[0])
                temp = temp.replace('\'', '')
                temp = temp.replace(' ', '')
                file_open.write(',(' + temp + ')')
            else:
                temp = str(x)
                temp = temp.replace('\'', '')
                temp = temp.replace(' ', '')
                file_open.write(',' + temp)
        else:
            temp = str(x)
            temp = temp.replace('\'', '')
            temp = temp.replace(' ', '')
            file_open.write('\n\n')
            file_open.write(temp)
            len_final = len(x)

    file_open.close()
    delta_time = time.time() - start
    print('Duration:',delta_time)



