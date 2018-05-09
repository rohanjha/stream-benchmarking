from __future__ import print_function
import sys
import csv

ids_to_ts2 = []
ids_to_ts3 = []
diffs = []

if __name__ == "__main__":
    if (len(sys.argv) != 3):
        print("usage: <ts2 file> <ts3 file>")
        exit()

    ts2_file_name = sys.argv[1]
    ts3_file_name = sys.argv[2]

    with open(ts2_file_name, 'rb') as ts2_file:
        ts2_reader = csv.reader(ts2_file)
        for row in ts2_reader:
            id = int(row[0])
            ts2 = int(row[1])
            ids_to_ts2.append((id, ts2))

    with open(ts3_file_name, 'rb') as ts3_file:
        ts3_reader = csv.reader(ts3_file)
        for row in ts3_reader:
            id = int(row[0])
            ts3 = int(row[1])
            ids_to_ts3.append((id, ts3))

    for tuple_ts2 in ids_to_ts2:
        id_ts2 = tuple_ts2[0]

        # we want the ts3 of the largest id <= to this one
        bucket_ts3 = None
        for tuple_ts3 in reversed(ids_to_ts3):
            if tuple_ts3[0] <= id_ts2:
                bucket_ts3 = tuple_ts3
                break

        diffs.append(bucket_ts3[1] - tuple_ts2[1])

    print(sum(diffs) / float(len(diffs)))
