from __future__ import print_function

import sys
import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import socket
import pickle

testing = False

if (testing):
    print_interval = 100
else:
    print_interval = 500

# sanity check
def print_dp(param):
    if (int((param[0].split(" "))[0]) % print_interval == 0):
        print(str(param[0]) + " " + str(param[1]) + " " + str(param[2]) + " " + str(param[3]))

def send_partition_to_db(partition, out_port):
    list_of_records = []
    for record in partition:
        list_of_records.append(record)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(pickle.dumps(list_of_records), ('localhost', out_port))
    sock.close()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: network_wordcount.py <host name> <in port> <local out port>", file=sys.stderr)
        exit(-1)

    out_port = int(sys.argv[3])

    sc = SparkContext(appName="PythonStreamingNetworkWordCount")
    ssc = StreamingContext(sc, 1)

    buckets = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    w_1_ts = buckets.map(lambda val: [val, str(int(round(time.time() * 1e8)))]) # adding pre-processing timestamp
    w_counts = w_1_ts.map(lambda val: [val[0], val[1], int(val[0].split(" ")[1]) > 22000]) # processing
    w_2_ts = w_counts.map(lambda val: [val[0], val[1], str(int(round(time.time() * 1e8))), val[2]]) # adding post-processing and pre-storage timestamp
    w_2_ts.foreachRDD(lambda rdd: rdd.foreachPartition(lambda partition: send_partition_to_db(partition, out_port))) # storing

    # storage side will take care of the post-storage timestamps

    # sanity check
    w_2_ts.foreachRDD(lambda rdd: rdd.foreach(print_dp))

    ssc.start()
    ssc.awaitTermination()
