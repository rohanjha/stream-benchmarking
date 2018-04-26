from __future__ import print_function

import sys
import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# sanity check
def print_dp(param):
    print(str(param[0]) + " " + str(param[1]) + " " + str(param[2]) + " " + str(param[3]))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: network_wordcount.py <hostname> <port>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="PythonStreamingNetworkWordCount")
    ssc = StreamingContext(sc, 1)

    buckets = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    w_1_ts = buckets.map(lambda val: [val, str(int(round(time.time() * 1e8)))]) # adding pre-processing timestamp
    w_counts = w_1_ts.map(lambda val: [val[0], val[1], abs(float(val[0].split(" ")[1])) > 2]) # processing
    w_2_ts = w_counts.map(lambda val: [val[0], val[1], str(int(round(time.time() * 1e8))), val[2]]) # adding post-processing and pre-storage timestamp
    w_2_ts.saveAsTextFiles("text") # storing

    # storage side will take care of the pos-storage timestamps

    # sanity check
    w_2_ts.foreachRDD(lambda rdd: rdd.foreach(print_dp))

    ssc.start()
    ssc.awaitTermination()
