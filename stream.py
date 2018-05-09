from __future__ import print_function

import sys
import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import socket
import pickle
import ast
import csv
import numpy as np

from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint

from pyspark.mllib.clustering import KMeans, KMeansModel

testing = False
model = None

if (testing):
    print_interval = 100
else:
    print_interval = 500

def get_labeled_points():
    train_data = []
    with open('data/training.csv', 'rb') as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            attributes = []
            for i in range(len(row) - 1):
                attributes.append(float(row[i]))
            train_data.append(LabeledPoint(int(row[len(row) - 1]), attributes))
    return train_data

def get_attributes():
    train_data = []
    with open('data/training.csv', 'rb') as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            attributes = []
            for i in range(len(row) - 1):
                attributes.append(float(row[i]))
            train_data.append(attributes)
    return train_data


# training the model if training is required
def train(model_name, sc):
    if (model_name == "logistic"):
        train_data = get_labeled_points()
        model = LogisticRegressionWithLBFGS.train(sc.parallelize(train_data))
        print("Done training.")
        print("Sample prediction: " + str(model.predict([0, 0, 0, 0, 0, 9])))
    elif (model_name == "kmeans"):
        train_data = get_attributes()
        model = KMeans.train(sc.parallelize(train_data), 2, maxIterations=10, initializationMode="random")
        print("Done training.")
        print(model.clusterCenters)
    elif (not model_name == "baseline"):
        print("Model not implemented", file=sys.stderr)
        exit(-1)

# processing the data
def process(val, model_name):
    if (model_name == "baseline"):
        return val[1] > 0.8
    elif (model_name == "logistic"):
        return model.predict(val[1:len(val)-1])
    elif (model_name == "kmeans"):
        attributes = np.array(val[1:len(val)-1])
        min_distance = np.min(np.linalg.norm(model.clusterCenters - attributes, axis=1))
        return min_distance >= 0.8

    # shouldn't get here
    return 0

# sanity check
def print_dp(param):
    if (int(param[0]) % print_interval == 0):
       print(str(param))

def send_partition_to_db(partition, out_port):
    list_of_records = []
    for record in partition:
        list_of_records.append(record)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(pickle.dumps(list_of_records), ('localhost', out_port))
    sock.close()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: network_wordcount.py <host name> <in port> <local out port> <model>", file=sys.stderr)
        exit(-1)

    out_port = int(sys.argv[3])
    model_name = sys.argv[4]

    sc = SparkContext(appName="PythonStreamingNetworkWordCount")
    ssc = StreamingContext(sc, 1)

    # wil also verify the model name
    train(model_name, sc)

    buckets = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    buckets = buckets.map(lambda val: ast.literal_eval(val)) # converting to strings
    buckets = buckets.map(lambda val: val + [int(round(time.time() * 1e8))]) # adding pre-processing timestamp
    buckets = buckets.map(lambda val: val + [process(val, model)]) # processing
    buckets = buckets.map(lambda val: val + [int(round(time.time() * 1e8))])  # adding post-processing timestamp
    buckets.foreachRDD(lambda rdd: rdd.foreachPartition(lambda partition: send_partition_to_db(partition, out_port))) # storage

    # storage side will take care of the post-storage timestamps

    # sanity check
    buckets.foreachRDD(lambda rdd: rdd.foreach(print_dp))

    ssc.start()
    ssc.awaitTermination()
