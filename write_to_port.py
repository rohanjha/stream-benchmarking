import socket
import time
import numpy as np
import sys
import csv
import pickle

TCP_PORT = 9999
if (len(sys.argv) > 1):
	TCP_PORT = int(sys.argv[1])

TCP_IP = 'localhost'

# not ideal but hardcoded for now
f = open("data/testing.csv")
reader = csv.reader(f)

data = []
for row in reader:
	data.append([float(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5])])

s = socket.socket()
s.bind((TCP_IP, TCP_PORT))
s.listen(5)
c, addr = s.accept()

print("Connection accepted from " + repr(addr[1]))
index = 0
while True:
    c, addr
    dp = data[index % len(data)]
    dp.insert(0, index)
    #print(pickle.dumps(dp))

    c.send(str(dp).encode() + "\n")
    # c.send(("hello").encode() + "\n")
    #c.send(pickle.dumps(dp))
    time.sleep(0.01)
    index += 1
