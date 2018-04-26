import socket
import time
import numpy as np
import sys
import csv

TCP_PORT = 9999
if (len(sys.argv) > 1):
	TCP_PORT = int(sys.argv[1])

TCP_IP = 'localhost'

f = open("nyc_taxi-train.csv")
reader = csv.reader(f)

data = []
for row in reader:
	data.append(row[2])
data.pop(0)

s = socket.socket()
s.bind((TCP_IP, TCP_PORT))
s.listen(5)
c, addr = s.accept()

print("Connection accepted from " + repr(addr[1]))
index = 0
while True:
    c, addr
    c.send((str(index) + " " + str(data[index % len(data)]) + "\n").encode())
    time.sleep(0.01)
    index += 1
