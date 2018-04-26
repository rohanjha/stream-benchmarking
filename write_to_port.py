import socket
import time
import numpy as np
import sys

TCP_PORT = 9999
if (len(sys.argv) > 1):
	TCP_PORT = int(sys.argv[1])

TCP_IP = 'localhost'

s = socket.socket()
s.bind((TCP_IP, TCP_PORT))
s.listen(5)
c, addr = s.accept()

print("Connection accepted from " + repr(addr[1]))
id = 0
while True:
    c, addr
    rand_int = np.random.normal()
    c.send(str(id) + " " + (str(rand_int) + "\n").encode())
    time.sleep(0.05)
    id += 1
