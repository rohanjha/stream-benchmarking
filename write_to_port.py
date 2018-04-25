import socket
import time
import numpy as np

TCP_IP = 'localhost'
TCP_PORT = 9999

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((TCP_IP, TCP_PORT))

while True:
    rand_int = np.random.normal()
    s.send((str(rand_int) + "\n").encode())
    time.sleep(0.05)
