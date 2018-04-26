from __future__ import print_function

import SocketServer
import sys
import pickle
import sqlite3
import time

conn = sqlite3.connect("results.db")
c = conn.cursor()

# NOTE: we often want to drop our previous results
c.execute('''CREATE TABLE IF NOT EXISTS results (dp text, ts1 bigint, ts2 bigint, result bit)''')
conn.commit()
conn.close()

ts_4 = []

class UDPHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        # self.request is the UDP socket connected to the client
        print("Connection accepted from " + str(self.client_address[1]))
        data = pickle.loads(self.request[0].strip())
        print("Sample: " + str(data[0]))

        conn = sqlite3.connect("results.db")
        c = conn.cursor()
        c.executemany('''INSERT INTO results VALUES (?, ?, ?, ?)''', data)
        conn.commit()
        conn.close()

        first_id = int(data[0][0].split(" ")[0])
        ts_4.append((first_id, int(round(time.time() * 1e8))))

if __name__ == "__main__":
    try:
        PORT = 9999
        if (len(sys.argv) > 1):
        	PORT = int(sys.argv[1])

        HOST = 'localhost'

        # Create the server, binding to localhost on port 9999
        server = SocketServer.UDPServer((HOST, PORT), UDPHandler)

        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C
        server.serve_forever()
    finally:
        f = open('results.txt', 'w')
        f.write(str(ts_4) + "\n")
        f.close()
