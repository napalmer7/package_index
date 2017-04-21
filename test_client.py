"""
Simple test client to run some sample commands against the package service.

It supports multiple processes to execute the same command set. This could be
extended to use multiple sets (one for each process)
"""

import logging
import socket
from multiprocessing import Process

from thread_server import HOST, PORT

logging.basicConfig(level=logging.INFO,
                    format="%(created)-15s %(msecs)d %(levelname)8s %(thread)d %(name)s %(message)s")
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    num_clients = 1  # Increase this to do parallel clients
    data = """INDEX|cloog|gmp,isl,pkg-config
INDEX|ceylon|
QUERY|cloog|
REMOVE|cloog|
QUERY|cloog|
QUERY|ceylon|
INDEX|clooper|ceylon
REMOVE|ceylon|
REMOVE|clooper|
REMOVE|ceylon|"""

    def test_client(uid, data):
        """
        Method wrapper for client communication
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))
        logger.info("Connected to server")

        for line in data.splitlines():
            sock.sendall(line+'\n')
            logger.info("Instance {} sent: {}".format(uid, line))
            response = sock.recv(8192)
            logger.info("Instance {} received: {}".format(uid, response))
        sock.close()
    procs = []
    for num in range(num_clients):
        p = Process(target=test_client, args=(num, data))
        procs.append(p)
        p.start()

    for x in procs:
        x.join()
