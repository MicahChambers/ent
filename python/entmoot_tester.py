#!/usr/bin/python3

import sys
import asyncore
import json
import logging
import subprocess
import asynchat
import copy
import socket
import time
import re

RUNNING = 123
STOPPED = 124
DEAD = 283
FAIL = 2398
WAITING = 293

class EntCommunicator(asynchat.async_chat):
    """Sends messages to the server and receives response.
    """
    def __init__(self, host, port):
        self.received_data = []
        self.logger = logging.getLogger('EntCommunicator')
        asynchat.async_chat.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.logger.debug('connecting to %s', (host, port))
        self.connect((host, port))
        self.response = {}

    def handle_connect(self):
        self.logger.debug('handle_connect()')
        self.set_terminator(b'\n')

    def collect_incoming_data(self, data):
        """Read an incoming message from the server"""
        self.logger.debug('collect_incoming_data() -> (%d)\n"""%s"""', len(data), data)
        self.received_data.append(data)

    def found_terminator(self):
        self.logger.debug('found_terminator()')
        received_message = ''.join([bstr.decode("utf-8") for bstr in self.received_data])
        received_message = json.loads(received_message)
        self.response = dict(list(self.response.items()) + list(received_message.items()))
        self.waitfor -= 1
        if self.waitfor == 0:
            self.close()

    def sendrecieve(self, reqlist):
        if type(reqlist) != type([]):
            reqlist = [reqlist]

        self.waitfor = 0
        for req in reqlist:
            req = req.strip()
            if len(req) == 0:
                continue
            comm.push(bytearray(req+'\n', 'utf-8'))
            self.waitfor += 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(name)s: %(message)s',)
    host = 'localhost'
    port = 12345
    try:
        host = sys.argv[1]
        port = int(sys.argv[2])
    except:
        pass

    try:
        comm = EntCommunicator(host, port)
    except Exception as e:
        print("Error:", e)
        sys.exit(-1)

    comm.sendrecieve(['USER micahc', 'USER root'])
    asyncore.loop()
    print(comm.response)
    sys.exit(0)
