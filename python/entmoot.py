#!/usr/bin/python3

import sys
import asyncore
import logging
import json
import subprocess
import asynchat
import copy
import socket
import time
import re

# { user : {pid : [info] } } + {pid : [info] }
database = dict()

RUNNING = 123
STOPPED = 124
DEAD = 283
FAIL = 2398
WAITING = 293

### Main Basically
def qsubmain(host, port):
    raise "qsubmain not yet implemented"

def psmain(host, port):
    global database

    # list of handlers
    handlers = dict()

    ####################
    # Start Listening
    ####################
    srvr = EntMootCreator(host, port)

    ####################
    # Update Database
    ####################
    # QSUB Format:
    # job-ID prior name user state submit/start at queue slots ja-task-ID

    psre = re.compile("([a-zA-Z0-9]*)\s+([0-9]*)\s+([a-zA-Z]*)\s+(.*)")
    pscmd = ['ps','-eo','user,pid,state,args']
    while True:

        # Create New Database
        newdb = dict()
        olddb = copy.deepcopy(database)
        lines = subprocess.check_output(pscmd, universal_newlines=True).split('\n')

        # in qsub ignore the first two lines
        for line in lines[1:]:
            m = psre.match(line)
            if m:
                user = m.group(1)
                pid = int(m.group(2))
                state = m.group(3)
                args = m.group(4)
                entry = [user, pid, state, args]
                newdb[pid] = entry
                if user in newdb:
                    newdb[user][pid] = entry
                else:
                    newdb[user] = {pid : entry}

        # update global database
        database = newdb
        asyncore.loop()
        #time.sleep(1)

class EntMootCommunicator(asynchat.async_chat):
    """Acts as a middle man for Hobbit's
    """

    def __init__(self, sock):
        self.received_data = []
        self.logger = logging.getLogger('EntMoot')
        asynchat.async_chat.__init__(self, sock)
        self.set_terminator(b'\n')
        return

    def collect_incoming_data(self, data):
        """Read an incoming message from the client and put it into queue."""
        self.logger.debug('collect_incoming_data() -> (%d bytes)\n"""%s"""', len(data), data)
        self.received_data.append(data)

    def found_terminator(self):
        """The end of a command or message has been seen."""
        self.logger.debug('found_terminator()')
        self.process_data()

    def process_data(self):
        """We have the full command"""
        self.logger.debug('_process_command()')
        print(self.received_data)
        cmd = ''.join([bstr.decode("utf-8") for bstr in self.received_data])
        self.received_data = []

        # Load database to full response
        global database

        # Fill Output Buffer with response
        outputbuffer = []
        cmd = cmd.split(' ')

        response = dict()
        if cmd[0] == 'USER':
            # Look Up User
            for c in cmd[1:]:
                if c and c in database:
                    for pid, data in database[c].items():
                        response[pid] = data

        elif cmd[0] == 'PID':
            # Look Up PID
            for c in cmd[1:]:
                print(c)
                if c and c in database:
                    response[c] = database[c]

        response = json.dumps(response)+'\n'
        self.logger.debug('response: %s' % response)
        self.push(bytearray(response, 'utf-8'))

class EntMootCreator(asyncore.dispatcher):
    """ Recieves connections and creates handler for each user
    """

    def __init__(self, host, port):
        asyncore.dispatcher.__init__(self)
        self.logger = logging.getLogger('EntMootCommunicator')
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.bind((host, port))
        self.address = self.socket.getsockname()
        self.listen(1)
        return

    def handle_accept(self):
        # Called when a client connects to our socket
        self.logger.debug('handle_accept()')
        client_info = self.accept()
        handler = EntMootCommunicator(sock=client_info[0])

    def handle_close(self):
        self.logger.debug('handle_close()')
        self.close()


if __name__ == "__main__":
    host = 'localhost'
    port = 12345
    try:
        host = sys.argv[1]
        port = int(sys.argv[2])
    except:
        pass

    try:
        psmain(host, port)
    except Exception as e:
        print("Error:", e)
        sys.exit(-1)
    sys.exit(0)
