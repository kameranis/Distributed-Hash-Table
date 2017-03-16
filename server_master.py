#!/usr/bin/env python

import socket
import select
import Queue
import time
import sys
import threading
import logging

from server import Server
from neighbors import Neighbors, send_request
from hashlib import sha1

logging.basicConfig(filename='debug.log',level=logging.ERROR)

class Server_master(Server):
    def __init__(self, HOST, repl):
        self._network_size = 1
        self.close = False
        super(Server_master, self).__init__(HOST,-1)
        self.replication = int(repl)

    def _join(self,data,sock):
        self._network_size +=1
        super(Server_master, self)._join(data,sock)
        self.message_queues[sock].put(self.message_queues[sock].get() + ':' + str(self.replication))
        
    def _depart(self,data,sock):
        self._network_size -= 1
        self.message_queues[sock].put('Done')
        
    def _bye(self,data,sock):
        if self._network_size > 1:
            t = threading.Thread(target=send_request, args=(self.neighbors.front_port, 'bye',))
            t.start()
        else:
            self.close = True
        self.message_queues[sock].put('Done')
    
