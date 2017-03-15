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
from binascii import hexlify

logging.basicConfig(filename='debug.log',level=logging.ERROR)

class Server_master(Server):
    def __init__(self, HOST, repl):
        self._network_size = 1
        self.close = False
        super(Server_master, self).__init__(HOST,-1)
        self.replication = int(repl)
        self.operations['bye'] = self._bye
    ''' Usable functions are below '''

    def _join(self,data,sock):
        self._network_size +=1
        super(Server_master, self)._join(data,sock)
        self.message_queues[sock].put(self.message_queues[sock].get() + ':' + str(self.replication))
        
    def _depart(self,data,sock):
        # data = depart:Back_PORT:Back_hash:Next_PORT:Next_hash:Depart_PORT
        x = data.split(':')
        logging.debug('Depart starts for {}'.format(data))
        # if int(x[1]) == self.PORT:
        #     self.neighbors.update_front(x[4], int(x[3]))
        # else:
        #     threading.Thread(target=send_request, args=(int(x[1]),'next:'+x[3]+':'+x[4],)).start()
        # logging.debug('Prev updated')
        #
        # if int(x[3]) == self.PORT:
        #     self.neighbors.update_back(x[2], int(x[1]))
        # else:
        #     threading.Thread(target=send_request,args= (int(x[3]),'prev:'+x[1]+':'+x[2],)).start()
        # logging.debug('Next updated')
        # time.sleep(1)
        # # TODO : need a thread.join() here but its not a problem. Ignore it for now
        # self.message_queues[sock].put('Depart is completed')
        self._network_size -= 1
        logging.debug('Depart ends')
        
        
    def _bye(self,data,sock):
        if self._network_size > 1:
            t = threading.Thread(target=send_request, args=(self.neighbors.front_port, 'bye',))
            t.start()
        else:
            self.close = True
        self.message_queues[sock].put('Done')
    
    def _print(self,data,sock):
        if self._network_size > 1:
            message = self.HOST+str([value for key, value in self.data.iteritems()])+'->'+ self.neighbors.send_front('print:'+str(self._network_size-1))
            self.message_queues[sock].put(message)
        else:
            self.message_queues[sock].put(self.HOST+str([value for key, value in self.data.iteritems()]))
