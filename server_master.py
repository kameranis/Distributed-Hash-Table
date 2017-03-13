#!/usr/bin/env python

import socket
import select
import Queue
import time
import sys
import threading
import logging

from server import Server
from neighbors import Neighbors, send_request, close_server, send_request2
from hashlib import sha1
from binascii import hexlify

logging.basicConfig(filename='debug.log',level=logging.ERROR)

class Server_master(Server):
    def __init__(self, HOST):
        self._network_size = 1        
        self.close = False
        super(Server_master, self).__init__(HOST,-1)
        self.operations['bye'] = self._bye

    def _join(self,data,sock):
        self._network_size +=1
        super(Server_master, self)._join(data,sock)
        
    def _depart(self,data,sock):
        # data = depart:Back_PORT:Back_hash:Next_PORT:Next_hast:Depart_PORT
        x = data.split(':')
        logging.debug('Depart starts')
        if int(x[1]) == self.PORT:
            self.neighbors.update_front(self.PORT, int(x[3]))
            #t = threading.Thread(target=self.neighbors.update_front, args=(self.PORT, int(x[3])))
            self.N_hash = x[4]
        else:
            t = threading.Thread(target=send_request, args=(int(x[1]),'next:'+x[3]+':'+x[4],))
            t.start()
            t.join()
        logging.debug('Prev updated')
        if int(x[3]) == self.PORT:
            self.neighbors.update_back(self.PORT, int(x[1]))
            #t = threading.Thread(target=self.neighbors.update_back, args=(self.PORT, int(x[1])))
            self.P_hash = x[2]
        else:
            t = threading.Thread(target=send_request,args= (int(x[3]),'prev:'+x[1]+':'+x[2],))
            t.start()
            t.join()
        logging.debug('Next updated')
        threading.Thread(target=close_server, args = (int(x[5]),)).start()
        self.message_queues[sock].put('Your job is completed')
        self._network_size -= 1
        logging.debug('Depart ends')
        
        if self.close and (int(x[3]) == self.PORT) and (int(x[1]) == self.PORT):
            time.sleep(1)
            self.s.close()
            self.neighbors.destroy(self.PORT)
            logging.debug('Shutdown complete')
            return
        elif self.close and (int(x[3]) != self.PORT) and (int(x[1]) == self.PORT):
            logging.debug('Shutting down, closing {}'.format(self.N_hash))
            threading.Thread(target=send_request, args=(self.neighbors.front_port, 'depart')).start()
        
    def _bye(self,data,sock):
        self.close = True
        #threading.Thread(target=self.DHT_close, args=(self._network_size,)).start()
        threading.Thread(target=send_request, args=(self.neighbors.front_port, 'depart',)).start()

    def _print(self,data,sock):
        if self._network_size > 1:
            message = self.HOST+'->'+ self.neighbors.send_front('print:'+str(self._network_size-1))
            self.message_queues[sock].put(message)
        else:
            self.message_queues[sock].put(self.HOST)

    def _total_shut(self,next_msg,sock):
        self._quit(next_msg,sock)
