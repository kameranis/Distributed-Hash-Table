#!/usr/bin/env python

import socket
import time
from server import Server
from server_master import Server_master 
from client import Client
from multiprocessing import Process
import logging
logging.basicConfig(filename='debug.log',level=logging.DEBUG)

main_PORT=8882

def g():
    k = Server_master('1',main_PORT)
    k.accept_connection()

    
def f(temp):
    p = Server(temp,main_PORT)
    p.DHT_join()
    logging.debug('Server '+temp+ ' started...')
    p.accept_connection()
               
if __name__ == '__main__':
    #lock = Lock()
    p=[]
    
    p.append(Process(target=g,args=()))
    p[0].start()
    logging.debug('Server 1 started...')
    time.sleep(2)
    i=1
    while True:
        inp=raw_input('Number, query: ').split(', ')
        temp=inp[0]
        p.append(Process(target=f, args=(temp,)))
        p[i].start()
        i+=1
           
        time.sleep(2)

    for j in xrange(i):
        p[j].join()
        
    logging.debug('END')
#x = server('',8888)
#x.accept_connection()
#print x.get_port()



