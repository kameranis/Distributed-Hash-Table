#!/usr/bin/env python
import random
import socket
import time
import sys

from server import Server
from server_master import Server_master 
from client import Client
from multiprocessing import Process, Queue

import logging
logging.basicConfig(filename='debug.log',level=logging.DEBUG)

help_dict = {'join':'Number, join', 'depart':'Number, depart', 'DHT destroy':'exit', 'print DHT': 'print'}

def g(q):
    k = Server_master('1')
    q.put(('1', k.get_port()))
    k.accept_connection()
    sys.exit()

    
def f(temp, main_port, q):
    p = Server(temp, main_port)
    p.DHT_join()
    q.put((temp,p.get_port()))
    logging.debug('Server '+temp+ ' started...')
    p.accept_connection()
    sys.exit()           

if __name__ == '__main__':
    p=[]
    hosts_and_ports = {}
    q = Queue()
    p.append(Process(target=g,args=(q,)))
    p[0].start()
    host, port = q.get()
    hosts_and_ports[host] = port
    main_port = port
    logging.debug('Server 1 started...')
    time.sleep(2)
    i=1
    print "''''''''''''''''''''''''''''''''''''''''"
    print '---COMMAND LIST---'
    for key, value in help_dict.iteritems():
        print ': '.join([key,value])
    print "''''''''''''''''''''''''''''''''''''''''"

    while True:
        inp=raw_input('Action: ').split(', ')
        if inp[0] == 'print':
            with Client(hosts_and_ports['1']) as cli:
                print cli.make_query('print')

        elif inp[0] == 'exit':
            x = Client(hosts_and_ports['1'])
            x.send_info('bye')
            x.close_connection()
            break
        elif inp[1]=='join':
            temp=inp[0]
            p.append(Process(target=f, args=(temp,main_port, q,)))
            p[i].start()
            t, port = q.get()
            hosts_and_ports[t]=port
            i+=1
        elif inp[1] == 'insert':
            host = random.sample(hosts_and_ports, 1)[0]
            port = hosts_and_ports[host]
            with Client(port) as cli:
                cli.make_query('insert:-1:-1:{}:{}'.format(inp[0], inp[2]))
       
        else:
            x = Client(hosts_and_ports[inp[0]])
            x.send_info('depart')
            x.close_connection()
            
        time.sleep(2)

    for j in xrange(i):
        p[j].join()
        
    logging.debug('END')


