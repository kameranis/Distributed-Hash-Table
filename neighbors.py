from client import Client

import time
import sys
import logging
logging.basicConfig(filename='debug.log',level=logging.DEBUG)

''' Class tha implements the neighbors of a server in DHT '''

class Neighbors(object):

    def __init__(self,prev_port,next_port,b_port,n_port):
        self.back = Client(prev_port)
        self.front = Client(next_port)
        self.back_port = b_port
        self.front_port = n_port

    def destroy(self,port):
        if port != self.back_port:
            self.back.close_connection()
        if port != self.front_port:
            self.front.close_connection()
        logging.debug('Neighbors closed gracefully')
        
    def create_back(self,new_back,port1,myhash):
        self.back = Client(new_back)
        self.back.make_query('next:' + str(port1)+ ':' + myhash)
        self.back_port = new_back
        logging.debug('Back neighbor created')
        
    def create_front(self,new_front,port1,myhash):
        self.front = Client(new_front)
        self.front.make_query('prev:' + str(port1)+ ':' + myhash)
        self.front_port = new_front
        logging.debug('Front neighbor created')

    def update_back(self,myport,new_back_port):
        if self.back_port != myport:
            self.back.close_connection()
            
        self.back_port = new_back_port
        if myport!=new_back_port:
            self.back = Client(self.back_port)
        logging.debug('Back neighbor updated')
        
    def update_front(self,myport,new_front_port):
        if self.front_port != myport:
            self.front.close_connection()
            
        self.front_port = new_front_port
        if myport!=new_front_port:
            self.front = Client(self.front_port)
        logging.debug('Front neighbor updated')

    def send_back(self,data):
        return self.back.make_query(data)

    def send_front(self,data):
        return self.front.make_query(data)

    def get_front(self):
        return self.front_port

    def get_back(self):
        return self.back_port

''' Some usefull functions '''

def find_neighbors(hash_value,PORT):
    y = Client(PORT)
    x = y.make_query('join:' + hash_value).split()
    x[0] = int(x[0])
    x[2] = int(x[2])
    y.close_connection()
    logging.debug('Neighbors found: '+str(x[0]) + ' ' +str(x[2]))
    
    return x
                                
def send_request(PORT,message):
    x = Client(PORT)
    x.make_query(message)
    x.close_connection()
    
def send_request2(PORT,message):
    x = Client(PORT)
    x.send_info(message)
    x.close_connection()
    
def close_server(port):
    cl = Client(port)
    logging.debug('Shut Client ON')
    cl.close_and_shut()
    sys.exit()

def DHT_close(self,size,port):
    x = size
    for i in xrange(x-1):
        logging.debug('NEXT PREPARE TO GET DESTROYED')
        send_request2(port, 'depart')
        time.sleep(1)
        logging.debug('MY NEXT IS DEAD NOW BUAHAHA')
    #self.s.close()
    #del self.neighbors
    logging.debug('Shutdown complete')
    #sys.exit()

                                                                                                            
