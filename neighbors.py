

from client import Client

import logging
logging.basicConfig(filename='debug.log',level=logging.DEBUG)

class Neighbors(object):

    def __init__(self,prev_port,next_port,b_port,n_port):
        self.back = Client(prev_port)
        self.front = Client(next_port)
        self.back_port = b_port
        self.front_port = n_port

    def __del__(self):
        self.back.close_connection()
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


