from client import Client

import time
import sys
import logging
logging.basicConfig(filename='debug.log',level=logging.DEBUG)

''' Class tha implements the neighbors of a server in DHT '''

class Neighbors(object):

    def __init__(self,back_hash,back_port,front_hash,front_port):
        self.back_hash = back_hash
        self.front_hash = front_hash
        self.back_port = back_port
        self.front_port = front_port
        
    def create_back(self, new_back_hash, new_back_port, port1, myhash):
        Client(new_back_port).communication('next:' + str(port1)+ ':' + myhash)
        self.back_hash = new_back_hash
        self.back_port = new_back_port
        logging.debug('Back neighbor created')
        
    def create_front(self, new_front_hash, new_front_port, port1, myhash):
        Client(new_front_port).communication('prev:' + str(port1)+ ':' + myhash)
        self.front_hash = new_front_hash
        self.front_port = new_front_port
        logging.debug('Front neighbor created')

    def update_back(self, new_back_hash, new_back_port):
        self.back_hash = new_back_hash
        self.back_port = new_back_port
        logging.debug('Back neighbor updated')
        
    def update_front(self, new_front_hash, new_front_port):
        self.front_hash = new_front_hash
        self.front_port = new_front_port
        logging.debug('Front neighbor updated')

    def send_back(self,data):
        return Client(self.back_port).communication(data)

    def send_front(self,data):
        return Client(self.front_port).communication(data)

    def get_front(self):
        return str(self.front_port) + ':' + self.front_hash

    def get_back(self):
        return str(self.back_port) + ':' + self.back_hash

''' Some usefull functions '''

def find_neighbors(hash_value,PORT):
    x = Client(PORT).communication('join:' + hash_value).split(':')
    x[0] = int(x[0])
    x[2] = int(x[2])
    x[4] = int(x[4])
    logging.debug('Neighbors found: '+str(x[0]) + ' ' +str(x[2]))
    return x
                                
def send_request(PORT,message):
    Client(PORT).communication(message)
