"""Request class to model the different operations of the DHT"""

from client import Client

import logging
logging.basicConfig(filename='debug.log',level=logging.DEBUG)

def send(self, sock):
        """Sends the request to the socket"""
        pickle.dump(self, sock)

def find_neighbors(hash_value,PORT):
    logging.debug(hash_value + ': I want to join mofos')
    y = Client(PORT)
    x = y.make_query('join:' + hash_value).split()
    x[0] = int(x[0])
    x[2] = int(x[2])
    y.close_connection()
    logging.debug('Neighbors found')

    return x

        
