#!/usr/bin/env python

import socket
import logging
logging.basicConfig(filename='debug.log',level=logging.DEBUG)

class Client(object):
    def __init__(self, PORT):
        self.PORT=PORT
        self.client_socket=socket.socket()
        if PORT!=-1:
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.connect(('localhost', PORT))

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()
        return False

    def __enter__(self):
        return self

    def make_query(self, question):
        self.client_socket.send(question)
        return self.client_socket.recv(1024)

    def send_info(self,info):
        self.client_socket.send(info)
        
    def close_connection(self):
        self.client_socket.send('quit')
        logging.debug('Ask permission to quit '+str(self.PORT))
        self.client_socket.recv(1024)
        logging.debug('Quit gracefully')
        self.client_socket.close()

    def close_and_shut(self):
        self.client_socket.send('Shut down')
        logging.debug('Ask permission to quit '+str(self.PORT))
        self.client_socket.recv(1024)
        logging.debug('Quit gracefully')
        self.client_socket.close()

        
    def get_socket(self):
        return self.client_socket

