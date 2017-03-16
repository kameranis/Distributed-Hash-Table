#!/usr/bin/env python

import socket
import logging
import sys

logging.basicConfig(filename='debug.log',level=logging.DEBUG)

class Client(object):
    def __init__(self, PORT):
        self.PORT=PORT
        self.answer = ''
        self.client_socket=socket.socket()
        if PORT!=-1:
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.connect(('localhost', PORT))

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __enter__(self):
        return self

    def send_info(self,info):
        self.client_socket.send(info)

    def get_socket(self):
        return self.client_socket

    def close(self):
        try:
            self.client_socket.send('quit')
        except socket.error:
            logging.error('client: CLOSURE WAS UNSUCCESSFUL')
            sys.exit()
        else:
            self.client_socket.recv(1024)
            self.client_socket.close()

        
    def communication(self,message):
        try:
            self.client_socket.send(message)
        except socket.error:
            logging.error('client: SEND MESSAGE FAIL')
            sys.exit()
            
        try:
            self.answer = self.client_socket.recv(1024)
        except socket.error:
            logging.error('client: READ MESSAGE FAIL '+ str(self.PORT))
            sys.exit()
        else:
            return self.answer



