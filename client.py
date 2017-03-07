#!/usr/bin/env python

import socket

class Client:

    def __init__(self,PORT):
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect(('localhost', PORT))        
        
    def make_query(self,question):

        #data = raw_input ( "SEND :" )
        self.client_socket.send(question)
        answer=self.client_socket.recv(1024)
        print answer


x = Client(8881)
y = raw_input("SEND: ")
while y!='quit':         
    x.make_query(y)
    y = raw_input("SEND: ")
