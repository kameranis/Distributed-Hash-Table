#!/usr/bin/env python

import socket
import select
import Queue
import re
import sys
import request
from hashlib import sha1
from client import Client
import logging
logging.basicConfig(filename='debug.log',level=logging.DEBUG)

class Server_master(object):
    def __init__(self, HOST, PORT):
        
        self.HOST = HOST
        self.N_HOST = HOST
        self.P_HOST = HOST
        self.PORT = PORT
        self.Next = -1
        self.Prev = -1
        self.client_socket_prev = Client(-1)
        self.client_socket_next = Client(-1)
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.s.bind(('', PORT))
        except socket.error as msg:
            logging.error('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
            sys.exit()
        self.s.listen(10)
        logging.debug(str(PORT)+' server created')
        self.connection_list = [self.s]  
        self.write_to_client = []  
        self.message_queues = {}  # servers' reply messages
        
    def _hash(self, key):
        """Hashes the key and returns its hash"""
        while True:
            self.hasher.update(key)
            yield hexlify(self.hasher.digest())
            
    def accept_connection(self):
        while True:
            read_sockets, write_sockets, error_sockets = select.select(self.connection_list, self.write_to_client,
                                                                       self.connection_list)
            for sock in read_sockets:
                if sock == self.s:
                    # wait to accept a connection - blocking call
                    conn, addr = self.s.accept()
                    self.connection_list.append(conn)
                    logging.debug(str(self.PORT)+' Connected with ' + addr[0] + ':' + str(addr[1]))
                    self.message_queues[conn] = Queue.Queue()

                else:
                    data = sock.recv(1024)
                    if data == 'quit':
                        self.message_queues[sock].put('OK...')
                    elif data:
                        if data[:4] == 'next':
                            data=data.split(':')
                            if self.Next != -1:
                                self.client_socket_next.close_connection()
                                logging.debug(str(self.PORT) + ' connection with '+str(self.Next)+ ' shutted')
                            self.Next = int(data[1])
                            self.N_HOST=int(data[2])
                            logging.debug(str(self.HOST)+'->'+str(self.N_HOST))
                            if self.Next!=self.PORT:
                                self.client_socket_next=Client(self.Next)
                                self.message_queues[sock].put(str(self.HOST)+': Connection granted...' + str(self.N_HOST))
                        elif data[:4] == 'prev':
                            data=data.split(':')
                            if self.Prev != -1:
                                self.client_socket_prev.close_connection()
                                logging.debug(str(self.PORT) + ' connection with '+str(self.Prev)+ ' shutted')
                            self.Prev = int(data[1])
                            self.P_HOST=int(data[2])
                            if self.Prev!=self.PORT:
                                self.client_socket_prev = Client(self.Prev)
                                self.message_queues[sock].put(str(self.PORT)+': Connection granted...' + str(self.Prev))
                        elif data[:4]=='join':
                            x = data.split(':')
                            if self.HOST==self.N_HOST:
                                self.message_queues[sock].put(str(self.PORT)+' '+str(self.HOST)+' '+str(self.PORT)+' '+str(self.N_HOST))
                                logging.debug('Branch 1')
                            elif self.HOST < int(x[1]):
                                if int(x[1]) < self.N_HOST:
                                    self.message_queues[sock].put(str(self.PORT)+' '+str(self.HOST)+' '+str(self.Next)+' '+str(self.N_HOST))
                                    logging.debug('Branch 2')
                                else:
                                    self.message_queues[sock].put(self.client_socket_next.make_query(data))
                                    logging.debug('Branch 3')
                       
                        elif data[:5]=='print':
                            self.message_queues[sock].put(str(self.HOST)+'->'+self.client_socket_next.make_query('print'))
                        else:
                            self.message_queues[sock].put('You send me... ' + data)
                    if sock not in self.write_to_client:
                        self.write_to_client.append(sock)

            for sock in write_sockets:
                try:
                     next_msg = self.message_queues[sock].get_nowait()
                except Queue.Empty:
                    self.write_to_client.remove(sock)
                else:
                    sock.send(next_msg)
                    if next_msg =='OK...':
                        if sock in self.write_to_client:
                            self.write_to_client.remove(sock)
                            logging.debug(str(self.PORT) + ' connection with client shutted')
                            
                        self.connection_list.remove(sock)
                        sock.close()
                        del self.message_queues[sock]
                                       

            for sock in error_sockets:
                print >> sys.stderr, 'handling exceptional condition for', sock.getpeername()
                # Stop listening for input on the connection
                self.connection_list.remove(sock)
                if sock in self.write_to_client:
                    self.write_to_client.remove(sock)
                sock.close()
                del self.message_queues[sock]
        self.s.close()

    def get_port(self):
        return self.PORT

    def get_host(self):
        return self.HOST

    def get_sock(self):
        return self.s
