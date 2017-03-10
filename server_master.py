#!/usr/bin/env python

import socket
import select
import Queue
import re
import sys
import request
from hashlib import sha1
from binascii import hexlify
from client import Client
import logging
logging.basicConfig(filename='debug.log',level=logging.ERROR)

class Server_master(object):
    def __init__(self, HOST, PORT):
        self._hasher=sha1(HOST)
        self._network_size = 1
        self.HOST = HOST
        self.myhash = self._hasher.hexdigest()
        self.N_hash = self.myhash
        self.P_hash = self.myhash
        self.PORT = PORT
        self.Next = PORT
        self.Prev = PORT
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
        self._hasher.update(key)
        return self._hasher.hexdigest()
            
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
                            if self.Next != self.PORT:
                                self.client_socket_next.close_connection()
                                logging.debug(str(self.PORT) + ' connection with '+str(self.Next)+ ' shutted')
                            self.Next = int(data[1])
                            self.N_hash=data[2]
                            if self.Next!=self.PORT:
                                self.client_socket_next=Client(self.Next)
                                self.message_queues[sock].put(self.HOST+': Connection granted...')
                        elif data[:4] == 'prev':
                            data=data.split(':')
                            if self.Prev != self.PORT:
                                self.client_socket_prev.close_connection()
                                logging.debug(str(self.PORT) + ' connection with '+str(self.Prev)+ ' shutted')
                            self.Prev = int(data[1])
                            self.P_hash = data[2]
                            if self.Prev!=self.PORT:
                                self.client_socket_prev = Client(self.Prev)
                                self.message_queues[sock].put(str(self.PORT)+': Connection granted...')
                        elif data[:4]=='join':
                            
                            self._network_size +=1
                            x = data.split(':')
                            logging.error(x[1]+' asked to join')
                          
                            if (self.myhash < x[1] < self.N_hash) or (x[1] <= self.N_hash <= self.myhash) or (self.N_hash <= self.myhash <= x[1]):
                                self.message_queues[sock].put(str(self.PORT)+' '+self.myhash+' '+str(self.Next)+' '+self.N_hash)
                                logging.error('node in front me mofos')
                            else:
                                self.message_queues[sock].put(self.client_socket_next.make_query(data))
                                logging.error('Go to next')
                            
                                
                        elif data[:5]=='print':
                            if self._network_size > 1:
                                self.message_queues[sock].put(self.HOST+'->'+self.client_socket_next.make_query('print:'+str(self._network_size-1)))
                            else:
                                self.message_queues[sock].put(self.HOST)
                        elif data[:6]=='myhash':
                            self.message_queues[sock].put(self._hash(data.split(':')[1]))
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
