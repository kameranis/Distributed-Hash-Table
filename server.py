#!/usr/bin/env python

import socket
import select
import Queue
import sys
import logging
import request

from client import Client
from hashlib import sha1
from binascii import hexlify

logging.basicConfig(filename='debug.log',level=logging.ERROR)

class Server(object):
    def __init__(self, HOST, master):
        self.HOST = HOST
        self.myhash = ''
        self.N_hash = ''
        self.P_hash = ''
        self.m_PORT=master
        self.master=Client(master)
        self.client_socket_prev = Client(-1)
        self.client_socket_next = Client(-1)
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.s.bind(('', 0))
        except socket.error as msg:
            logging.error('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
            sys.exit()
        self.PORT = self.s.getsockname()[1]
        self.Next = self.PORT
        self.Prev = self.PORT
        self.s.listen(6)
        logging.debug(str(self.PORT)+' server created')
        self.connection_list = [self.s]
        self.write_to_client = []
        self.message_queues = {}  # servers' reply messages
        self._hasher=sha1()
        
    def _hash(self, key):
        """Hashes the key and returns its hash"""
        self._hasher.update(key)
        return self._hasher.hexdigest()
                        
    def DHT_join(self):
        self.myhash=self._hash(self.HOST) #self.master.make_query('myhash:'+self.HOST)
        logging.error(self.myhash+': I want to join mofos')
        x=self.master.make_query('join:'+self.myhash).split()
        logging.debug('I am '+self.HOST+ ' PREV: '+x[1]+' NEXT: '+x[3])
        self.P_hash=x[1]
        self.N_hash=x[3]
        x[0]=int(x[0])
        x[1]=int(x[2])

        self.client_socket_prev = Client(x[0])
        self.client_socket_prev.make_query('next:' + str(self.PORT)+ ':'+self.myhash)
        self.Prev = x[0]

        self.client_socket_next = Client(x[1])
        self.client_socket_next.make_query('prev:' + str(self.PORT)+':'+self.myhash)
        self.Next = x[1]

        self.master.close_connection()

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
                            self.N_hash = data[2]
                            self.client_socket_next=Client(self.Next)
                            self.message_queues[sock].put(self.HOST+': Connection granted...')
                        elif data[:4] == 'prev':
                            data=data.split(':')
                            if self.Prev != self.PORT:
                                self.client_socket_prev.close_connection()
                                logging.debug(str(self.PORT) + ' connection with '+str(self.Prev)+ ' shutted')
                            self.Prev = int(data[1])
                            self.P_hash = data[2]
                            self.client_socket_prev = Client(self.Prev)
                            self.message_queues[sock].put(self.HOST+': Connection granted...')
                        elif data[:4]=='join':
                            x = data.split(':')
                            if (self.myhash < x[1] < self.N_hash) or (x[1] <= self.N_hash <= self.myhash) or (self.N_hash <= self.myhash <= x[1]):
                                self.message_queues[sock].put(str(self.PORT)+' '+self.myhash+' '+str(self.Next)+' '+self.N_hash)
                                logging.error('node in front me mofos')
                            else:
                                self.message_queues[sock].put(self.client_socket_next.make_query(data))
                                logging.error('Go to next')
                                                                    
                        elif data[:5]=='print':
                            x = data.split(':')
                            if int(x[1])>1:
                                self.message_queues[sock].put(self.HOST+'->'+self.client_socket_next.make_query('print:'+str(int(x[1])-1)))
                            else:
                                self.message_queues[sock].put(self.HOST)
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
