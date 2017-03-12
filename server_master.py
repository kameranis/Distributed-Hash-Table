#!/usr/bin/env python

import socket
import select
import Queue
import time
import sys
import threading
import logging

from neighbors import Neighbors, send_request, close_server
from hashlib import sha1
from binascii import hexlify

logging.basicConfig(filename='debug.log',level=logging.ERROR)

class Server_master(object):
    def __init__(self, HOST):
        self._hasher=sha1(HOST)
        self._network_size = 1
        self.HOST = HOST
        self.myhash = self._hasher.hexdigest()
        self.N_hash = self.myhash
        self.P_hash = self.myhash

        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.s.bind(('', 0))
        except socket.error as msg:
            logging.error('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
            sys.exit()
        self.s.listen(10)
        self.PORT = self.s.getsockname()[1]
        self.neighbors = Neighbors(-1,-1,self.PORT,self.PORT)
        logging.debug(str(self.PORT)+' server created')
        self.connection_list = [self.s]
        self.write_to_client = []
        self.message_queues = {}  # servers' reply messages
        self.close = False

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
                    #logging.debug(str(self.PORT)+' Connected with ' + addr[0] + ':' + str(addr[1]))
                    self.message_queues[conn] = Queue.Queue()

                else:
                    data = sock.recv(1024)
                    if data == 'quit':
                        self.message_queues[sock].put('OK...')

                    elif data:
                        if data.startswith('next'):
                            data=data.split(':')
                            self.neighbors.update_front(self.PORT,int(data[1]))
                            self.N_hash = data[2]
                            self.message_queues[sock].put(self.HOST+': Connection granted...')
                        elif data.startswith('prev'):
                            data=data.split(':')
                            self.neighbors.update_back(self.PORT,int(data[1]))
                            self.P_hash = data[2]
                            self.message_queues[sock].put(self.HOST+': Connection granted...')
                        elif data.startswith('join'):
                            x = data.split(':')
                            logging.debug(x[1] + 'asks to join')
                            self._network_size += 1
                            if (self.myhash < x[1] < self.N_hash) or (x[1] <= self.N_hash <= self.myhash) or (self.N_hash <= self.myhash <= x[1]):
                                self.message_queues[sock].put(str(self.PORT)+' '+self.myhash+' '+str(self.neighbors.get_front())+' '+self.N_hash)
                                logging.error('node in front me mofos')
                            else:
                                self.message_queues[sock].put(self.neighbors.send_front(data))#self.client_socket_next.make_query(data))
                                logging.error('Go to next')

                        elif data.startswith('depart'):
                            self._network_size-=1
                            x = data.split(':')
                            logging.debug('Depart starts')
                            if int(x[1]) == self.PORT:
                                self.neighbors.update_front(self.PORT,int(x[3]))
                                self.N_hash = x[4]
                            else:
                                t = threading.Thread(target=send_request, args=(int(x[1]),'next:'+x[3]+':'+x[4],))
                                t.start()
                                t.join()
                            logging.debug('Prev updated')
                            if int(x[3]) == self.PORT:
                                self.neighbors.update_back(self.PORT,int(x[1]))
                                self.P_hash = x[2]
                            else:
                                t = threading.Thread(target=send_request,args= (int(x[3]),'prev:'+x[1]+':'+x[2],))
                                t.start()
                                t.join()
                            logging.debug('Next updated')
                            threading.Thread(target=close_server, args = (int(x[5]),)).start()
                            self.message_queues[sock].put('Your job completed')
                            if self.close and (int(x[3]) == self.PORT) and (int(x[1]) == self.PORT):
                                time.sleep(1)
                                self.s.close()
                                del self.neighbors
                                logging.debug('Shutdown complete')
                                return
                            elif self.close and (int(x[3]) != self.PORT) and (int(x[1]) == self.PORT):
                                logging.debug('Shutting down, closing {}'.format(self.N_hash))
                                threading.Thread(target=send_request, args=(self.neighbors.front_port, 'bye')).start()
                        elif data.startswith('bye'):
                            self.close = True
                            threading.Thread(target=send_request, args=(self.neighbors.front_port, 'bye',)).start()

                        elif data.startswith('print'):
                            if self._network_size > 1:
                                self.message_queues[sock].put(self.HOST+'->'+ self.neighbors.send_front('print:'+str(self._network_size-1)))
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
