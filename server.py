#!/usr/bin/env python

import socket
import select
import Queue
import sys
import logging
import threading

from neighbors import Neighbors, find_neighbors, send_request
from hashlib import sha1

logging.basicConfig(filename='debug.log', level=logging.ERROR)


class Server(object):
    def __init__(self, HOST, master):
        # Every new implemented method must be added to the dictionary with 'key' a unique identifier like below 
        self.operations = {'next': self._update_my_front,
                           'prev': self._update_my_back,
                           'join': self._join,
                           'depart': self._depart,
                           'insert': self._insert,
                           'query': self._query,
                           'print': self._print,
                           'None': self._reply}
        # If we only want just to reply, we add to this dict as key:value -> read_message:reply_message
        self.replies = {'You are ready to depart': 'I am ready to depart',
                        'Shut down': '123456',
                        'quit': '12345'}
        self.close = False
        self.HOST = HOST
        self.myhash = sha1(HOST).hexdigest()
        self.N_hash = self.myhash
        self.P_hash = self.myhash
        self.data = {}
        self.replication = 1
        self.m_PORT = master
        self.data_lock = threading.Lock()

        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.s.bind(('', 0))
        except socket.error as msg:
            logging.error('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
            sys.exit()
        self.PORT = self.s.getsockname()[1]
        self.s.listen(10)

        self.neighbors = Neighbors(-1, -1, self.PORT, self.PORT)

        self.connection_list = [self.s]
        self.write_to_client = []
        self.message_queues = {}  # servers' reply messages

    def DHT_join(self):
        x = find_neighbors(self.myhash, self.m_PORT)
        self.P_hash = x[1]
        self.N_hash = x[3]
        self.neighbors.create_back(x[0], self.PORT, self.myhash)
        self.neighbors.create_front(x[2], self.PORT, self.myhash)

    def _insert(self, data, sock):
        x = data.split(':')
        logging.debug('Hash: {}, inserting {}'.format(self.myhash, x[3]))
        key = sha1(x[3]).hexdigest()
        if x[1] == '-1':
            if self.belongs_here(key):
                logging.debug('Hash: {}, {} belongs here'.format(self.myhash, x[3]))
                x[1] = self.myhash
                x[2] = str(self.replication - 1)
                self.data_lock.acquire()
                self.data[key] = x[4]
                self.data_lock.release()

        else:
            if x[1] != self.myhash:
                if self.data.get(key, -1) != x[4]:
                    self.data_lock.acquire()
                    self.data[key] = x[4]
                    self.data_lock.release()
                    x[2] = str(int(x[2]) - 1)
        if (int(x[2]) != 0) and (x[1] != self.myhash):
            answer = self.neighbors.send_front(':'.join(x))
            self.message_queues[sock].put(answer)
        else:
            self.message_queues[sock].put(':'.join(x[-2:]))

    def _query(self, data, sock):
        """Searches for a key
        data = 'query:key"""
        x = data.split(':')
        song = x[3]
        logging.debug('Hash: {}, quering {}'.format(self.myhash, song))
        key = sha1(song).hexdigest()
        self.data_lock.acquire()
        value = self.data.get(key, None)
        self.data_lock.release()
        if self.belongs_here(key):
            logging.debug('query:{}:{}'.format(song, value))
            self.message_queues[sock].put('{}:{}'.format(song, value))
        elif value is not None:
            logging.debug('query:{}:{}'.format(song, value))
            self.message_queues[sock].put('{}:{}'.format(song, value))
        else:
            logging.debug('Passing forward query:{}:{}'.format(song, value))
            answer = self.neighbors.send_front(':'.join(x))
            self.message_queues[sock].put(answer)

    def belongs_here(self, key):
        return (self.P_hash < key < self.myhash) or \
               (key <= self.myhash <= self.P_hash) or \
               (self.myhash <= self.P_hash <= key)

    def _update_my_front(self, data, sock):
        # data = next:Next_port:Next_hash
        data = data.split(':')
        self.neighbors.update_front(self.PORT, int(data[1]))
        self.N_hash = data[2]
        self.message_queues[sock].put(self.HOST + ': Connection granted...')

    def _update_my_back(self, data, sock):
        # data = prev:Prev_port:Prev_hash
        data = data.split(':')
        self.neighbors.update_back(self.PORT, int(data[1]))
        self.P_hash = data[2]
        self.message_queues[sock].put(self.HOST + ': Connection granted...')

    def _join(self, data, sock):
        x = data.split(':')

        if self.belongs_here(x[1]):
            message = str(self.neighbors.back_port) + ' ' + self.P_hash + ' ' + str(self.PORT) + ' ' + self.myhash
            self.message_queues[sock].put(message)
            logging.debug('Join complete')
        else:
            message = self.neighbors.send_front(data)
            self.message_queues[sock].put(message)

    def _depart(self, data, sock):
        answer = 'depart:' + str(self.neighbors.get_back()) + ':' + self.P_hash + ':' + str(
            self.neighbors.get_front()) + ':' + self.N_hash + ':' + str(self.PORT)
        threading.Thread(target=send_request, args=(self.m_PORT, answer,)).start()
        # send_request(self.m_PORT,answer)
        # self.message_queues[sock].put('Done...Bye Bye')

    def _print(self, data, sock):
        x = data.split(':')
        if int(x[1]) > 1:
            message = self.HOST + str(
                [value for key, value in self.data.iteritems()]) + '->' + self.neighbors.send_front(
                'print:' + str(int(x[1]) - 1))
            self.message_queues[sock].put(message)
        else:
            self.message_queues[sock].put(self.HOST + str([value for key, value in self.data.iteritems()]))

    def _reply(self, data, sock):
        self.message_queues[sock].put(self.replies.get(data, 'Server cant support this operation'))

    def _quit(self, data, sock):
        if sock in self.write_to_client:
            self.write_to_client.remove(sock)

        self.connection_list.remove(sock)
        sock.close()
        del self.message_queues[sock]
        logging.debug('Quit completed')

    def _shut(self, data, sock):
        if data.startswith('123456'):
            self.neighbors.destroy(self.PORT)
            self.s.close()
            sys.exit()

    def _total_shut(self, next_msg, sock):
        if next_msg.startswith('12345'):
            self._quit(next_msg, sock)
        self._shut(next_msg, sock)

    def accept_connection(self):
        while True:
            read_sockets, write_sockets, error_sockets = select.select(self.connection_list, self.write_to_client,
                                                                       self.connection_list)
            for sock in read_sockets:
                if sock == self.s:
                    # wait to accept a connection - blocking call
                    conn, addr = self.s.accept()
                    self.connection_list.append(conn)
                    self.message_queues[conn] = Queue.Queue()

                else:
                    data = sock.recv(1024)
                    # self.operations.get(data.split(':')[0],self._reply)(data,sock)
                    self.operations.get((['None'] + [key for key in self.operations if data.startswith(key)]).pop())(
                        data, sock)

                    if sock not in self.write_to_client:
                        self.write_to_client.append(sock)

            for sock in write_sockets:
                try:
                    next_msg = self.message_queues[sock].get_nowait()
                except Queue.Empty:
                    self.write_to_client.remove(sock)
                else:
                    sock.send(next_msg)

                    self._total_shut(next_msg, sock)

            for sock in error_sockets:
                print >> sys.stderr, 'handling exceptional condition for', sock.getpeername()
                # Stop listening for input on the connection
                self._quit('', sock)

    def get_port(self):
        return self.PORT

    def get_host(self):
        return self.HOST

    def get_sock(self):
        return self.s
