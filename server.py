#!/usr/bin/env python

import socket
import select
import Queue
import sys
import logging
import threading
import time

from neighbors import Neighbors, find_neighbors, send_request
from hashlib import sha1
from binascii import hexlify

logging.basicConfig(filename='debug.log', level=logging.ERROR)


class Server(object):
    def __init__(self, HOST, master):
        # Every new implemented method must be added to the dictionary with 'key' a unique identifier like below 
        self.operations = {'quit': self._quit,
                           'join': self._join,
                           'next': self._update_my_front,
                           'prev': self._update_my_back,
                           'depart': self._depart,
                           'insert': self._insert,
                           'add': self._add_data,
                           'query': self._query,
                           'print_all_data': self._print_all_data,
                           'print_my_data': self._print_my_data,
                           'bye': self._bye,
                           'print': self._print}
        # If we only want just to reply, we add to this dict as key:value -> read_message:reply_message
        self.replies = {'You are ready to depart': 'I am ready to depart',
                        'Shut down': '123456'}
        self.close = False
        self.HOST = HOST
        self.myhash = sha1(HOST).hexdigest()
        self.data = {}
        self.replication = 0
        self.m_PORT = master
        self.data_lock = threading.Lock()
        # self.thread_queue = {} Not used for now
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.s.bind(('', 0))
        except socket.error as msg:
            logging.error('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
            sys.exit()
        self.PORT = self.s.getsockname()[1]
        self.s.listen(10)
        self.neighbors = Neighbors(self.myhash, self.PORT, self.myhash, self.PORT)

        self.connection_list = [self.s]
        self.message_queues = {}  # servers' reply messages

    ''' Usable functions ar below '''

    def __del__(self):
        for sock in self.connection_list:
            # sock.send('Server is closing')
            sock.close()
        self.s.close()

    def DHT_join(self):
        x = find_neighbors(self.myhash, self.m_PORT)
        # x[0] = Prev_Port, x[1] = Prev_hash, x[2] = Next_port, x[3] = Next_hash, x[4] = replication
        self.replication = x[4]
        self.neighbors.create_back(x[1], x[0], self.PORT, self.myhash)
        self.neighbors.create_front(x[3], x[2], self.PORT, self.myhash)

    def _update_my_front(self, data, sock):
        # data = next:Next_port:Next_hash
        data = data.split(':')
        self.neighbors.update_front(data[2], int(data[1]))
        self.message_queues[sock].put(self.HOST + ': Connection granted...')

    def _update_my_back(self, data, sock):
        # data = prev:Prev_port:Prev_hash
        data = data.split(':')
        self.neighbors.update_back(data[2], int(data[1]))
        self.message_queues[sock].put(self.HOST + ': Connection granted...')

    def belongs_here(self, key):
        return (self.neighbors.back_hash < key < self.myhash) or \
               (key <= self.myhash <= self.neighbors.back_hash) or \
               (self.myhash <= self.neighbors.back_hash <= key)

    def _join(self, data, sock):
        # data = join:key_hash
        x = data.split(':')
        if self.belongs_here(x[1]):
            message = self.neighbors.get_back() + ':' + str(self.PORT) + ':' + self.myhash
            logging.debug('Join complete')
        else:
            message = self.neighbors.send_front(data)
        self.message_queues[sock].put(message)

    def _depart(self, data, sock, forward=True):
        if forward:
            self.send_data_forward()
        self.neighbors.send_back('next:{}:{}'.format(self.neighbors.front_port, self.neighbors.front_hash))
        self.neighbors.send_front('prev:{}:{}'.format(self.neighbors.back_port, self.neighbors.back_hash))
        send_request(self.m_PORT, 'depart:{}'.format(self.PORT))
        self.close = True
        self.message_queues[sock].put('Done...Bye Bye')

    def send_data_forward(self):
        """In case of departing, sends all stored data to the next server"""
        self.data_lock.acquire()
        for key, value in self.data.itervalues():
            self.neighbors.send_front('add:{}:{}:1:{}'.format(key, value, self.myhash))
        self.data_lock.release()

    def _bye(self, data, sock):
        self._depart(data, sock, forward=False)
        t = threading.Thread(target=send_request, args=(self.neighbors.front_port, 'bye',))
        t.start()

    def _add_data(self, data, sock):
        # data = add:key:value:copies:host
        x = data.split(':')
        logging.debug('Host: {}, add: {}'.format(self.HOST, x[2]))
        if (x[3] == '0') or (x[4] == self.myhash):
            self.message_queues[sock].put(':'.join(x[-2:]))
        else:
            key = sha1(x[1]).hexdigest()
            self.data_lock.acquire()
            if self.data.get(key, None) != (x[1], x[2]):
                self.data[key] = (x[1], x[2])
                x[3] = str(int(x[3]) - 1)
            self.data_lock.release()
            self.message_queues[sock].put(self.neighbors.send_front(':'.join(x)))

    def _insert(self, data, sock):
        # data = insert:key:value
        x = data.split(':')
        key = sha1(x[1]).hexdigest()
        logging.debug('Host: {}, insert: {}'.format(self.HOST, x[1]))
        if self.belongs_here(key):
            x.append(str(self.replication - 1))
            x.append(self.myhash)
            self.data_lock.acquire()
            self.data[key] = (x[1], x[2])
            self.data_lock.release()
            threading.Thread(target=send_request, args=(self.neighbors.front_port, 'add:' + ':'.join(x[-4:], ))).start()
        else:
            self.neighbors.send_front(data)
        self.message_queues[sock].put('Done')

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

    def _print_my_data(self,data,sock):
        self.data_lock.acquire()
        print [value for key, value in self.data.iteritems()]
        self.data_lock.release()
        self.message_queues[sock].put(str(self.neighbors.front_port))
        
    def _print_all_data(self,data,sock):
        
        self.data_lock.acquire()
        print [value for key, value in self.data.iteritems()]
        self.data_lock.release()
        x = self.neighbors.front_port
        while x != self.PORT:
            x = int(send_request(x, 'print_my_data'))
            
        self.message_queues[sock].put('Done')
        

    def _print(self, data, sock):
        x = data.split(':')
        if int(x[1]) > 1:
            message = self.HOST + str(
                [value for key, value in self.data.iteritems()]) + '->' + self.neighbors.send_front(
                'print:' + str(int(x[1]) - 1))
            self.message_queues[sock].put(message)
        else:
            self.message_queues[sock].put(self.HOST + str([value for key, value in self.data.iteritems()]))

    def _quit(self, data, sock):
        self.message_queues[sock].put('bb')

    def _reply(self, data, sock):
        self.message_queues[sock].put(self.replies.get(data, 'Server cant support this operation'))

    def _connection(self):
        # wait to accept a connection - blocking call
        conn, addr = self.s.accept()
        self.connection_list.append(conn)
        self.message_queues[conn] = Queue.Queue()

    def accept_connection(self):
        while True:
            read_sockets, write_sockets, error_sockets = select.select(self.connection_list, [], self.connection_list)
            for sock in read_sockets:
                if sock == self.s:
                    self._connection()
                else:
                    try:
                        data = sock.recv(1024)
                        self.operations.get(data.split(':')[0], self._reply)(data, sock)
                        # self.operations.get( (['None'] + [key for key in self.operations if data.startswith(key)]).pop() )(data,sock)
                    except socket.error:
                        logging.error('Data recv failed')
                        break
                    else:
                        try:
                            new_msg = self.message_queues[sock].get_nowait()
                        except Queue.Empty:
                            pass
                        else:
                            sock.send(new_msg)
                    finally:
                        self.connection_list.remove(sock)
                        del self.message_queues[sock]
                        sock.close()

            for sock in error_sockets:
                print >> sys.stderr, 'handling exceptional condition for', sock.getpeername()
                # Stop listening for input on the connection
                self._quit('', sock)
                if sock in self.write_to_client:
                    self.write_to_client.remove(sock)

                self.connection_list.remove(sock)
                del self.message_queues[sock]
                sock.close()

            if self.close:
                # self.thread_queue.join()
                # time.sleep(1)
                logging.debug('return')
                return

        self.s.close()

    def get_port(self):
        return self.PORT

    def get_host(self):
        return self.HOST

    def get_sock(self):
        return self.s
