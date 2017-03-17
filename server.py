#!/usr/bin/env python

import Queue
import logging
import socket
import sys
import threading
import time
from hashlib import sha1

from neighbors import Neighbors, find_neighbors, send_request

logging.basicConfig(filename='debug.log', level=logging.ERROR)


class Server(object):
    """Server class

    Implements all the stuff that a DHT server should do"""
    def __init__(self, host, master):
        """Every new implemented method must be added to the dictionary
        with 'key' a unique identifier as below"""
        self.operations = {'quit': self._quit,
                           'join': self._join,
                           'next': self._update_my_front,
                           'prev': self._update_my_back,
                           'depart': self._depart,
                           'insert': self._insert,
                           'add': self._add_data,
                           'delete': self._delete,
                           'remove': self._remove,
                           'query': self._query,
                           'print_all_data': self._print_all_data,
                           'print_my_data': self._print_my_data,
                           'retrieve': self._retrieve,
                           'bye': self._bye}
        self.close = False
        self.host = host
        self.hash = sha1(host).hexdigest()
        self.data = {}
        self.replication = 0
        self.m_port = master
        self.data_lock = threading.Lock()
        self.thread_list = []
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.sock.bind(('', 0))
        except socket.error as msg:
            logging.error('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
            sys.exit()
        self.port = self.sock.getsockname()[1]
        self.sock.settimeout(1)
        self.sock.listen(10)
        self.neighbors = Neighbors(self.hash, self.port, self.hash, self.port)

        self.message_queues = {}  # servers' reply messages

    def __del__(self):
        """Destructor"""
        self.sock.close()

    def DHT_join(self):
        """Servers join the DHT"""
        back_port, back_hash, front_port, front_hash, self.replication = \
            find_neighbors(self.hash, self.m_port)
        self.neighbors.create_back(back_hash, back_port, self.port, self.hash)
        self.neighbors.create_front(front_hash, front_port, self.port, self.hash)
        # Get data from the next server
        self.neighbors.send_front('retrieve:*')

    def _retrieve(self, data, sock):
        """Send data requested to previous server
        If next server doesn't have data, then remove
        data = retrieve:key"""
        _, find_key = data.split(':')
        # Wildcard
        if find_key == '*':
            res = []
            self.data_lock.acquire()
            for key, value in self.data.iteritems():
                if not self.belongs_here(key):
                    req = 'add:{}:{}:1:{}'.format(value[0], value[1], self.hash)
                    threading.Thread(target=send_request, args=(self.neighbors.back_port, req)).start()
                    if self.neighbors.send_front('retrieve:' + key) == 'None:None':
                        res.append(key)
            for key in res:
                del self.data[key]
            self.message_queues[sock].put('Done')
            self.data_lock.release()
        # Single retrieval
        else:
            self.data_lock.acquire()
            key, value = self.data.get(find_key, (None, None))
            if key is not None:
                if self.neighbors.send_front('retrieve:' + find_key) == 'None:None':
                    del self.data[find_key]

            self.data_lock.release()
            self.message_queues[sock].put('{}:{}'.format(key, value))

    def _update_my_front(self, data, sock):
        """Updates front neighbor
        data = next:port:hash"""
        _, front_port, front_hash = data.split(':')
        self.neighbors.update_front(front_hash, int(front_port))
        self.message_queues[sock].put(self.host + ': Connection granted...')

    def _update_my_back(self, data, sock):
        """Updates back neighbor
        data = prev:port:hash"""
        _, back_port, back_hash = data.split(':')
        self.neighbors.update_back(back_hash, int(back_port))
        self.message_queues[sock].put(self.host + ': Connection granted...')

    def belongs_here(self, key):
        """Decides whether a certain hash belongs in this server"""
        return (self.neighbors.back_hash < key <= self.hash) or \
               (key <= self.hash <= self.neighbors.back_hash) or \
               (self.hash <= self.neighbors.back_hash <= key)

    def _join(self, data, sock):
        """Command he receives to determine where a new server belongs
        data = join:hash"""
        _, key_hash = data.split(':')
        if self.belongs_here(key_hash):
            message = self.neighbors.get_back() + ':' + str(self.port) + ':' + self.hash
            logging.debug('Join complete')
        else:
            message = self.neighbors.send_front(data)
        self.message_queues[sock].put(message)

    def _depart(self, data, sock, forward=True):
        """Function to gracefully depart the DHT
        If forward=False, then the DHT is shutting down
        and we don't need to move the data
        data = depart"""
        if forward:
            self.send_data_forward()
        self.neighbors.send_back('next:{}:{}'.format(self.neighbors.front_port, self.neighbors.front_hash))
        self.neighbors.send_front('prev:{}:{}'.format(self.neighbors.back_port, self.neighbors.back_hash))
        send_request(self.m_port, 'depart')
        self.close = True
        self.message_queues[sock].put('Done...Bye Bye')
        logging.debug('DEPART COMPLETED')

    def send_data_forward(self):
        """In case of departing, sends all stored data to the next server"""
        self.data_lock.acquire()
        for key, value in self.data.itervalues():
            self.neighbors.send_front('add:{}:{}:1:{}'.format(key, value, self.hash))
        self.data_lock.release()

    def _bye(self, data, sock):
        """DHT is shutting down
        data = bye"""
        self._depart(data, sock, forward=False)
        t = threading.Thread(target=send_request, args=(self.neighbors.front_port, 'bye',))
        t.start()

    def _add_data(self, data, sock):
        """Someone in the back of us wants to add
        more replicas of what they sent me
        If I already have it, then push it forward
        data = add:key:value:copies:host"""
        _, key, value, copies, host = data.split(':')
        logging.debug('Host: {}, add: {}'.format(self.host, value))
        # No more replicas to add or circle
        if (copies == '0') or (host == self.hash):
            self.message_queues[sock].put(value)
        else:
            key_hash = sha1(key).hexdigest()
            self.data_lock.acquire()
            # Don't have it
            if self.data.get(key_hash, None) != (key, value):
                self.data[key_hash] = (key, value)
                copies = str(int(copies) - 1)
            self.data_lock.release()
            if copies == '0':
                self.message_queues[sock].put(value)
            else:
                self.message_queues[sock].put(
                    self.neighbors.send_front('add:{}:{}:{}:{}'.format(key, value, copies, host)))

    def _insert(self, data, sock):
        """A new (key, value) pair is inserted
        If it doesn't belong to us, send it forward
        Otherwise add replication-1
        data = insert:key:value"""
        _, key, value = data.split(':')
        key_hash = sha1(key).hexdigest()
        logging.debug('Host: {}, insert: {}'.format(self.host, key))
        self.data_lock.acquire()
        if self.data.get(key_hash, (None, None))[1] == value:
            self.data_lock.release()
        elif self.belongs_here(key_hash):
            self.data[key_hash] = (key, value)
            self.data_lock.release()
            self.message_queues[sock].put(value)
            threading.Thread(target=self.neighbors.send_front, args=('add:{}:{}:{}:{}'.format(key, value, self.replication - 1, self.hash), )).start()
        else:
            self.data_lock.release()
            self.message_queues[sock].put(self.neighbors.send_front(data))

    def _delete(self, data, sock):
        """Deletes key, value
        Same as insert
        data = delete:key"""
        _, key = data.split(':')
        key_hash = sha1(key).hexdigest()
        logging.debug('Host: {}, delete: {}'.format(self.host, key))
        if self.belongs_here(key_hash):
            self.data_lock.acquire()
            answer = self.data.pop(key_hash, (None, None))
            self.data_lock.release()
            self.message_queues[sock].put('{}:{}'.format(*answer))
            if answer[0] is not None:
                self.neighbors.send_front('remove:{}'.format(key))
        else:
            self.neighbors.send_front(data)
            self.message_queues[sock].put('Done')

    def _remove(self, data, sock):
        """Removes key, same as add
        data = remove:key"""
        _, key = data.split(':')
        key_hash = sha1(key).hexdigest()
        self.data_lock.acquire()
        answer = self.data.pop(key_hash, (None, None))
        self.data_lock.release()
        if answer[0] is not None:
            self.neighbors.send_front('remove:{}'.format(key))
        self.message_queues[sock].put('{}:{}'.format(*answer))

    def _query(self, data, sock):
        """Searches for a key
        data = 'query:copies:hostkey"""
        _, copies, host, song = data.split(':')
        key = sha1(song).hexdigest()
        self.data_lock.acquire()
        value = self.data.get(key, None)
        self.data_lock.release()
        if self.belongs_here(key):
            logging.debug('{}: Belongs query:{}:{}'.format(self.host, song, value))
            self.message_queues[sock].put('{}:{}'.format(song, value))
        elif value is not None:
            logging.debug('{}: Found query:{}:{}'.format(self.host, song, value))
            self.message_queues[sock].put('{}:{}'.format(song, value))
        else:
            logging.debug('{} Passing forward query:{}:{}'.format(self.host, song, value))
            self.message_queues[sock].put(self.neighbors.send_front('query:{}:{}:{}'.format(copies, host, song)))

    def _print_my_data(self, data, sock):
        """Prints my data and forwards the message
        data = print_my_data:hash"""
        _, host_hash = data.split(':')
        if host_hash != self.hash:
            self.data_lock.acquire()
            print self.host, [value for value in self.data.itervalues()]
            self.data_lock.release()
            self.message_queues[sock].put(self.neighbors.send_front(data))
        else:
            self.message_queues[sock].put('Done')

    def _print_all_data(self, data, sock):
        """Starts data printing of all servers
        data = print_all_data"""
        self.data_lock.acquire()
        print self.host, [value for value in self.data.itervalues()]
        self.data_lock.release()
        if self.neighbors.front_hash != self.hash:
            self.message_queues[sock].put(self.neighbors.send_front('print_my_data:' + self.hash))
        else:
            self.message_queues[sock].put('Done')

    def _quit(self, data, sock):
        """Quits"""
        self.message_queues[sock].put('CLOSE MAN')

    def _reply(self, data, sock):
        """Bad Request"""
        self.message_queues[sock].put('Server cant support this operation')

    def _connection(self):
        """Main function to serve a connection"""
        try:
            conn, _ = self.sock.accept()
        except socket.timeout:
            pass
        else:
            self.message_queues[conn] = Queue.Queue()
            self.thread_list.append(threading.Thread(target=self.clientthread, args=(conn,)))
            self.thread_list[-1].start()

    def clientthread(self, sock):
        """A thread executes the command"""
        while True:
            try:
                data = sock.recv(1024)
                if not data:
                    break
                else:
                    fun = self.operations.get(data.split(':')[0], self._reply)
                    fun(data, sock)
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
                    if new_msg == 'CLOSE MAN':
                        del self.message_queues[sock]
                        sock.close()
                        return

    def accept_connection(self):
        """Main loop"""
        while True:
            self._connection()
            if self.close:
                logging.debug('CLOSEEEEEEEEEEEEEEE')
                time.sleep(2)
                return

    def get_port(self):
        """Returns port"""
        return self.port

    def get_host(self):
        """Returns host"""
        return self.host

    def get_sock(self):
        """Returns socket"""
        return self.sock


class Server_master(Server):
    """First server of the DHT"""
    def __init__(self, host, repl):
        self._network_size = 1
        self.close = False
        super(Server_master, self).__init__(host, -1)
        self.replication = int(repl)

    def _join(self, data, sock):
        self._network_size += 1
        super(Server_master, self)._join(data, sock)
        self.message_queues[sock].put(self.message_queues[sock].get() + ':' + str(self.replication))

    def _depart(self, data, sock, forward=True):
        self._network_size -= 1
        self.message_queues[sock].put('Done')

    def _bye(self, data, sock):
        if self._network_size > 1:
            thr = threading.Thread(target=send_request, args=(self.neighbors.front_port, 'bye',))
            thr.start()
        else:
            self.close = True
        self.message_queues[sock].put('Done')