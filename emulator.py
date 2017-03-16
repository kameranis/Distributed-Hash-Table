"""Contains the Emulator class which builds a DHT
and provides all the necessary functions to interact
with the DHT"""

import sys
import logging
import random
import time

from multiprocessing import Process, Queue
from client import Client
from server import Server
from server_master import Server_master


class Emulator(object):
    """Emulator class"""

    def __init__(self, replication):
        """Constructor"""
        self.help_dict = {'join': 'join, ID',
                          'depart': 'depart, ID',
                          'DHT destroy': 'exit',
                          'Insert': 'insert, key, value',
                          'Query': 'query, key',
                          'Help': 'help'}
        self.command_dict = {'join': self.join,
                             'depart': self.depart,
                             'exit': self.DHT_destroy,
                             'insert': self.insert,
                             'delete': self.delete,
                             'query': self.query}
        self.processes = {}
        self.ports = {}
        self.queue = Queue()
        self.processes['1'] = Process(target=self.create_DHT,
                                      args=(replication,))
        self.processes['1'].start()
        host, self.main_port = self.queue.get()
        self.ports[host] = self.main_port
        logging.debug('Server 1 started...')
        time.sleep(1)

    def __del__(self):
        for proc in self.processes:
            proc.join()

    def execute(self, command):
        """Executes a single command"""
        fun = self.command_dict.get(command[0], self.bad_command)
        fun(command)

    @staticmethod
    def bad_command(command):
        """Informs the user that he has enterred an invalid command"""
        sys.stderr.write('Bad command: {}\n'.format(', '.join(command)))

    def join(self, command):
        """Creates a new server in a new process.
        Keeps the processes and ports updated"""
        server_id = command[1]
        self.processes[server_id] = Process(target=self.spawn_server,
                                            args=(server_id,))
        self.processes[server_id].start()
        host, port = self.queue.get()
        self.ports[host] = port

    def depart(self, command):
        """Commands the server to shut down"""
        with Client(self.ports[command[1]]) as cli:
            cli.communication(command[0])
        self.processes[command[1]].join()
        del self.processes[command[1]]
        del self.ports[command[1]]

    def DHT_destroy(self, command):
        """Forces the whole DHT to shutdown"""
        with Client(self.ports['1']) as cli:
            cli.communication('bye')

    def insert(self, command):
        """Sends a request to a random server to insert a (key, value) pair"""
        host = random.sample(self.ports, 1)[0]
        port = self.ports[host]
        with Client(port) as cli:
            cli.communication('{}:{}:{}'.format(*command))

    def delete(self, command):
        """Sends a request to a random server to delete the key"""
        host = random.sample(self.ports, 1)[0]
        port = self.ports[host]
        with Client(port) as cli:
            cli.communication('{}:{}'.format(*command))

    def query(self, command):
        """Queries a random server for the value of a key"""
        host = random.sample(self.ports, 1)[0]
        port = self.ports[host]
        if command[1] == '*':
            with Client(port) as cli:
                cli.communication('print_all_data')
        else:
            with Client(port) as cli:
                print cli.communication('{}:-1:-1:{}'.format(*command))

    def print_help(self, command):
        """Prints a helping message to the user"""
        print "+--------------------------------------+"
        print '+------------COMMAND LIST--------------+'
        for key, value in self.help_dict.iteritems():
            print '| {:>14}: {:<20} |'.format(key, value)
        print "+--------------------------------------+"

    def create_DHT(self, repl):
        """Creates the master server of the DHT"""
        k = Server_master('1', repl)
        self.queue.put(('1', k.get_port()))
        k.accept_connection()
        sys.exit()

    def spawn_server(self, server_id):
        """Creates a new server, has him join the DHT
        and accept any incoming connections"""
        server = Server(server_id, self.main_port)
        server.DHT_join()
        self.queue.put((server_id, server.get_port()))
        logging.debug('Server ' + server_id + ' started...')
        server.accept_connection()
        sys.exit()
