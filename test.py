#!/usr/bin/env python
import random
import time
import sys
import logging

from multiprocessing import Process, Queue
from client import Client
from server import Server
from server_master import Server_master


logging.basicConfig(filename='debug.log', level=logging.DEBUG)

help_dict = {'join': 'join, ID',
             'depart': 'depart, ID',
             'DHT destroy': 'exit',
             'Insert': 'insert, key, value',
             'Query': 'query, key',
             'Help': 'help'}
             
processes = {}
ports = {}
queue = Queue()
main_port = None


def main():
    """Main script
    
    Acts as a terminal towards the DHT"""
    global main_port
    processes['1'] = Process(target=create_DHT, args=(int(sys.argv[1]),))
    processes['1'].start()
    host, main_port = queue.get()
    ports[host] = main_port
    logging.debug('Server 1 started...')
    time.sleep(1)
    print_help('')
    for i in range(2,10):
        join(('join, '+str(i)).split(', '))
        # time.sleep(1)
    
    insert('insert:Imagine:2'.split(':'))
    insert('insert:Ameranhs:3'.split(':'))
    insert('insert:Valkanos:4'.split(':'))
    query(['query','*'])
    
    while True:
        command = raw_input('Action: ').split(', ')
        fun = command_dict.get(command[0], bad_command)
        fun(command)

        if command[0] == 'exit':
            break
        time.sleep(1)
    for host, proc in processes.iteritems():
        proc.join()
    logging.debug('END')


def bad_command(command):
    """Informs the user that he has enterred an invalid command"""
    sys.stderr.write('Bad command: {}\n'.format(', '.join(command)))


def join(command):
    """Creates a new server in a new process.
    Keeps the processes and ports updated"""
    server_id = command[1]
    processes[server_id] = Process(target=spawn_server, args=(server_id,))
    processes[server_id].start()
    host, port = queue.get()
    ports[host] = port


def depart(command):
    """Commands the server to shut down"""
    with Client(ports[command[1]]) as cli:
        cli.communication(command[0])
    processes[command[1]].join()
    del processes[command[1]]
    del ports[command[1]]


def DHT_destroy(command):
    """Forces the whole DHT to shutdown"""
    with Client(ports['1']) as cli:
        cli.communication('bye')


def insert(command):
    """Sends a request to a random server to insert a (key, value) pair"""
    host = random.sample(ports, 1)[0]
    port = ports[host]
    with Client(port) as cli:
        cli.communication('{}:{}:{}'.format(*command))


def delete(command):
    """Sends a request to a random server to delete the key"""
    host = random.sample(ports, 1)[0]
    port = ports[host]
    with Client(port) as cli:
        cli.communication('{}:{}'.format(*command))


def query(command):
    """Queries a random server for the value of a key"""
    host = random.sample(ports, 1)[0]
    port = ports[host]
    if command[1] == '*':
        with Client(port) as cli:
            print cli.communication('print_all_data')
    else:
        with Client(port) as cli:
            print cli.communication('{}:-1:-1:{}'.format(*command))
                                               

def print_help(command):
    """Prints a helping message to the user"""
    print "+--------------------------------------+"
    print '+------------COMMAND LIST--------------+'
    for key, value in help_dict.iteritems():
        print '| {:>14}: {:<20} |'.format(key, value)
    print "+--------------------------------------+"


def create_DHT(repl):
    """Creates the master server of the DHT"""
    k = Server_master('1', repl)
    queue.put(('1', k.get_port()))
    k.accept_connection()
    sys.exit()


def spawn_server(server_id):
    """Creates a new server, has him join the DHT
    and accept any incoming connections"""
    server = Server(server_id, main_port)
    server.DHT_join()
    queue.put((server_id, server.get_port()))
    logging.debug('Server ' + server_id + ' started...')
    server.accept_connection()
    sys.exit()


command_dict = {'join': join,
                'depart': depart,
                'exit': DHT_destroy,
                'insert': insert,
                'delete': delete,
                'query': query}


if __name__ == '__main__':
    main()
