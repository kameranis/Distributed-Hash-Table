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
             'Help': 'help',
             'print DHT': 'print'}

processes = []
ports = {}
queue = Queue()
main_port = None


def main():
    global main_port
    processes.append(Process(target=create_DHT, args=(queue,)))
    processes[0].start()
    host, main_port = queue.get()
    ports[host] = main_port
    logging.debug('Server 1 started...')
    time.sleep(1)
    help('')
    while True:
        command = raw_input('Action: ').split(', ')
        fun = command_dict.get(command[0], bad_command)
        fun(command)

        if command[0] == 'exit':
            break
        time.sleep(1)
    for p in processes:
        p.join()
    logging.debug('END')


def bad_command(command):
    sys.stderr.write('Bad command: {}\n'.format(', '.join(command)) )


def join(command):
    server_id = command[1]
    processes.append(Process(target=spawn_server, args=(server_id, main_port, queue,)))
    processes[-1].start()
    t, port = queue.get()
    ports[t] = port


def depart(command):
    with Client(ports[command[1]]) as x:
        x.send_info('depart')


def DHT_destroy(command):
    with Client(ports['1']) as x:
        x.send_info('bye')


def insert(command):
    host = random.sample(ports, 1)[0]
    port = ports[host]
    with Client(port) as cli:
        cli.make_query('insert:-1:-1:{}:{}'.format(command[1], command[2]))


def query(command):
    raise NotImplementedError


def DHT_print(command):
    with Client(ports['1']) as cli:
        print cli.make_query('print')


def help(command):
    print "+--------------------------------------+"
    print '+------------COMMAND LIST--------------+'
    for key, value in help_dict.iteritems():
        print '| {:>14}: {:<20} |'.format(key, value)
    print "+--------------------------------------+"


def create_DHT(queue):
    k = Server_master('1')
    queue.put(('1', k.get_port()))
    k.accept_connection()
    sys.exit()


def spawn_server(server_id, main_port, queue):
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
                'query': query,
                'print': DHT_print}


if __name__ == '__main__':
    main()
