#!/usr/bin/env python

"""Master process for the DHT

Spawns the DHT servers and clients, issues requests towards the servers
and overall oversights the DHT. In fact, it's the emulator"""

import sys
import logging
import random
import time

from server import Server
from server_master import Server_master
from client import Client
from multiprocessing import Process, Queue


logging.basicConfig(filename='DHT.log', level=logging.DEBUG)

processes = {}
ports = {}
queue = Queue()
main_port = None


def main():
    """Emulator

    Creates the DHT
    Adds another 9 nodes

    Uses the 'insert.txt' file to insert data and measures its throughput
    Uses the 'query.txt' file to find data and measures its throughput
    Uses the 'request.txt' file to perform requests and measures its throughput

    Closes the DHT"""
    # Create the DHT
    global main_port
    processes['1'] = Process(target=create_DHT, args=(int(sys.argv[1]),))
    processes['1'].start()
    host, main_port = queue.get()
    ports[host] = main_port
    logging.debug('Server 1 started...')
    time.sleep(1)
    for i in range(2, 10):
        join(('join, ' + str(i)).split(', '))
        # time.sleep(1)

    # Measure throughputs and report them to the user
    write_throughput = measure_throughput('insert.txt')
    print 'Write throughput:', write_throughput
    read_throughput = measure_throughput('query.txt')
    print 'Read throughput:', read_throughput
    request_throughput = measure_throughput('requests.txt')
    print 'Request throughput:', request_throughput
    DHT_destroy([])


def spawn_DHT(queue):
    """Creates the first DHT server"""
    master_server = Server_master('1', (sys.argv[1]))
    queue.put(('1', master_server.get_port()))
    master_server.accept_connection()


def spawn_server(server_id):
    """Creates a new server, has him join the DHT
    and accept any incoming connections"""
    server = Server(server_id, main_port)
    server.DHT_join()
    queue.put((server_id, server.get_port()))
    logging.debug('Server ' + server_id + ' started...')
    server.accept_connection()
    sys.exit()


def measure_throughput(filename):
    """Reads requests from filename and forwards
    each of them to a random server"""
    start = time.time()
    request_count = 0
    with open(filename, 'r') as f:
        for l in f:
            line = l.rstrip()
            if line.startswith('query'):
                query(line.split(', '))
            elif len(line.split(', ')) == 1:
                query(['query', line])
            elif line.startswith('insert'):
                insert(line.split(', '))
            elif len(line.split(', ')) == 2:
                insert(['insert'] + line.split(', '))
            else:
                raise ValueError('Bad Request')
            request_count += 1
    return (time.time() - start) / request_count


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
            cli.communication('print_all_data')
    else:
        with Client(port) as cli:
            cli.communication('{}:-1:-1:{}'.format(*command))


def DHT_print(command):
    """Requests the DHT topology from the master server"""
    with Client(ports['1']) as cli:
        print cli.communication(command[0])


def create_DHT(repl):
    """Creates the master server of the DHT"""
    k = Server_master('1', repl)
    queue.put(('1', k.get_port()))
    k.accept_connection()
    sys.exit()


command_dict = {'join': join,
                'depart': depart,
                'exit': DHT_destroy,
                'insert': insert,
                'delete': delete,
                'query': query,
                'print': DHT_print}

if __name__ == '__main__':
    main()

