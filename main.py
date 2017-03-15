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

processes = []
ports = {}


def spawn_DHT(queue):
    """Creates the first DHT server"""
    master_server = Server_master('1')
    queue.put(('1', master_server.get_port()))
    master_server.accept_connection()


def spawn_server(server_id, main_port, queue):
    """Spawns a new DHT server and calls its join method"""
    new_server = Server(server_id, main_port)
    new_server.DHT_join()
    queue.put((server_id, new_server.get_port()))
    logging.debug('Server {} started'.format(server_id))
    new_server.accept_connection()
    sys.exit()


def measure_throughput(filename):
    """Reads requests from filename and forwards
    each of them to a random server"""
    start = time.time()
    request_count = 0
    with open(filename, 'r') as f:
        for line in f:
            host = random.sample(ports, 1)[0]
            port = ports[host]
            with Client(port) as cli:
                if line.startswith('query'):
                    cli.make_query('query:{}'.format(*line.split(', ')[1:]))
                elif len(line.split(', ')) == 1:
                    cli.make_query('query:{}'.format(line))
                elif line.startswith('insert'):
                    cli.make_query('insert:-1:-1:{}:{}'.format(*line.split(', ')[1:]))
                elif len(line.split(', ')) == 2:
                    cli.make_query('insert:-1:-1:{}:{}'.format(*line.split(', ')))
                else:
                    raise ValueError('Bad Request')
            request_count += 1
    return start - time.time() / request_count


def main():
    """Emulator

    Creates the DHT
    Adds another 9 nodes

    Uses the 'insert.txt' file to insert data and measures its throughput
    Uses the 'query.txt' file to find data and measures its throughput
    Uses the 'request.txt' file to perform requests and measures its throughput

    Closes the DHT"""
    # Create the DHT
    queue = Queue()
    processes.append(Process(target=spawn_DHT, args=(queue,)))
    processes[0].start()
    server_id, port = queue.get()
    ports[server_id] = port
    main_port = port
    logging.debug('DHT created')
    time.sleep(1)

    # Add another 9 nodes
    for i in xrange(2, 11):
        processes.append(Process(target=spawn_server, args=(str(i), main_port, queue)))
        processes[-1].start()
        server_id, port = queue.get()
        ports[server_id] = port
        time.sleep(1)

    # Measure throughputs and report them to the user
    write_throughput = measure_throughput('insert.txt')
    print 'Write throughput: ', write_throughput
    read_throughput = measure_throughput('query.txt')
    print 'Read throughput: ', read_throughput
    request_throughput = measure_throughput('requests.txt')
    print 'Request throughput: ', request_throughput
    with Client(ports['1']) as cli:
        cli.send_info('bye')


if __name__ == '__main__':
    main()

