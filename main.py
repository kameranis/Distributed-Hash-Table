#!/usr/bin/env python

"""Master process for the DHT

Spawns the DHT servers and clients, issues requests towards the servers
and overall oversights the DHT. In fact, it's the emulator"""

import sys
import time

from emulator import Emulator

def main():
    """Emulator

    Creates the DHT
    Adds another 9 nodes

    Uses the 'insert.txt' file to insert data and measures its throughput
    Uses the 'query.txt' file to find data and measures its throughput
    Uses the 'request.txt' file to perform requests and measures its throughput

    Closes the DHT"""
    # Create the DHT
    emu = Emulator(int(sys.argv[1]))
    for i in range(2, 10):
        emu.execute(['join', str(i)])

    # Measure throughputs and report them to the user
    write_throughput = measure_throughput(emu, 'insert.txt')
    print 'Write throughput:', write_throughput
    read_throughput = measure_throughput(emu, 'query.txt')
    print 'Read throughput:', read_throughput
    request_throughput = measure_throughput(emu, 'requests.txt')
    print 'Request throughput:', request_throughput
    emu.execute(['exit'])


def measure_throughput(emu, filename):
    """Reads requests from filename and forwards
    each of them to a random server"""
    start = time.time()
    request_count = 0
    with open(filename, 'r') as f:
        for file_line in f:
            line = file_line.rstrip()
            if line.startswith('query'):
                emu.execute(line.split(', '))
            elif len(line.split(', ')) == 1:
                emu.execute(['query', line])
            elif line.startswith('insert'):
                emu.execute(line.split(', '))
            elif len(line.split(', ')) == 2:
                emu.execute(['insert'] + line.split(', '))
            else:
                raise ValueError('Bad Request %s' % line)
            request_count += 1
    return (time.time() - start) / request_count


if __name__ == '__main__':
    main()