"""Master process for the DHT

Spawns the DHT servers and clients, issues requests towards the servers
and overall oversights the DHT. In fact, it's the emulator"""

import time

from server import server


def spawn_server(server_id):
    """Spawns a new DHT server and calls its join method"""
    new_server = server(server_id)
    new_server.join()


def measure_throughput(filename):
    start = time.time()
    count = 0
    with open(filename, 'r') as f:
        for line in f:
            req = Request(line)
            req.send()
            count += 1
    return start - time.time() / count


def main():
    raise NotImplementedError


if __name__ == '__main__':
    main()

