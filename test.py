#!/usr/bin/env python

"""Debug script to be used in development"""

import sys
import time
import logging

from emulator import Emulator

logging.basicConfig(filename='debug.log', level=logging.DEBUG)


def main():
    """Main script

    Acts as a terminal towards the DHT"""
    emu = Emulator(int(sys.argv[1]))
    emu.print_help('')
    for i in range(2, 10):
        emu.execute(['join', str(i)])

    emu.execute(['insert', 'Imagine', '2'])
    emu.execute(['insert', 'Ameranhs', '3'])
    emu.execute(['insert', 'Valkanos', '4'])
    emu.execute(['query', '*'])

    while True:
        command = raw_input('Action: ').split(', ')
        emu.execute(command)

        if command[0] == 'exit':
            break
        time.sleep(1)
    logging.debug('END')


if __name__ == '__main__':
    main()