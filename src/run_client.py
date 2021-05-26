import logging
import sys

from chord.client import ChordClient


if len(sys.argv) > 2:
    ip = sys.argv[1]
    port = sys.argv[2]

    client = ChordClient(ip, port)
    client.run()
else:
    logging.error('Usage: client.py <ip> <port>')
