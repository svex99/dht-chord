import logging
import sys

from chord.node import ChordNode


if len(sys.argv) > 2:
    ip = sys.argv[1]
    port = sys.argv[2]

    node = ChordNode(ip, port)
    node.run()
else:
    logging.error('Usage: node.py <ip> <port>')