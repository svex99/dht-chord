import logging
from argparse import ArgumentParser

from channel.server import Channel


parser = ArgumentParser()
parser.add_argument('--nbits', type=int, default=5, help='Bits for identifier space')
parser.add_argument('--ip', type=str, default='redis', help='IP address of redis server')
parser.add_argument('--port', type=int, default=6379, help='Port of redis server')
parser.add_argument('--numw', type=int, default=10, help='Number of worker threads')
args = parser.parse_args()

channel = Channel(
    nBits=args.nbits,
    hostIP=args.ip,
    portNo=args.port,
    numw=args.numw
)
try:
    channel.run()
except KeyboardInterrupt:
    channel.stop()
    logging.info('Channel stopped by user')
