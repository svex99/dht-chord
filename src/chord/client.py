import logging
import random

from channel.client import ChannelClient
from constChord import LOOKUP_REQ, STOP


class ChordClient:
    def __init__(self, chan_ip, chan_port):
        self.chan = ChannelClient(chan_ip, chan_port)
        self.nodeID = self.chan.join('client')

    def run(self):
        self.chan.bind(self.nodeID)
        procs = self.chan.subgroup('node')
        procs.sort()
        logging.info(['%04d' % k for k in procs])

        p = procs[random.randint(0, len(procs)-1)]
        key = random.randint(0, self.chan.MAXPROC-1)
        logging.info(f'{self.nodeID} sending LOOKUP request for {key} to {p}')

        self.chan.sendTo([p], (LOOKUP_REQ, key))
        msg = self.chan.recvFrom([p])
        while msg[1][1] != p:
            p = msg[1][1]
            self.chan.sendTo([p], (LOOKUP_REQ, key))
            msg = self.chan.recvFrom([p])
        logging.info(f'{self.nodeID} received final answer from {p}')
        self.chan.sendTo(procs, (STOP))
