import logging
import random

from channel import ChannelClient
from constChord import JOIN, LEAVE, LOOKUP_REP, LOOKUP_REQ, STOP


logging.basicConfig(
    format='[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s',
    level=logging.INFO
)


class ChordNode:

    def __init__(self, chan_ip, chan_port):
        self.chan = ChannelClient(chan_ip, chan_port)   # Create ref to actual channel
        self.nBits = self.chan.nBits                    # Num of bits for the ID space
        self.MAXPROC = self.chan.MAXPROC                # Maximum num of processes
        # Find out who you are
        self.nodeID = self.chan.join('node')
        # FT[0] is predecessor
        self.FT = [None for i in range(self.nBits+1)]
        self.nodeSet = []               # Nodes discovered so far

    def inbetween(self, key, lwb, upb):
        if lwb <= upb:
            return lwb <= key and key < upb
        else:
            return (lwb <= key and key < upb + self.MAXPROC) or (lwb <= key + self.MAXPROC and key < upb)

    def addNode(self, nodeID):
        self.nodeSet.append(nodeID)
        self.nodeSet = list(set(self.nodeSet))
        self.nodeSet.sort()

    def delNode(self, nodeID):
        assert nodeID in self.nodeSet, ''
        del self.nodeSet[self.nodeSet.index(nodeID)]
        self.nodeSet.sort()

    def finger(self, i):
        succ = (self.nodeID + pow(2, i-1)) % self.MAXPROC    # succ(p+2^(i-1))
        # own index in nodeset
        lwbi = self.nodeSet.index(self.nodeID)
        # index next neighbor
        upbi = (lwbi + 1) % len(self.nodeSet)
        for k in range(len(self.nodeSet)):                   # go through all segments
            if self.inbetween(succ, self.nodeSet[lwbi]+1, self.nodeSet[upbi]+1):
                # found successor
                return self.nodeSet[upbi]
            (lwbi, upbi) = (upbi, (upbi+1) %
                            len(self.nodeSet))  # go to next segment
        return None

    def recomputeFingerTable(self):
        self.FT[0] = self.nodeSet[self.nodeSet.index(
            self.nodeID)-1]  # Predecessor
        self.FT[1:] = [self.finger(i)
                       for i in range(1, self.nBits+1)]  # Successors

    def localSuccNode(self, key):
        # key in (FT[0],self]
        if self.inbetween(key, self.FT[0]+1, self.nodeID+1):
            return self.nodeID                                 # node is responsible
        # key in (self,FT[1]]
        elif self.inbetween(key, self.nodeID+1, self.FT[1]):
            # successor responsible
            return self.FT[1]
        for i in range(1, self.nBits+1):                     # go through rest of FT
            if self.inbetween(key, self.FT[i], self.FT[(i+1) % self.nBits]):
                # key in [FT[i],FT[i+1])
                return self.FT[i]

    def run(self):
        self.chan.bind(self.nodeID)
        self.addNode(self.nodeID)
        others = list(set([other for other in self.chan.subgroup('node')]) - set([self.nodeID]))
        logging.info(f'Other members: {others}')

        for i in others:
            self.addNode(i)
            self.chan.sendTo([i], (JOIN, ''))

        self.recomputeFingerTable()

        while True:
            message = None
            while message is None:
                message = self.chan.recvFromAny(timeout=1)  # Wait for any request

            sender = message[0]                         # Identify the sender
            request = message[1]                        # And the actual request

            if request[0] != LEAVE and self.chan.exists('node', sender):
                self.addNode(sender)
            if request[0] == STOP:
                break
            if request[0] == LOOKUP_REQ:    # A lookup request
                logging.info(f'Lookup request from {sender}: {request[1]}')
                # look up next node
                nextID = self.localSuccNode(request[1])
                # return to sender
                self.chan.sendTo([sender], (LOOKUP_REP, nextID))
                if not self.chan.exists('members', nextID):
                    self.delNode(nextID)
                continue
            elif request[0] == JOIN:
                logging.info(f'Join request from {sender}: {request[1]}')
            elif request[0] == LEAVE:
                logging.info(f'Leave request from {sender}: {request[1]}')
                self.delNode(sender)

            self.recomputeFingerTable()

        logging.info(
            'FT[' + '%04d' % self.nodeID + ']:' + str(['%04d' % k for k in self.FT]))


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
