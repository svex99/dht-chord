import logging
import os
import pickle
import random

import redis
import zmq


logging.basicConfig(
    format='[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s',
    level=logging.INFO
)


def add(list_, *args, **kwargs):
    """
    Append function to a list.
    """
    def wrapped_func(f, *args, **kwargs):
        list_.append(f)
        return f

    return wrapped_func


class Channel():

    public_rpc = []  # exposed functions to RPC

    def __init__(self, nBits=5, hostIP='redis', portNo=6379):
        self.channel = redis.Redis(host=hostIP, port=portNo, db=0)
        self.osmembers = {}
        self.nBits = nBits
        self.MAXPROC = pow(2, nBits)

        self.sock = zmq.Context().socket(zmq.ROUTER)

    @add(public_rpc)
    def join(self, subgroup):
        members = self.channel.smembers('members')
        newpid = random.choice(
            list(set([str(i) for i in range(self.MAXPROC)]) - members))
        if len(members) > 0:
            xchan = [[str(newpid), other] for other in members] + \
                [[other, str(newpid)] for other in members]
            for xc in xchan:
                self.channel.rpush('xchan', pickle.dumps(xc))
        self.channel.sadd('members', str(newpid))
        self.channel.sadd(subgroup, str(newpid))
        return str(newpid)

    # def leave(self, subgroup):
    #     ospid = os.getpid()
    #     pid = self.osmembers[ospid]
    #     assert self.channel.sismember('members', str(pid)), ''
    #     del self.osmembers[ospid]
    #     self.channel.sdel('members', str(pid))
    #     members = self.channel.smembers('members')
    #     if len(members) > 0:
    #         xchan = [[str(pid), other] for other in members] + \
    #             [[other, str(pid)] for other in members]
    #         for xc in xchan:
    #             self.channel.rpop('xchan', pickle.dumps(xc))
    #     self.channel.sdel(subgroup, str(pid))
    #     return

    @add(public_rpc)
    def exists(self, pid):
        return self.channel.sismember('members', str(pid))

    @add(public_rpc)
    def bind(self, pid):
        ospid = os.getpid()
        self.osmembers[ospid] = str(pid)
        # print "Process "+str(ospid)+" ["+pid+"] joined "
        # print self.osmembers

    # def subgroup(self, subgroup):
    #     return list(self.channel.smembers(subgroup))

    @add(public_rpc)
    def sendTo(self, destinationSet, message):
        caller = self.osmembers[os.getpid()]
        assert self.channel.sismember('members', str(caller)), ''
        for i in destinationSet:
            assert self.channel.sismember('members', str(i)), ''
            self.channel.rpush([str(caller), str(i)], pickle.dumps(message))

    # def sendToAll(self, message):
    #     caller = self.osmembers[os.getpid()]
    #     assert self.channel.sismember('members', str(caller)), ''
    #     for i in self.channel.smembers('members'):
    #         self.channel.rpush([str(caller), str(i)], pickle.dumps(message))

    @add(public_rpc)
    def recvFromAny(self, timeout=0):
        caller = self.osmembers[os.getpid()]
        assert self.channel.sismember('members', str(caller)), ''
        members = self.channel.smembers('members')
        xchan = [[str(i), str(caller)] for i in members]
        msg = self.channel.blpop(xchan, timeout)
        if msg:
            return [msg[0].split("'")[1], pickle.loads(msg[1])]

    @add(public_rpc)
    def recvFrom(self, senderSet, timeout=0):
        caller = self.osmembers[os.getpid()]
        assert self.channel.sismember('members', str(caller)), ''
        for i in senderSet:
            assert self.channel.sismember('members', str(i)), ''
        xchan = [[str(i), str(caller)] for i in senderSet]
        msg = self.channel.blpop(xchan, timeout)
        if msg:
            return [msg[0].split("'")[1], pickle.loads(msg[1])]

    def run(self):
        """
        Process RPCs from chord nodes and clients.
        JSON expected format for RPC:
        {
            name: "method_name",
            args: [
                arg_1,
                arg_2,
                ...,
                arg_n
            ],
            kwargs: {
                kwarg_1: value_1,
                kwarg_2: value_2,
                ...,
                kwarg_n: value_n
            }
        }
        """
        port = 1207
        self.sock.bind(f'tcp://*:{port}')

        while True:
            _ = self.sock.recv()
            data = self.sock.recv_json()

            name = data.get('name', '')
            args = data.get('args', [])
            kwargs = data.get('kwargs', {})

            try:
                # raise AttributeError if function is not defined
                func = self.__getattribute__(name)

                # raise AttributeError if function is not public for RPC
                if func not in self.public_rpc:
                    raise AttributeError()
            except AttributeError:
                logging.error(f'Invoked invalid endpoint: \'{name}\'')
            else:
                # invoke RPC function
                func(*args, **kwargs)
