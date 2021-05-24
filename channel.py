import logging
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
        self.members = {}
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
    def bind(self, pid, _caller_id=None):
        # ospid = os.getpid()
        self.members[_caller_id] = str(pid)
        # print "Process "+str(ospid)+" ["+pid+"] joined "
        # print self.osmembers

    @add(public_rpc)
    def subgroup(self, subgroup):
        return list(self.channel.smembers(subgroup))

    @add(public_rpc)
    def sendTo(self, destinationSet, message, _caller_id=None):
        caller = self.members[_caller_id]
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
    def recvFromAny(self, timeout=0, _caller_id=None):
        caller = self.members[_caller_id]
        assert self.channel.sismember('members', str(caller)), ''
        members = self.channel.smembers('members')
        xchan = [[str(i), str(caller)] for i in members]
        msg = self.channel.blpop(xchan, timeout)
        if msg:
            return [msg[0].split("'")[1], pickle.loads(msg[1])]

    @add(public_rpc)
    def recvFrom(self, senderSet, timeout=0, _caller_id=None):
        caller = self.members[_caller_id]
        assert self.channel.sismember('members', str(caller)), ''
        for i in senderSet:
            assert self.channel.sismember('members', str(i)), ''
        xchan = [[str(i), str(caller)] for i in senderSet]
        msg = self.channel.blpop(xchan, timeout)
        if msg:
            return [msg[0].split("'")[1], pickle.loads(msg[1])]

    @add(public_rpc)
    def nBits(self):
        return self.nBits

    @add(public_rpc)
    def MAXPROC(self):
        return self.MAXPROC

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
            client_id = self.sock.recv()
            data = self.sock.recv_json()

            name = data.get('name', '')
            args = data.get('args', [])
            kwargs = data.get('kwargs', {})
            kwargs['_caller_id'] = client_id    # replace os.getpid()

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
                result = func(*args, **kwargs)

                self.sock.send(client_id, zmq.SNDMORE)
                self.sock.send_json({'result': result})


class ChannelClient:
    """
    Interface for communicate with Channel in a handy way.
    """
    def __init__(self, ip, port):
        conn_string = f'tcp://{ip}:{port}'

        self.sock = zmq.Context().socket(zmq.REQ)
        self.sock.connect(conn_string)

        logging.info(f'ChannelClient connected to {conn_string}')

    def _invoke_rpc(self, name, args=[], kwargs={}):
        self.sock.send_json(
            {
                'name': name,
                'args': args,
                'kwargs': kwargs,
            }
        )
        return self.sock.recv_json().get('result', None)

    def join(self, subgroup):
        return self._invoke_rpc('join', [subgroup])

    def exists(self, pid):
        return self._invoke_rpc('exists', [pid])

    def bind(self, pid):
        return self._invoke_rpc('bind', [pid])

    def subgroup(self, subgroup):
        return self._invoke_rpc('subgroup', [subgroup])

    def sendTo(self, destinationSet, message):
        return self._invoke_rpc('sendTo', [destinationSet, message])

    def recvFromAny(self, timeout=0):
        return self._invoke_rpc('recvFromAny', [], {'timeout': timeout})

    def recvFrom(self, senderSet, timeout=0):
        return self._invoke_rpc('revFrom', [senderSet], {'timeout': timeout})

    @property
    def nBits(self):
        return self._invoke_rpc('nBits')

    @property
    def MAXPROC(self):
        return self._invoke_rpc('MAXPROC')
