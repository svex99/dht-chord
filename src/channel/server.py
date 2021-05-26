import logging
import pickle
import random
import threading

import redis
import zmq

from constChord import LEAVE


logging.basicConfig(
    format='[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s',
    level=logging.INFO
)


def add(list_, *args, **kwargs):
    """
    Append function name to a list.
    """
    def wrapped_func(f, *args, **kwargs):
        list_.append(f.__name__)
        return f

    return wrapped_func


class Channel():

    endpoints = []              # exposed functions for RPC

    def __init__(self, nBits=5, hostIP='redis', portNo=6379, numw=10):
        self.channel = redis.Redis(host=hostIP, port=portNo, db=0)
        self.members = {}
        self._nBits = nBits
        self._MAXPROC = pow(2, nBits)

        self.ctx = zmq.Context()
        self.clients_sock = self.ctx.socket(zmq.ROUTER)
        self.workers_sock = self.ctx.socket(zmq.DEALER)
        self.stop_workers = threading.Event()   # pill for kill worker's thread
        self.num_workers = numw

    @add(endpoints)
    def join(self, subgroup, _caller_id=None):
        members = self.channel.smembers('members')
        newpid = random.choice(
            list(set([i for i in range(self._MAXPROC)]) - set([int(m) for m in members])))

        if len(members) > 0:
            xchan = [[newpid, other] for other in members] + \
                [[other, str(newpid)] for other in members]
            for xc in xchan:
                self.channel.rpush('xchan', pickle.dumps(xc))
        self.channel.sadd('members', newpid)
        self.channel.sadd(subgroup, newpid)
        return newpid

    @add(endpoints)
    def leave(self, subgroup, _caller_id=None):
        caller = self.members[_caller_id]
        assert self.channel.sismember('members', caller), ''

        del self.members[_caller_id]
        self.channel.srem('members', caller)

        # members = [int(m) for m in self.channel.smembers('members')]
        # if len(members) > 0:
            # xchan = [[caller, other] for other in members] + \
            #     [[other, caller] for other in members]

            # for xc in xchan:
            #     self.channel.rpop('xchan', pickle.dumps(xc))

            # for m in members:
            #     self.channel.rpush(f'{caller}->{m}', pickle.dumps((LEAVE, '')))

        self.channel.srem(subgroup, caller)

    @add(endpoints)
    def exists(self, subgroup, pid, _caller_id=None):
        return self.channel.sismember(subgroup, str(pid))

    @add(endpoints)
    def bind(self, pid, _caller_id=None):
        self.members[_caller_id] = pid

    @add(endpoints)
    def subgroup(self, subgroup, _caller_id=None):
        return [int(m) for m in self.channel.smembers(subgroup)]

    @add(endpoints)
    def sendTo(self, destinationSet, message, _caller_id=None):
        caller = self.members[_caller_id]
        assert self.channel.sismember('members', caller), ''

        for i in destinationSet:
            assert self.channel.sismember('members', i), ''
            self.channel.rpush(f'{caller}->{i}', pickle.dumps(message))

    @add(endpoints)
    def sendToAll(self, message, _caller_id=None):
        caller = self.members[_caller_id]
        assert self.channel.sismember('members', caller), ''
        for i in self.channel.smembers('members'):
            self.channel.rpush(f'{caller}->{int(i)}', pickle.dumps(message))

    @add(endpoints)
    def recvFromAny(self, timeout=0, _caller_id=None):
        caller = self.members[_caller_id]
        assert self.channel.sismember('members', caller), ''
        members = [int(m) for m in self.channel.smembers('members')]
        xchan = [f'{i}->{caller}' for i in members]

        msg = self.channel.blpop(xchan, timeout)

        if msg:
            return [int(msg[0].split(b'->')[0]), pickle.loads(msg[1])]

        return None

    @add(endpoints)
    def recvFrom(self, senderSet, timeout=0, _caller_id=None):
        caller = self.members[_caller_id]
        assert self.channel.sismember('members', caller), ''
        for i in senderSet:
            assert self.channel.sismember('members', i), ''

        xchan = [f'{i}->{caller}' for i in senderSet]
        msg = self.channel.blpop(xchan, timeout)

        if msg:
            return [int(msg[0].split(b'->')[0]), pickle.loads(msg[1])]

    @add(endpoints)
    def nBits(self, _caller_id=None):
        return self._nBits

    @add(endpoints)
    def MAXPROC(self, _caller_id=None):
        return self._MAXPROC

    def stop(self):
        # close clients socket
        self.clients_sock.close(linger=1)
        logging.info('Closed clients socket')

        # close workers socket
        self.workers_sock.close(linger=1)
        logging.info('Closed workers socket')

        # stop workers
        logging.info('Stopping workers...')
        self.stop_workers.set()

        # end context
        self.ctx.term()

    def worker(self):
        """
        Process RPCs from chord nodes and clients.
        """
        sock = self.ctx.socket(zmq.DEALER)
        sock.connect('inproc://workers')

        while not self.stop_workers.is_set():
            try:
                event = sock.poll(timeout=1000)
            except zmq.error.ContextTerminated:
                break
            else:
                if event == 0:
                    continue

            client_id = sock.recv()
            data = sock.recv_pyobj()

            name = data.get('name', '')
            args = data.get('args', [])
            kwargs = data.get('kwargs', {})
            kwargs['_caller_id'] = hash(client_id)    # replace os.getpid()

            try:
                # raise AttributeError if function is not defined
                func = self.__getattribute__(name)

                # raise AttributeError if function is not public for RPC
                if name not in self.endpoints:
                    raise AttributeError()
            except AttributeError:
                logging.error(f'Invoked invalid endpoint: \'{name}\'')
            else:
                # invoke RPC function
                logging.info(f'Invoked RPC: {name} {args} {kwargs}')

                result = func(*args, **kwargs)

                try:
                    sock.send(client_id, zmq.SNDMORE)
                    sock.send_pyobj({'result': result})
                except zmq.ContextTerminated:
                    break

        sock.close(linger=1)
        logging.info(f'Stopped {threading.current_thread().name}')

    def run(self):
        """
        Send RPCs from chord nodes and clients to workers.
        Expected dict format for RPC:
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
        Response dict format:
        {
            result: <result>,
        }
        The transport is made with zmq.Socket.send_pyobj and
        zmq.Socket.recv_pyobj methods.
        """
        self.channel.flushall()

        port = 1207
        clients_conn = f'tcp://*:{port}'
        self.clients_sock.bind(clients_conn)
        logging.info(f'Channel ready in {clients_conn}')
        logging.info(f'Public Endpoints: {self.endpoints}')

        workers_conn = 'inproc://workers'
        self.workers_sock.bind(workers_conn)

        # launch pool of working threads
        for i in range(self.num_workers):
            thread = threading.Thread(target=self.worker, name=f'worker{i}')
            # self.workers_pool.append(thread)
            thread.start()
            logging.info(f'Started worker{i}')

        zmq.device(zmq.QUEUE, self.clients_sock, self.workers_sock)
