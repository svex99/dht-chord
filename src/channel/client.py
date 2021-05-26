import logging
import zmq


logging.basicConfig(
    format='[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s',
    level=logging.INFO
)


class ChannelClient:
    """
    Interface for communicate with Channel in a handy way.
    """
    def __init__(self, ip, port):
        conn_string = f'tcp://{ip}:{port}'

        self.sock = zmq.Context().socket(zmq.DEALER)
        self.sock.connect(conn_string)

        logging.info(f'ChannelClient connected to {conn_string}')

    def _invoke_rpc(self, name, args=[], kwargs={}):
        self.sock.send_pyobj(
            {
                'name': name,
                'args': args,
                'kwargs': kwargs,
            }
        )
        logging.info(f'Invoked RPC: {name}, {args}, {kwargs}')
        return self.sock.recv_pyobj().get('result', None)

    def join(self, subgroup):
        return self._invoke_rpc('join', [subgroup])

    def leave(self, subgroup):
        return self._invoke_rpc('leave', [subgroup])

    def exists(self, subgroup, pid):
        return self._invoke_rpc('exists', [subgroup, pid])

    def bind(self, pid):
        return self._invoke_rpc('bind', [pid])

    def subgroup(self, subgroup):
        return self._invoke_rpc('subgroup', [subgroup])

    def sendTo(self, destinationSet, message):
        return self._invoke_rpc('sendTo', [destinationSet, message])

    def sendToAll(self, message):
        return self._invoke_rpc('sendToAll', [message])

    def recvFromAny(self, timeout=0):
        return self._invoke_rpc('recvFromAny', [], {'timeout': timeout})

    def recvFrom(self, senderSet, timeout=0):
        return self._invoke_rpc('recvFrom', [senderSet], {'timeout': timeout})

    @property
    def nBits(self):
        return self._invoke_rpc('nBits')

    @property
    def MAXPROC(self):
        return self._invoke_rpc('MAXPROC')
