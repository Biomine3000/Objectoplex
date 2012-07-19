# -*- coding: utf-8 -*-
import logging
import socket
import select
import signal
import errno
import fcntl

from contextlib import contextmanager
from Queue import Queue
from time import sleep

from system import BusinessObject, InvalidObject


@contextmanager
def timeout(seconds):
    """
    Raises IOError with e.errno == errno.EINTR when it times out.
    """
    def timeout_handler(signum, frame):
        pass

    original_handler = signal.signal(signal.SIGALRM, timeout_handler)

    try:
        signal.alarm(seconds)
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, original_handler)


class _MetaService(type):
    def __new__(cls, clsname, clsbases, clsdict):  
        t = type.__new__(cls, clsname, clsbases, clsdict)  
  
        service = getattr(t, '__service__', None)

        if service is not None:
            clsdict['__slots__'] = ['__service__', 'host', 'port']
            t = type.__new__(cls, clsname, clsbases, clsdict)  
            t.__service__ = service

        return t


class Service(object):
    __metaclass__ = _MetaService

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.logger = logging.getLogger(self.__class__.__service__)
        self.queue = Queue()

    def start(self):
        self.connect()

    def _open(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))

    def subscribe(self):
        BusinessObject({ 'event': "routing/subscribe",
                         'receive-mode': 'all',
                         'types': 'all' },
                       None).serialize(socket=self.socket)
        self.logger.info("Subscribed to server")

    def register(self):
        BusinessObject({'event': "services/register",
                        'name': self.__class__.__service__},
                       None).serialize(socket=self.socket)
        self.logger.info("Registered to server as service '%s'" %
                         self.__class__.__service__)

    def connect(self):
        while True:
            try:
                self._open()
                self.subscribe()
                self.register()
                self.receive()
            except socket.error, e:
                self.logger.warning(u"{0}:{1}; {2}".format(self.host, self.port, e))
            sleep(10)

    def receive(self):
        while True:
            try:
                if not self.queue.empty():
                    rlist, wlist, xlist = select.select([self.socket], [self.socket], [], 0.2)
                    if len(wlist) > 0:
                        obj = self.queue.get()
                        obj.serialize(socket=self.socket)
                else:
                    rlist, wlist, xlist = select.select([self.socket], [], [], 0.2)
                    if len(rlist) > 0:
                        obj = BusinessObject.read_from_socket(self.socket)
                        if obj is None:
                            raise InvalidObject
                        if obj.event == 'services/discovery':
                            response = self.handle_discovery(obj)
                            if response is not None:
                                self.queue.put(response)
                        elif self.should_handle(obj):
                            response = self.handle(obj)
                            if response is not None:
                                self.queue.put(response)
                # TODO: implement functionality for other than pure stimuli induced behavior
            except InvalidObject, ivo:
                self.socket.close()
                break
            except KeyboardInterrupt, kbi:
                self.socket.close()
                raise kbi

    def should_handle(self, obj):
        if obj.event != 'services/request' or \
               obj.metadata.get('name', None) != self.__class__.__service__:
            return False
        return True

    def handle_discovery(self, obj):
        metadata = { 'event': "services/discovery/reply",
                     'name': self.__class__.__service__,
                     'in-reply-to': obj.id }

        if 'route' in obj.metadata:
            metadata['to'] = obj.metadata['route'][0]

        return BusinessObject(metadata, None)

    def handle(self, obj):
        pass

    def cleanup(self):
        self.socket.close()
