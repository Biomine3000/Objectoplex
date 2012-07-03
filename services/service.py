# -*- coding: utf-8 -*-
import logging
import socket
import select

from time import sleep

from system import BusinessObject, InvalidObject

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

    def start(self):
        self.connect()

    def _open(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        self.socket_file = self.socket.makefile()

    def register(self):
        BusinessObject({'event': "services/register",
                        'name': self.__class__.__service__},
                       None).tofile(self.socket_file)
        self.logger.info("Registered to server as service %s" %
                         self.__class__.__service__)

    def connect(self):
        while True:
            try:
                self._open()
                self.register()
                self.receive()
                sleep(10)
            except socket.error, e:
                self.logger.warning(u"{0}:{1}".format(self.host, self.port), e)

    def receive(self):
        while True:
            try:
                rlist, wlist, xlist = select.select([self.socket], [], [], 1)
                if len(rlist) > 0:
                    obj = BusinessObject.read_from_socket(self.socket)
                    if obj is None:
                        raise InvalidObject
                    if self.should_handle(obj):
                        response = self.handle(obj)
                        if response is not None:
                            response.tofile(self.socket_file)
            except InvalidObject, ivo:
                self.socket.close()
                break
            except KeyboardInterrupt, kbi:
                self.socket.close()
                raise kbi

    def should_handle(self, obj):
        if obj.event != 'service/request' or \
               obj.metadata.get('name', None) != self.__class__.__service__:
            return False
        return True

    def handle(self, obj):
        pass

    def cleanup(self):
        self.socket.close()
