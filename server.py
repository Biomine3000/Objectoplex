# -*- coding: utf-8 -*-
"""
This module implements the machinery to create servers and provides a sample
implementation of a server called ObjectoPlex.
"""

import errno
import socket
import logging
import signal

from Queue import Queue

import gevent

from gevent.server import StreamServer
from gevent import Greenlet
from gevent.select import select

from system import BusinessObject, ObjectType
from middleware import StatisticsMiddleware, MultiplexingMiddleware, ChecksumMiddleware

logger = logging.getLogger('server')


class SystemClient(Greenlet):
    def __init__(self, socket, address, gateway):
        Greenlet.__init__(self)

        self.socket = socket
        self.address = address
        self.gateway = gateway

        self.queue = Queue()

    def _run(self):
        logger.info(u"Handling client from {0}".format(self.address))

        while True:
            if not self.queue.empty():
                rlist, wlist, xlist = select([self.socket], [self.socket], [], timeout=1)
            else:
                rlist, wlist, xlist = select([self.socket], [], [], timeout=1)

            if not self.queue.empty() and len(wlist) == 1:
                try:
                    self.socket.send(self.queue.get().serialize())
                except socket.error, e:
                    if e[0] == errno.ECONNRESET or e[0] == errno.EPIPE:
                        logger.warning(u"Received {0} from {1}".format(e, self.address))
                        self.close()
                        return
                    raise e
            elif len(rlist) == 1:
                logger.debug(u"Attempting to read an object from {0}".format(self.socket))
                try:
                    obj = BusinessObject.read_from_socket(self.socket)
                    if obj is None:
                        logger.error(u"Couldn't read object from {0}, closing!".format(self.socket))
                        self.close()
                        return
                    logger.debug(u"Successfully read object {0}".format(str(obj)))
                    self.gateway.send(obj, self)
                except socket.error, e:
                    if e[0] == errno.ECONNRESET or e[0] == errno.EPIPE:
                        logger.warning(u"Received {0} from {1}".format(e, self.address))
                        self.close()
                        return
                    raise e

    def send(self, message, sender):
        self.queue.put(message)

    def close(self):
        try:
            logger.warning(u"Closing connection to {0}".format(self.address))
            self.gateway.unregister(self)
            self.socket.close()
        except:
            pass


class ObjectoPlex(StreamServer):
    """
    ObjectoPlex is parameterized by giving a list of middleware classes.  The
    defaults are StatisticsMiddleware, ChecksumMiddleware and
    MultiplexingMiddleware.
    """
    def __init__(self, listener, middlewares=[], **kwargs):
        StreamServer.__init__(self, listener, **kwargs)
        self.clients = set()

        if len(middlewares) == 0:
            self.middlewares = [StatisticsMiddleware(),
                                ChecksumMiddleware(),
                                MultiplexingMiddleware()]
        else:
            self.middlewares = middlewares

    def handle(self, source, address):
        client = SystemClient(source, address, self)
        gevent.signal(signal.SIGTERM, client.kill)
        gevent.signal(signal.SIGINT, client.kill)
        self.clients.add(client)

        for middleware in self.middlewares:
            middleware.register(client)

        client.start()

    def send(self, message, sender):
        logger.info(u"{0}: {1}".format(sender, message))

        for middleware in self.middlewares:
            message = middleware.handle(message, sender, set(self.clients))
            if message is None:
                break

    def unregister(self, client):
        self.clients.remove(client)

        for middleware in self.middlewares:
            middleware.unregister(client)
