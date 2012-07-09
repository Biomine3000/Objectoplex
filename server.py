# -*- coding: utf-8 -*-
"""
This module implements the machinery to create servers and provides a sample
implementation of a server called ObjectoPlex.
"""

import errno
import socket
import logging
import signal

from time import sleep
from Queue import Queue
from datetime import datetime, timedelta

import gevent

from gevent.server import StreamServer
from gevent import Greenlet
from gevent.select import select

from system import BusinessObject, ObjectType

logger = logging.getLogger('server')


class SystemClient(Greenlet):
    def __init__(self, socket, address, gateway, server=False):
        Greenlet.__init__(self)

        self.socket = socket
        self.address = address
        self.gateway = gateway
        self.server = server

        self.queue = Queue()

    def _run(self):
        logger.info(u"Handling client from {0}".format(self.address))

        last_activity = datetime.now()
        while True:
            if self.server and last_activity + timedelta(minutes=60) < datetime.now():
                logger.warning(u"Closing connection {0} due to inactivity".format(self))
                self.close()
                return

            if not self.queue.empty():
                rlist, wlist, xlist = select([self.socket], [self.socket], [], timeout=0.2)
            else:
                rlist, wlist, xlist = select([self.socket], [], [], timeout=0.2)

            if not self.queue.empty() and len(wlist) == 1:
                try:
                    obj = self.queue.get()
                    size, sent = obj.serialize(socket=self.socket)
                    logger.debug(u"Sent {0}/{1} of {2}".format(sent, size, obj))
                    last_activity = datetime.now()
                except socket.error, e:
                    if e[0] == errno.ECONNRESET or e[0] == errno.EPIPE:
                        logger.warning(u"Received {0} from {1}".format(e, self.address))
                        self.close()
                        return
                    raise e
            if len(rlist) == 1:
                logger.debug(u"Attempting to read an object from {0}".format(self.socket))
                try:
                    obj = BusinessObject.read_from_socket(self.socket)
                    if obj is None:
                        logger.error(u"Couldn't read object from {0}, closing!".format(self.socket))
                        self.close()
                        return
                    logger.debug(u"Successfully read object {0}".format(str(obj)))
                    self.gateway.send(obj, self)
                    last_activity = datetime.now()
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

    def __unicode__(self):
        return u'<{0} {1}, server: {2}>'.format(self.__class__.__name__, self.address, self.server)

    def __str__(self):
        return unicode(self).encode('ASCII', 'backslashreplace')



class ObjectoPlex(StreamServer):
    """
    ObjectoPlex is parameterized by giving a list of middleware classes.  The
    defaults are StatisticsMiddleware, ChecksumMiddleware and
    MultiplexingMiddleware.
    """
    def __init__(self, listener, middlewares=[], linked_servers=[], **kwargs):
        StreamServer.__init__(self, listener, **kwargs)
        self.clients = set()

        from middleware import StatisticsMiddleware, MultiplexingMiddleware, ChecksumMiddleware
        if len(middlewares) == 0:
            self.middlewares = [StatisticsMiddleware(),
                                ChecksumMiddleware(),
                                MultiplexingMiddleware()]
        else:
            self.middlewares = middlewares

        for linked_server in linked_servers:
            while True:
                try:
                    self.open_link(linked_server)
                    break
                except Exception, e:
                    logger.warning(u"Unable to connect to linked server: {0}".format(e))
                sleep(10)

    def open_link(self, listener):
        logger.info(u"Connecting to server at {0}:{1}".format(*listener))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(listener)
        logger.info(u"Socket opened to {0}:{1}".format(*listener))

        client = SystemClient(sock, listener, self, server=True)
        gevent.signal(signal.SIGTERM, client.kill)
        gevent.signal(signal.SIGINT, client.kill)

        self.clients.add(client)
        client.host = listener[0]
        client.port = listener[1]
        client.start()
        logger.info(u"Connected to server at {0}:{1}".format(*listener))

    def handle(self, source, address):
        client = SystemClient(source, address, self)
        gevent.signal(signal.SIGTERM, client.kill)
        gevent.signal(signal.SIGINT, client.kill)

        for middleware in self.middlewares:
            middleware.connect(client, set(self.clients))

        self.clients.add(client)

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
            middleware.disconnect(client, set(self.clients))

        if client.server:
            self.open_link((client.host, client.port))
