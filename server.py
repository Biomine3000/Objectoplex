# -*- coding: utf-8 -*-
"""
This module implements the machinery to run servers.
"""

import errno
import logging
import signal
import traceback

from time import sleep
from datetime import datetime, timedelta

import gevent

from gevent.server import StreamServer
from gevent import Greenlet
from gevent import socket
from gevent.select import select
from gevent.queue import Queue, Empty

from system import BusinessObject, ObjectType

logger = logging.getLogger('server')


class Sender(Greenlet):
    def __init__(self, client):
        Greenlet.__init__(self)
        self.client = client

    def _run(self):
        client = self.client
        logger.info(u"Sender handling connection from {0}".format(client.address))

        while True:
            try:
                obj = client.queue.get(timeout=1.0)
                size, sent = obj.serialize(socket=client.socket)
                # logger.debug(u"Sent {0}/{1} of {2}".format(sent, size, obj))
                last_activity = datetime.now()
            except Empty, empty:
                pass
            except socket.error, e:
                if e[0] == errno.ECONNRESET or e[0] == errno.EPIPE:
                    # logger.warning()
                    client.close(u"{0}".format(e))
                    return
                raise e


class Receiver(Greenlet):
    def __init__(self, client):
        Greenlet.__init__(self)
        self.client = client

    def _run(self):
        client = self.client
        logger.info(u"Receiver handling connection from {0}".format(client.address))

        last_activity = datetime.now()
        while True:
            if client.server and last_activity + timedelta(minutes=30) < datetime.now():
                client.close('inactivity')
                return

            rlist, wlist, xlist = select([client.socket], [], [], timeout=1.0)

            if len(rlist) == 1:
                # logger.debug(u"Attempting to read an object from {0}".format(self.socket))
                try:
                    obj = BusinessObject.read_from_socket(client.socket)
                    if obj is None:
                        client.close("couldn't read object")
                        return
                    # logger.debug(u"Successfully read object {0}".format(str(obj)))
                    client.gateway.send(obj, client)
                    last_activity = datetime.now()
                except socket.error, e:
                    if e[0] == errno.ECONNRESET or e[0] == errno.EPIPE:
                        client.close(u"{0}".format(e))
                        return
                    raise e


class SystemClient(object):
    def __init__(self, socket, address, gateway, server=False):
        self.socket = socket
        self.address = address
        self.gateway = gateway
        self.server = server
        self.queue = Queue(maxsize=100)

        self.receiver = Receiver(self)
        self.sender = Sender(self)

    def start(self):
        self.receiver.start()
        self.sender.start()

    def kill(self):
        self.receiver.kill()
        self.sender.kill()

    def send(self, message, sender):
        self.queue.put(message)

    def close(self, message=""):
        try:
            logger.warning(u"Closing connection to {0} due to {1}".format(self.address, message))
            self.gateway.unregister(self)
            self.socket.close()
            self.receiver.kill()
            self.sender.kill()
        except Exception, e:
            traceback.print_exc()
            logger.error(u"Got {0} while trying to close and unregister a client!".format(e))

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
            self.open_link(linked_server)

        self.previous_sent = None

    def open_link(self, server):
        while True:
            try:
                client = self._open_link_helper(server)
                for middleware in self.middlewares:
                    try:
                        middleware.connect(client, set(self.clients))
                    except Exception, e:
                        traceback.print_exc()
                        logger.error(u"Got {0} while calling {1}.connect!".format(e, middleware))
                break
            except socket.error, e:
                logger.warning(u"Unable to connect to linked server {0}: {1}".format(server, e))

            sleep(10)

    def _open_link_helper(self, listener):
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
        return client

    def handle(self, source, address):
        client = SystemClient(source, address, self)
        gevent.signal(signal.SIGTERM, client.kill)
        gevent.signal(signal.SIGINT, client.kill)

        for middleware in self.middlewares:
            try:
                middleware.connect(client, set(self.clients))
            except Exception, e:
                traceback.print_exc()
                logger.error(u"Got {0} while calling {1}.connect!".format(e, middleware))

        self.clients.add(client)

        client.start()

    def send(self, message, sender):
        if self.previous_sent != message:
            logger.info(u"{0}: {1}".format(sender, message))
        self.previous_sent = message

        for middleware in self.middlewares:
            try:
                message = middleware.handle(message, sender, set(self.clients))
                if message is None:
                    break
            except Exception, e:
                traceback.print_exc()
                logger.error(u"Got {0} while calling {1}.handle!".format(e, middleware))

    def unregister(self, client):
        self.clients.remove(client)

        for middleware in self.middlewares:
            try:
                middleware.disconnect(client, set(self.clients))
            except Exception, e:
                traceback.print_exc()
                logger.error(u"Got {0} while calling {1}.disconnect!".format(e, middleware))

        if client.server and hasattr(client, 'host'):
            self.open_link((client.host, client.port))
