#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import signal

from unittest import TestCase
from unittest import main as unittest_main
from optparse import OptionParser
from datetime import datetime, timedelta

import socket
import json
from uuid import uuid4

import gevent

from gevent import Greenlet
from gevent import socket
from gevent import sleep
from gevent import select

from system import BusinessObject, InvalidObject
from server import ObjectoPlex
from middleware import *
from services.client_registry import ClientRegistry

logger = logging.getLogger("tests")


def reply_for_object(obj, sock, timeout_secs=1.0):
    """
    Waits for a reply to a sent object (connected by in-reply-to field).

    Returns the object and seconds elapsed as tuple (obj, secs).
    """
    started = datetime.now()
    delta = timedelta(seconds=timeout_secs)
    while True:
        rlist, wlist, xlist = select.select([sock], [], [], 0.0001)

        if datetime.now() > started + delta:
            return None, timeout_secs

        if len(rlist) == 0:
            continue

        reply = BusinessObject.read_from_socket(sock)

        if reply is None:
            raise InvalidObject
        elif reply.metadata.get('in-reply-to', None) == obj.id:
            return reply, (datetime.now() - started).seconds


class BaseTestCase(TestCase):
    def start_server(self, host, port, linked_servers=[]):
        result = ObjectoPlex((host, port),
                             middlewares=[
                                 LegacySubscriptionMiddleware(),
                                 StatisticsMiddleware(),
                                 ChecksumMiddleware(),
                                 RoutingMiddleware(),
                                 ],
                             linked_servers=linked_servers)
        gevent.signal(signal.SIGTERM, result.stop)
        gevent.signal(signal.SIGINT, result.stop)
        Greenlet.spawn(result.serve_forever)
        sleep(0.1)

        return result

    def start_client_registry(self, host, port):
        self.service = ClientRegistry(host, port)
        self.service_greenlet = Greenlet(self.service.start)
        gevent.signal(signal.SIGTERM, self.service_greenlet.kill)
        gevent.signal(signal.SIGINT, self.service_greenlet.kill)
        self.service_greenlet.start()
        logger.info('Started client registry, connecting to %s:%s', host, port)

    def stop_client_registry(self):
        self.service.cleanup()
        self.service_greenlet.kill()
        logger.info('Stopped client registry, connecting to %s:%s', _host, _port)

    def assertCorrectClientListReply(self, obj, payload):
        self.assertIn('clients', payload, msg=u"attribute 'clients' not in payload")
        d = None
        for dct in payload['clients']:
            self.assertIn('routing-id', dct, msg=u"attribute 'routing-id' not in list item in clients list")
            if dct['routing-id'] == self.routing_id:
                d = dct
                break

        self.assertIsNotNone(d, msg=u'Client not present in returned client listing')
        self.assertEquals(obj.metadata['client'], d['client'], msg=u"attribute 'client' not equal")
        self.assertEquals(obj.metadata['user'], d['user'], msg=u"attribute 'user' not equal")



class SingleServerTestCase(BaseTestCase):
    def setUp(self):
        global _host, _port

        self.server = self.start_server(_host, _port)
        logger.info('Started server at %s:%s', *(self.server.address[:2]))

    def tearDown(self):
        self.server.stop(timeout=0)
        logger.info('Stopped server at %s:%s', *(self.server.address[:2]))

class TwoServerTestCase(BaseTestCase):
    def setUp(self):
        super(TwoServerTestCase, self).setUp()

        global _host, _port, _port2
        self.server1 = self.start_server(_host, _port)
        logger.info('Started server at %s:%s', *(self.server1.address[:2]))

        self.server2 = self.start_server(_host, _port2,
                                         linked_servers=[(_host, _port)])
        logger.info('Started server at %s:%s', *(self.server2.address[:2]))

    def tearDown(self):
        self.server.stop(timeout=0)
        logger.info('Stopped server at %s:%s', *(self.server.address[:2]))

        super(TwoServerTestCase, self).tearDown()


class ConnectionTest(SingleServerTestCase):
    def test_server_accepts_connection(self):
        global _host, _port
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((_host, _port))
        sock.close()
        logger.info("Closed socket")

class SubscriptionTest(SingleServerTestCase):
    def setUp(self):
        super(SubscriptionTest, self).setUp()

        global _host, _port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((_host, _port))

    def tearDown(self):
        self.sock.close()

        super(SubscriptionTest, self).tearDown()

    def make_send_subscription(self):
        obj = BusinessObject({'event': 'routing/subscribe',
                              'receive-mode': 'all',
                              'types': 'all'}, None)
        obj.serialize(socket=self.sock)
        return obj

    def assertValidReceiveAllReply(self, reply):
        self.assertIsNotNone(reply)
        self.assertIn('routing-id', reply.metadata)
        self.assertIn('event', reply.metadata)
        self.assertEquals(reply.metadata['event'], 'routing/subscribe/reply')

    def test_receive_all_subscription(self):
        reply, time = reply_for_object(self.make_send_subscription(), self.sock)
        self.assertValidReceiveAllReply(reply)

    def test_receive_all_subscription_with_100ms_timeout(self):
        reply, time = reply_for_object(self.make_send_subscription(),
                                       self.sock, timeout_secs=0.1)
        self.assertValidReceiveAllReply(reply)

    def test_receive_all_subscription_with_10ms_timeout(self):
        reply, time = reply_for_object(self.make_send_subscription(),
                                       self.sock, timeout_secs=0.01)
        self.assertValidReceiveAllReply(reply)


class ClientRegistryTest(SingleServerTestCase):
    def setUp(self):
        super(ClientRegistryTest, self).setUp()

        self.start_client_registry(_host, _port) # TODO: multiple inheritance
        logger.info('Started client registry, connecting to %s:%s', _host, _port)

        global _host, _port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((_host, _port))
        obj = BusinessObject({'event': 'routing/subscribe',
                              'receive-mode': 'all',
                              'types': 'all'}, None)
        obj.serialize(socket=self.sock)
        resp, time = reply_for_object(obj, self.sock)
        self.routing_id = resp.metadata['routing-id']

    def tearDown(self):
        self.sock.close()

        self.stop_client_registry()
        logger.info('Stopped client registry')

        super(ClientRegistryTest, self).tearDown()

    def test_answers_to_service_call(self):
        list_obj = BusinessObject({'event': 'services/request',
                                   'name': 'clients',
                                   'request': 'list'}, None)
        list_obj.serialize(socket=self.sock)
        logger.info(self.server)
        logger.info(self.service)
        reply, time = reply_for_object(list_obj, self.sock)
        self.assertIsNotNone(reply)

    def test_correct_registration(self):
        obj = BusinessObject({'event': 'services/request',
                              'name': 'clients',
                              'request': 'join',
                              'client': str(uuid4()),
                              'user': str(uuid4())}, None)
        obj.serialize(socket=self.sock)

        list_obj = BusinessObject({'event': 'services/request',
                                   'name': 'clients',
                                   'request': 'list'}, None)
        list_obj.serialize(socket=self.sock)
        reply, time = reply_for_object(list_obj, self.sock)
        self.assertIsNotNone(reply, msg=u'No reply to service request')

        payload_text = reply.payload.decode('utf-8')
        payload = json.loads(payload_text)

        self.assertCorrectClientListReply(obj, payload)


def main():
    global _host, _port
    parser = OptionParser()
    parser.add_option("-d", "--debug", action="store_true", dest="debug", default=False,
                      help="logging level DEBUG")

    parser.add_option("--host", dest="host", default="localhost")
    parser.add_option("--port", dest="port", default=17890, type="int")
    parser.add_option("--port2", dest="port2", default=17891, type="int")

    opts, args = parser.parse_args()

    if opts.debug:
        logging.basicConfig(level=logging.DEBUG)
    logging.basicConfig(level=logging.INFO)

    _host = opts.host
    _port = opts.port
    _port2 = opts.port2

    unittest_main()

if __name__ == '__main__':
    main()