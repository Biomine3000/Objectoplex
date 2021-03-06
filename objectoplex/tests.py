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
from utils import reply_for_object, read_object_with_timeout

logger = logging.getLogger("tests")


class BaseTestCase(TestCase):
    def start_server(self, host, port, linked_servers=[]):
        result = ObjectoPlex((host, port),
                             middlewares=[
                                 PingPongMiddleware(),
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
        self.server1.stop(timeout=0)
        logger.info('Stopped server at %s:%s', *(self.server1.address[:2]))
        self.server2.stop(timeout=0)
        logger.info('Stopped server at %s:%s', *(self.server2.address[:2]))

        super(TwoServerTestCase, self).tearDown()


class ConnectionTest(SingleServerTestCase):
    def test_server_accepts_connection(self):
        global _host, _port
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((_host, _port))
        sock.close()
        logger.info("Closed socket")

class SubscriptionTestCase(SingleServerTestCase):
    def setUp(self):
        super(SubscriptionTestCase, self).setUp()

        global _host, _port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((_host, _port))

    def tearDown(self):
        self.sock.close()

        super(SubscriptionTestCase, self).tearDown()

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
        reply, time = reply_for_object(self.make_send_subscription(), self.sock, select=select)
        self.assertValidReceiveAllReply(reply)

    def test_receive_all_subscription_with_100ms_timeout(self):
        reply, time = reply_for_object(self.make_send_subscription(),
                                       self.sock, timeout_secs=0.1, select=select)
        self.assertValidReceiveAllReply(reply)

    def test_receive_all_subscription_with_10ms_timeout(self):
        reply, time = reply_for_object(self.make_send_subscription(),
                                       self.sock, timeout_secs=0.01, select=select)
        self.assertValidReceiveAllReply(reply)


class ClientRegistryTestCase(SingleServerTestCase):
    def setUp(self):
        super(ClientRegistryTestCase, self).setUp()

        self.start_client_registry(_host, _port) # TODO: multiple inheritance
        logger.info('Started client registry, connecting to %s:%s', _host, _port)

        global _host, _port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((_host, _port))
        obj = BusinessObject({'event': 'routing/subscribe',
                              'receive-mode': 'all',
                              'types': 'all'}, None)
        obj.serialize(socket=self.sock)
        resp, time = reply_for_object(obj, self.sock, select=select)
        self.routing_id = resp.metadata['routing-id']

    def tearDown(self):
        self.sock.close()

        self.stop_client_registry()
        logger.info('Stopped client registry')

        super(ClientRegistryTestCase, self).tearDown()

    def test_answers_to_service_call(self):
        list_obj = BusinessObject({'event': 'services/request',
                                   'name': 'clients',
                                   'request': 'list'}, None)
        list_obj.serialize(socket=self.sock)
        logger.info(self.server)
        logger.info(self.service)
        reply, time = reply_for_object(list_obj, self.sock, select=select)
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
        reply, time = reply_for_object(list_obj, self.sock, select=select)
        self.assertIsNotNone(reply, msg=u'No reply to service request')

        payload_text = reply.payload.decode('utf-8')
        payload = json.loads(payload_text)

        self.assertCorrectClientListReply(obj, payload)


class RecipientBaseTestCase(object):
    def assert_receives_object(self, sock, id):
        reply = None
        while reply is None or reply.event != None:
            reply = read_object_with_timeout(sock, select=select)

        self.assertIsNotNone(reply)
        self.assertEquals(reply.id, id)


    def make_send_subscription(self, sock, no_echo=False):
        metadata = {'event': 'routing/subscribe',
                    'receive-mode': 'all',
                    'types': 'all'}

        if no_echo:
            metadata['receive-mode'] = 'no_echo'

        obj = BusinessObject(metadata, None)
        obj.serialize(socket=sock)
        return obj


class RecipientTestCase(SingleServerTestCase, RecipientBaseTestCase):
    def setUp(self):
        super(RecipientTestCase, self).setUp()
        self.clients = []

        for i in xrange(4):
            self.clients.append(self.make_subscribe_client())

    def tearDown(self):
        for sock, routing_id in self.clients:
            sock.close()

        super(RecipientTestCase, self).tearDown()

    def test_server_delivers_to_sender(self):
        obj = BusinessObject({}, None)
        obj.serialize(socket=self.clients[0][0])
        self.assert_receives_object(self.clients[0][0], obj.id)

    def test_server_doesnt_deliver_when_no_echo(self):
        client, routing_id = self.make_subscribe_client(no_echo=True)

        obj = BusinessObject({}, None)
        obj.serialize(socket=client)

        reply = read_object_with_timeout(client, timeout_secs=0.1, select=select)
        self.assertIsNone(reply)
        client.close()

    def test_server_delivers_to_all(self):
        obj = BusinessObject({}, None)
        obj.serialize(socket=self.clients[0][0])

        for sock, routing_id in self.clients:
            self.assert_receives_object(sock, obj.id)

    def test_server_delivers_to_specified_recipient(self):
        client, routing_id = self.clients[0]
        to_client, to_routing_id = self.clients[1]

        obj = BusinessObject({'to': to_routing_id}, None)
        obj.serialize(socket=client)

        self.assert_receives_object(to_client, obj.id)

        for client, routing_id in self.clients[2:]:
            reply = read_object_with_timeout(client, timeout_secs=0.1, select=select)
            if reply is not None:
                self.assertIsNotNone(reply.event)
            while reply is not None and reply.event is not None:
                reply = read_object_with_timeout(client, timeout_secs=0.1, select=select)

            self.assertIsNone(reply)

    def test_server_delivers_to_multiple_recipients(self):
        client, routing_id = self.clients[0]
        to_client, to_routing_id = self.clients[1]
        to_client2, to_routing_id2 = self.clients[2]

        obj = BusinessObject({'to': [to_routing_id, to_routing_id2]}, None)
        obj.serialize(socket=client)

        self.assert_receives_object(to_client, obj.id)
        self.assert_receives_object(to_client2, obj.id)

        for client, routing_id in self.clients[2:]:
            reply = read_object_with_timeout(client, timeout_secs=0.1, select=select)
            if reply is not None:
                self.assertIsNotNone(reply.event)
            while reply is not None and reply.event is not None:
                reply = read_object_with_timeout(client, timeout_secs=0.1, select=select)

            self.assertIsNone(reply)

    def make_subscribe_client(self, no_echo=False):
        global _host, _port
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((_host, _port))

        subscription = self.make_send_subscription(sock, no_echo=no_echo)
        resp, time = reply_for_object(subscription, sock, select=select)
        routing_id = resp.metadata['routing-id']

        return sock, routing_id


class RecipientTwoServerTestCase(TwoServerTestCase, RecipientBaseTestCase):
    def setUp(self):
        super(RecipientTwoServerTestCase, self).setUp()
        self.clients = []

        self.clients.append(self.make_subscribe_client(self.server1))
        self.clients.append(self.make_subscribe_client(self.server1))
        self.clients.append(self.make_subscribe_client(self.server2))
        self.clients.append(self.make_subscribe_client(self.server2))

    def tearDown(self):
        for sock, routing_id in self.clients:
            sock.close()

        super(RecipientTwoServerTestCase, self).tearDown()

    def test_clients_on_same_server_receive(self):
        client, routing_id = self.clients[0]
        to_client, to_routing_id = self.clients[1]

        obj = BusinessObject({'to': to_routing_id}, None)
        obj.serialize(socket=client)

        self.assert_receives_object(to_client, obj.id)

    def test_clients_on_other_server_receive(self):
        client, routing_id = self.clients[0]
        to_client, to_routing_id = self.clients[2]

        obj = BusinessObject({'to': to_routing_id}, None)
        obj.serialize(socket=client)

        self.assert_receives_object(to_client, obj.id)

    def make_subscribe_client(self, server, no_echo=False):
        host, port = server.address[:2]
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))

        subscription = self.make_send_subscription(sock, no_echo=no_echo)
        resp, time = reply_for_object(subscription, sock, select=select)
        routing_id = resp.metadata['routing-id']

        return sock, routing_id


def main():
    global _host, _port, _port2
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
