# -*- coding: utf-8 -*-
from __future__ import print_function

import hashlib
import logging

from datetime import datetime
from collections import defaultdict
from uuid import uuid4

from system import BusinessObject
from server import SystemClient

logger = logging.getLogger('middleware')


class Middleware(object):
    def handle(self, message, client, clients):
        """
        Return None to signify that this message is handled and doesn't need
        to be passed on to the next middleware.
        """
        return message

    def connect(self, client, clients):
        pass

    def disconnect(self, client, clients):
        pass


class ChecksumMiddleware(Middleware):
    def handle(self, obj, *args):
        if 'sha1' not in obj.metadata and obj.size > 0:
            obj.metadata['sha1'] = hashlib.sha1(obj.payload).hexdigest()
        return obj


class MultiplexingMiddleware(Middleware):
    def handle(self, obj, sender, clients):
        for client in clients:
            client.send(obj, sender)
        return None


class StatisticsMiddleware(Middleware):
    def __init__(self):
        self.received_objects = 0
        self.client_count = 0
        self.bytes_in = 0
        self.clients_connected_total = 0
        self.clients_disconnected_total = 0
        self.objects_by_type = defaultdict(int)
        self.events_by_type = defaultdict(int)
        self.started = datetime.now()
        self.average_queue = 0

    def handle(self, obj, sender, clients):
        self.received_objects += 1

        if obj.content_type is not None:
            self.objects_by_type[str(obj.content_type)] += 1
        else:
            self.objects_by_type[""] += 1

        if obj.event is not None:
            self.events_by_type[str(obj.event)] += 1

        if obj.event == 'services/request' and \
               obj.metadata.get('name', None) == 'statistics':
            self.send_statistics(sender, obj.id)
            return None

        self.client_count = len(clients)
        self.bytes_in += len(obj.serialize())

        queue_length_sum = 0
        for client in clients:
            queue_length_sum += client.queue.qsize()
        self.average_send_queue_length = float(queue_length_sum) / float(len(clients))

        return obj

    def connect(self, client, clients):
        self.clients_connected_total += 1

    def disconnect(self, client, clients):
        self.clients_disconnected_total += 1

    def send_statistics(self, client, original_id):
        metadata = {
            'event': 'services/reply',
            'in-reply-to': original_id,
            'statistics': {
                'received objects': self.received_objects,
                'clients connected total': self.clients_connected_total,
                'clients disconnected total': self.clients_disconnected_total,
                'objects by type': self.objects_by_type,
                'events by type': self.events_by_type,
                'client count': self.client_count,
                'bytes in': self.bytes_in,
                'average send queue length': self.average_send_queue_length,
                }
            }
        client.send(BusinessObject(metadata, None), None)


class StdErrMiddleware(Middleware):
    def __init__(self):
        from sys import stderr
        from codecs import getwriter
        self.out = getwriter('utf-8')(stderr)

    def handle(self, obj, sender, clients):
        print(u'## From: {0}'.format(sender), file=self.out)
        print(u'  ', end='', file=self.out)
        obj.serialize(file=self.out)
        print('\n##', file=self.out)
        return obj


class RoutedSystemClient(SystemClient):
    # {"name": "BiomineTV", "subscriptions": "all", "receive": "no_echo",
    #  "event": "clients/register", "user": "gua",
    #  "id": "<20120705155613.14376.94125@localhost>", "size": 0}
    def has_routing_id(self, routing_id):
        if self.routing_id == routing_id:
            return True
        elif routing_id in self.extra_routing_ids:
            return True
        return False

    def send(self, message, sender):
        if self.queue.qsize() > 100:
            self.queue.get()
            logger.warning(u"{0} send queue size is 100!".format(self))

        super(RoutedSystemClient, self).send(message, sender)


def make_registration_reply(client, obj, routing_id):
    payload = None
    metadata = {
        'event': 'clients/register/reply',
        'routing-id': routing_id
        }

    if 'name' in obj.metadata and 'user' in obj.metadata:
        payload = bytearray(u'Welcome, {0}-{1}'.format(obj.metadata['name'],
                                                       obj.metadata['user']), encoding='utf-8')
        metadata['size'] = len(payload)
        metadata['type'] = 'text/plain; charset=UTF-8'
        
    return BusinessObject(metadata, payload)

def promote_to_routed_system_client(client, obj):
    client.__class__ = RoutedSystemClient

    client.routing_id = make_routing_id(registration_object=obj)
    client.extra_routing_ids = []
    client.receive = "routed"
    client.subscriptions = "all"

    if obj is None:
        logger.info(u"Client {0} registered".format(client))
        return

    client.receive = obj.metadata.get('receive', 'routed')
    client.subscriptions = obj.metadata.get('subscriptions', 'all')

    if 'routing-ids' in obj.metadata:
        routing_ids = obj.metadata['routing-ids']
        if isinstance(routing_ids, basestring):
            logger.error(u"Got {0} as routing-ids from {1}".format(routing_ids, client))
        else:
            for routing_id in routing_ids:
                client.extra_routing_ids.append(routing_id)

    # Send a registration reply
    client.send(make_registration_reply(client, obj, client.routing_id), None)
    logger.info(u"Client {0} registered".format(client))
    return make_registration_notification(client, obj, client.routing_id)

def make_registration_notification(client, obj, routing_id):
    payload = None
    metadata = {
        'event': 'clients/register/notify',
        'routing-id': routing_id
        }

    if 'name' in obj.metadata:
        metadata['name'] = obj.metadata['name']
    if 'user' in obj.metadata:
        metadata['user'] = obj.metadata['user']

    if 'name' in obj.metadata and 'user' in obj.metadata:
        payload = bytearray(u'{0}-{1} joined!'.format(obj.metadata['name'],
                                                      obj.metadata['user']), encoding='utf-8')
        metadata['size'] = len(payload)
        metadata['type'] = 'text/plain; charset=UTF-8'

    return BusinessObject(metadata, payload)

def make_routing_id(registration_object=None):
    if registration_object:
        obj = registration_object
        return obj.metadata.get('routing-id',
                                obj.metadata.get('unique-routing-id',
                                                 make_routing_id()))

    return str(uuid4())


class RoutingMiddleware(Middleware):
    def __init__(self):
        self.routing_id = make_routing_id() # routing id of the server

    def handle(self, obj, sender, clients):
        if obj.event == 'clients/register':
            self.register(obj, sender)

        return self.route(obj, sender, clients)

    def connect(self, client, clients):
        self.register(None, client)

    def disconnect(self, client, clients):
        # TODO: notify of disconnect
        pass

    def route(self, obj, sender, clients):
        if 'route' in obj.metadata:
            route = obj.metadata['route']
        else:
            route = []
            obj.metadata['route'] = route

        if len(route) > 0:
            if self.routing_id in route:
                logger.info("Dropping object %s; loop!" % obj)
                return None
        elif len(route) == 0:
            route.append(sender.routing_id)

        route.append(self.routing_id)

        for recipient in clients:
            if self.should_route_to(obj, sender, recipient):
                recipient.send(obj, sender)

    def should_route_to(self, obj, sender, recipient):
        receive = recipient.receive
        subscriptions = recipient.subscriptions

        should = None

        if receive == "none":
            should = False
        elif receive == "no_echo":
            if sender is recipient:
                should = False
            else:
                should = True
        elif receive == "events_only":
            if obj.event is not None:
                should = True
            else:
                should = False
        elif receive == "all":
            should = True
        elif receive == "routed":
            if 'to' in obj.metadata:
                if recipient.has_routing_id(to):
                    should = True
                else:
                    should = False
            should = True

        if receive:
            if 'subscriptions' == "none":
                should = False
            elif 'subscriptions' == "all":
                should = True

        return should

    def register(self, obj, client):
        """
        Implements registration of clients' routing options.
        """
        promote_to_routed_system_client(client, obj)



class MOTDMiddleware(Middleware):
    def __init__(self, text):
        self.payload = bytearray(text, encoding='utf-8')

    def connect(self, client, clients):
        client.send(BusinessObject({'type': 'text/plain; charset=UTF-8',
                                    'size': len(self.payload),
                                    'sender': 'pyabboe'},
                                   self.payload), None)
