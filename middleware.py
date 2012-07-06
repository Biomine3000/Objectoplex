# -*- coding: utf-8 -*-
from __future__ import print_function

import hashlib
import logging

from datetime import datetime
from collections import defaultdict
from uuid import uuid4

from system import BusinessObject

logger = logging.getLogger('middleware')


class Middleware(object):
    def handle(self, message, client, clients):
        """
        Return None to signify that this message is handled and doesn't need
        to be passed on to the next middleware.
        """
        return message

    def connect(self, client):
        pass

    def disconnect(self, client):
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

        return obj

    def connect(self, client):
        self.clients_connected_total += 1

    def disconnect(self, client):
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
                'bytes in': self.bytes_in
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


class RoutingMiddleware(Middleware):
    def __init__(self):
        self.routing_id = self.make_routing_id() # routing id of the server
        self.routing_ids = {} # client => routing-id mapping
        self.clients = {} # routing-id => client mapping
        self.routing_information = {} # client => routing information dict; subscriptions, receive etc
        self.extra_routing_ids = {} # client => [extra-routing-id] mapping

    def handle(self, obj, sender, clients):
        # {"name": "BiomineTV", "subscriptions": "all", "receive": "no_echo",
        #  "event": "clients/register", "user": "gua",
        #  "id": "<20120705155613.14376.94125@localhost>", "size": 0}
        if obj.event == 'clients/register':
            self.register(obj, sender)
            return

        return self.route(obj, sender, clients)

    def connect(self, client):
        self.register(None, client)

    def disconnect(self, client):
        # TODO: refactor method protocol to allow for object sending here
        # result = self.make_client_part_notification(client)
        routing_id = self.routing_ids[client]
        del self.routing_ids[client]
        del self.clients[routing_id]

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
            route.append(self.routing_ids[sender])

        route.append(self.routing_id)

        for recipient in clients:
            if self.should_route_to(obj, sender, recipient):
                recipient.send(obj, sender)

    def should_route_to(self, obj, sender, recipient):
        receive = self.routing_information[recipient]['receive']
        subscriptions = self.routing_information[recipient]['subscriptions']

        should = None

        if receive == "none":
            should = True
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
                if self.has_routing_id(recipient, to):
                    should = True
                else:
                    should = False
            should = True

        if 'subscriptions' == "none":
            should = False
        elif 'subscriptions' == "all":
            should = True

        return should

    def has_routing_id(self, client, routing_id):
        if self.routing_ids[client] == routing_id:
            return True
        elif routing_id in self.extra_routing_ids[client]:
            return True
        return False

    def register(self, obj, client):
        """
        Implements registration of clients' routing options.
        """
        if obj is not None:
            routing_id = self.routing_id_from(obj)
        else:
            routing_id = self.make_routing_id()

        self.routing_ids[client] = routing_id
        self.clients[routing_id] = client

        self.routing_information[client] = {}
        self.extra_routing_ids[client] = []

        self.routing_information[client]['receive'] = 'routed'
        self.routing_information[client]['subscriptions'] = 'all'

        if obj is None:
            logger.info(u"Client {0} registered".format(client))
            return

        self.routing_information[client]['receive'] = obj.metadata.get('receive', 'all')
        self.routing_information[client]['subscriptions'] = obj.metadata.get('subscriptions', 'all')

        if 'routing-ids' in obj.metadata:
            routing_ids = obj.metadata['routing-ids']
            if isinstance(routing_ids, basestring):
                logger.error(u"Got {0} as routing-ids from {1}".format(routing_ids, client))
            else:
                for routing_id in routing_ids:
                    self.extra_routing_ids[client].append(routing_id)

        # Send a registration reply
        client.send(self.make_registration_reply(client, obj, routing_id), None)
        logger.info(u"Client {0} registered".format(client))
        return self.make_registration_notification(client, obj, routing_id)

    def make_registration_reply(self, client, obj, routing_id):
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

    def make_registration_notification(self, client, obj, routing_id):
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

    def make_client_part_notification(self, client):
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
            payload = bytearray(u'{0} parted!'.format(client.socket.getpeername()), encoding='utf-8')
            metadata['size'] = len(payload)
            metadata['type'] = 'text/plain; charset=UTF-8'

        return BusinessObject(metadata, payload)

    def routing_id_from(self, obj):
        return obj.metadata.get('routing-id',
                                    obj.metadata.get('unique-routing-id',
                                                         self.make_routing_id()))

    def make_routing_id(self):
        return str(uuid4())


class MOTDMiddleware(Middleware):
    def __init__(self, text):
        self.payload = bytearray(text, encoding='utf-8')

    def connect(self, client):
        client.send(BusinessObject({'type': 'text/plain; charset=UTF-8',
                                    'size': len(self.payload),
                                    'sender': 'pyabboe'},
                                   self.payload), None)
