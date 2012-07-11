# -*- coding: utf-8 -*-
from __future__ import print_function

import hashlib
import logging

from datetime import datetime
from collections import defaultdict
from uuid import uuid4
from os import environ as env
from random import choice

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


def promote_to_routed_system_client(client, obj):
    client.__class__ = RoutedSystemClient

    client.routing_id = make_routing_id(registration_object=obj)
    client.extra_routing_ids = []
    client.receive = "all"
    client.subscriptions = "all"
    client.service = None

def make_server_subscription(obj):
    metadata = {'event': 'routing/subscribe',
                'role': 'server',
                'routing-id': make_routing_id(obj),
                'receive': 'all',
                'subscriptions': 'all',
                'name': 'Objectoplex',
                'user': env['USER']
                }
    return BusinessObject(metadata, None)



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
        if obj.event == 'routing/subscribe':
            self.register(obj, sender, clients)
        elif obj.event == 'services/register':
            self.register(obj, sender, clients)

        if not isinstance(sender, RoutedSystemClient):
            self.register(obj, sender, clients)

        return self.route(obj, sender, clients)

    def connect(self, client, clients):
        self.route(self.register(None, client, clients), client, clients)

    def disconnect(self, client, clients):
        if isinstance(client, RoutedSystemClient):
            self.route(BusinessObject({ 'event': 'routing/disconnect',
                                        'routing-id': client.routing_id }, None), client, clients)

    def register(self, obj, client, clients):
        if not isinstance(client, RoutedSystemClient):
            promote_to_routed_system_client(client, obj)

        if obj is not None:
            return self.register_with_message(obj, client, clients)
        else:
            return self.register_without_message(client, clients)

    def register_with_message(self, obj, client, clients):
        if obj.event == 'routing/subscribe':
            return self.register_client(obj, client, clients)
        elif obj.event == 'services/register':
            return self.register_service(obj, client, clients)

    def register_client(self, obj, client, clients):
        if 'routing-ids' in obj.metadata:
            routing_ids = obj.metadata['routing-ids']
            if isinstance(routing_ids, basestring):
                logger.error(u"Got {0} as routing-ids from {1}".format(routing_ids, client))
            else:
                for routing_id in routing_ids:
                    client.extra_routing_ids.append(routing_id)

        client.receive = obj.metadata.get('receive_mode', 'all')
        client.subscriptions = obj.metadata.get('types', 'all')

        if 'role' in obj.metadata and obj.metadata['role'] == 'server':
            if 'route' in obj.metadata and len(route) == 1 or \
                   'route' not in obj.metadata:
                client.server = True
            return obj

        # Send a registration reply
        if client.server:
            client.send(make_server_registration(obj), None)
            logger.info(u"Server {0} registered".format(client))
            return obj
        else:
            client.send(make_registration_reply(client, obj, client.routing_id), None)
            logger.info(u"Client {0} registered".format(client))
            return BusinessObject({ 'event': 'routing/subscribe/reply',
                                    'routing-id': routing_id }, payload)

    def register_service(self, obj, client, clients):
        if 'name' not in obj.metadata:
            logger.warning(u"services/register without 'name' from {0}".format(client))
        client.service = obj.metadata['name']

        if 'route' in obj.metadata:
            routing_id = obj.metadata['route'][0]
        else:
            routing_id = client.routing_id

        logger.info(u"Service {0} registered".format(client))

    def register_without_message(self, client, clients):
        if client.server:
            logger.info(u"Server {0} registered".format(client))
        else:
            logger.info(u"Client {0} registered".format(client))

        return BusinessObject({ 'event': 'routing/connect',
                                'routing-id': client.routing_id }, None)

    def route(self, obj, sender, clients):
        if 'route' in obj.metadata:
            route = obj.metadata['route']
        else:
            route = []
            obj.metadata['route'] = route

        if self.routing_id in route:
            return False

        if len(route) == 0:
            route.append(sender.routing_id)
        route.append(self.routing_id)

        if obj.event == 'services/request':
            status, reason = self.route_service_request(obj, sender, clients)
            # print('---')
            # print(obj.metadata)
            # print(status)
            # print(reason)
            # print('---')
            if status is True:
                return

        for recipient in clients:
            # print('---')
            # print(obj.metadata)
            # print(self.should_route_to(obj, sender, recipient), recipient)
            # print('---')
            if self.should_route_to(obj, sender, recipient)[0]:
                recipient.send(obj, sender)

    def route_service_request(self, obj, sender, clients):
        service_name = obj.metadata.get('name', None)

        if not isinstance(service_name, basestring):
            return False, 'no proper service name specified in object'

        possible_targets = [client
                            for client in clients
                            if client.service == service_name]

        try:
            target = choice(possible_targets)
        except IndexError, ie:
            target = None

        if target is None:
            return False, u'no service targets found for {0}'.format(service_name)

        target.send(obj, sender)
        return True, 'found target'

    def should_route_to(self, obj, sender, recipient):
        if 'route' in obj.metadata:
            if isinstance(recipient, RoutedSystemClient) and \
                   recipient.routing_id in obj.metadata['route']:
                return False, 'recipient.routing_id in route'

        if recipient.server:
            return True, 'recipient is server'

        if 'to' in obj.metadata:
            if not recipient.has_routing_id(obj.metadata['to']):
                return False, "recipient doesn't have routing id for to field"

        receive = recipient.receive
        subscriptions = recipient.subscriptions

        reason = []
        should = None

        if receive == "none":
            should = False
            reason.append('receive is none')

        elif receive == "no_echo":
            if sender is recipient:
                should = False
                reason.append('receive is no_echo and sender is recipient')
            else:
                should = True
                reason.append("receive is no_echo and sender isn't recipient")

        elif receive == "events_only":
            if obj.event is not None:
                should = True
                reason.append("receive is events_only and this is an event")
            else:
                should = False
                reason.append("receive is events_only and this is not an event")

        elif receive == "all":
            reason.append("receive is all")
            should = True

        if should:
            if 'subscriptions' == "none":
                should = False
                reason.append("subscriptions is none")
            elif 'subscriptions' == "all":
                should = True
                reason.append("subscriptions is all")

        return should, '; '.join(reason)


class MOTDMiddleware(Middleware):
    def __init__(self, text):
        self.payload = bytearray(text, encoding='utf-8')

    def connect(self, client, clients):
        client.send(BusinessObject({'type': 'text/plain; charset=UTF-8',
                                    'size': len(self.payload),
                                    'sender': 'pyabboe'},
                                   self.payload), None)


class LegacySubscriptionMiddleware(Middleware):
    def handle(self, obj, sender, clients):
        if obj.event != 'clients/register':
            return obj

        self.handle_legacy_registration(obj, sender, clients)

    def handle_legacy_registration(self, obj, sender, clients):
        client = sender

        if not isinstance(client, RoutedSystemClient):
            promote_to_routed_system_client(client, obj)

        if 'route' in obj.metadata and len(obj.metadata['route']) > 1:
            return obj

        if 'routing-ids' in obj.metadata:
            routing_ids = obj.metadata['routing-ids']
            if isinstance(routing_ids, basestring):
                logger.error(u"Got {0} as routing-ids from {1}".format(routing_ids, client))
            else:
                for routing_id in routing_ids:
                    client.extra_routing_ids.append(routing_id)

        client.receive = obj.metadata.get('receive', 'all')
        client.subscriptions = obj.metadata.get('subscriptions', 'all')

        logger.info(u"Legacy client {0} registered".format(client))

        if client.receive != "none" and client.subscriptions != "none":
            client.send(BusinessObject({ 'event': 'clients/register/reply',
                                         'routing-id': sender.routing_id }, None), None)

        return BusinessObject({ 'event': 'services/request',
                                'name': 'clients',
                                'request': 'join',
                                'client': obj.metadata.get('name', 'no-client'),
                                'user': obj.metadata.get('user', 'no-user') }, None)
