# -*- coding: utf-8 -*-
from __future__ import print_function

import hashlib
import logging
import json

from datetime import datetime, timedelta
from collections import defaultdict
from uuid import uuid4
from os import environ as env
from random import choice

from system import BusinessObject
from server import SystemClient
from rule_engine import routing_decision

logger = logging.getLogger('middleware')


class Middleware(object):
    def handle(self, message, client, clients):
        """
        Return None to signify that this message is handled and doesn't need
        to be passed on to the next middleware.
        """
        return message

    def periodical(self, clients):
        """
        This method is called periodically (interval could be seconds, could
        be minutes).  Middleware mustn't expect to be called at certain given
        intervals; server implementation might change.
        """
        pass

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
        self.average_send_queue_length = 0

    def handle(self, obj, sender, clients):
        self.received_objects += 1

        if obj.content_type is not None:
            self.objects_by_type[str(obj.content_type)] += 1
        else:
            self.objects_by_type[""] += 1

        if obj.event is not None:
            self.events_by_type[str(obj.event)] += 1

        if obj.event == 'server/statistics':
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
        statistics = {
            'received objects': self.received_objects,
            'clients connected total': self.clients_connected_total,
            'clients disconnected total': self.clients_disconnected_total,
            'objects by type': self.objects_by_type,
            'events by type': self.events_by_type,
            'client count': self.client_count,
            'bytes in': self.bytes_in,
            'average send queue length': self.average_send_queue_length,
            }
        payload = bytearray(json.dumps(statistics, ensure_ascii=False), encoding='utf-8')

        metadata = {
            'event': 'server/statistics/reply',
            'in-reply-to': original_id,
            'size': len(payload),
            'type': 'text/json'
            }

        client.send(BusinessObject(metadata, payload), None)


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
    def has_routing_id(self, routing_id_or_list):
        if isinstance(routing_id_or_list, basestring):
            return self._check_routing_id(routing_id_or_list)
        else:
            for routing_id in routing_id_or_list:
                if self._check_routing_id(routing_id):
                    return True
            return False

    def _check_routing_id(self, routing_id):
        if self.routing_id == routing_id:
            return True
        elif routing_id in self.extra_routing_ids:
            return True
        return False

    def send(self, message, sender):
        if self.queue.full():
            self.queue.get()
            logger.warning(u"{0} send queue is full, dropped oldest item!".format(self))

        super(RoutedSystemClient, self).send(message, sender)

    @classmethod
    def promote(cls, instance, obj=None):
        if instance.__class__ == cls:
            return

        instance.__class__ = cls
        instance.routing_id = make_routing_id(registration_object=obj)
        instance.extra_routing_ids = []
        instance.echo = False
        instance.subscription = []
        instance.subscribed = False
        instance.subscribed_to = False

def make_server_subscription(routing_id):
    metadata = {'event': 'routing/subscribe',
                'role': 'server',
                'routing-id': routing_id,
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
        self.last_announcement = datetime.now()

    def connect(self, client, clients):
        RoutedSystemClient.promote(client, None)

        if client.server:
            self.subscribe_to_server(client)
            logger.info(u"Server {0} connected!".format(client))
        else:
            logger.info(u"Client {0} connected!".format(client))

    def disconnect(self, client, clients):
        assert(client.__class__ == RoutedSystemClient)

        if client.server:
            logger.info(u"Server {0} disconnected!".format(client))
        else:
            logger.info(u"Client {0} disconnected!".format(client))

        if client.subscribed:
            self.route(BusinessObject({ 'event': 'routing/disconnect',
                                        'routing-id': client.routing_id }, None), None, clients)

    def handle(self, obj, sender, clients):
        assert(sender.__class__ == RoutedSystemClient)

        if obj.event == 'routing/subscribe' and RoutingMiddleware.is_server(obj):
            self.handle_server_subscription(obj, sender, clients)
        elif obj.event == 'routing/subscribe':
            self.handle_client_subscription(obj, sender, clients)
        else:
            return self.route(obj, sender, clients)

    def periodical(self, clients):
        now = datetime.now()
        if now > self.last_announcement + timedelta(minutes=5):
            self.last_announcement = now
            self.route(self.neighbor_announcement(clients), None, clients)

    def neighbor_announcement(self, clients):
        logger.debug("Sending neighbor announcement")
        metadata = { 'event': 'routing/announcement/neighbors',
                     'node': self.routing_id,
                     'neighbors': [{ 'routing-id': client.routing_id }
                                   for client in clients] }
        obj = BusinessObject(metadata, None)

        return obj

    def subscribe_to_server(self, client):
        if client.subscribed_to:
            return
        subscription = make_server_subscription(self.routing_id)
        client.send(subscription, None)
        client.subscribed_to = True
        logger.info(u"Subscribed to server {0}".format(client))

    def handle_server_subscription(self, obj, client, clients):
        client.routing_id = obj.metadata['routing-id']
        client.extra_routing_ids = RoutingMiddleware.extra_routing_ids(obj)
        client.subscriptions = obj.metadata.get('subscriptions', [])
        client.echo = False
        client.subscribed = True
        client.server = True

        self.subscribe_to_server(client)

        client.send(BusinessObject({ 'event': 'routing/subscribe/reply',
                                     'routing-id': client.routing_id,
                                     'in-reply-to': obj.id,
                                     'role': 'server' }, None), None)

        notification = BusinessObject({ 'event': 'routing/subscribe/notification',
                                  'routing-id': client.routing_id,
                                  'role': 'server' }, None)

        for c in clients:
            if c != client:
                c.send(notification, None)

        self.route(self.neighbor_announcement(clients), None, clients)
        logger.info(u"Server {0} subscribed!".format(client))

    def handle_client_subscription(self, obj, client, clients):
        client.extra_routing_ids = RoutingMiddleware.extra_routing_ids(obj)
        client.subscriptions = obj.metadata.get('subscriptions', [])
        client.echo = False
        client.server = False
        client.subscribed = True

        notification = BusinessObject({ 'event': 'routing/subscribe/notification',
                                        'routing-id': client.routing_id }, None)
        # Send a registration reply
        client.send(BusinessObject({ 'event': 'routing/subscribe/reply',
                                     'routing-id': client.routing_id,
                                     'in-reply-to': obj.id }, None), None)

        for c in clients:
            if c != client:
                c.send(notification, None)

        self.route(self.neighbor_announcement(clients), None, clients)
        logger.info(u"Client {0} subscribed!".format(client))

    @classmethod
    def is_server(cls, obj):
        if 'role' in obj.metadata and obj.metadata['role'] == 'server':
            if 'route' not in obj.metadata:
                return True
            elif len(obj.metadata['route']) == 1:
                return True
        return False

    @classmethod
    def extra_routing_ids(cls, obj):
        extra_routing_ids = []
        if 'routing-ids' in obj.metadata:
            routing_ids = obj.metadata['routing-ids']
            if isinstance(routing_ids, basestring):
                logger.error(u"Got {0} as routing-ids from {1}".format(routing_ids, client))
            else:
                for routing_id in routing_ids:
                    extra_routing_ids.append(routing_id)
        return extra_routing_ids

    def route(self, obj, sender, clients):
        if sender is not None and not sender.subscribed:
            logger.warning("Dropped {0}, {1} not subscribed!".format(obj, sender))
            return

        if 'route' in obj.metadata:
            route = obj.metadata['route']
        else:
            route = []
            obj.metadata['route'] = route

        if self.routing_id in route:
            return False

        if len(route) == 0 and sender is not None:
            route.append(sender.routing_id)
        route.append(self.routing_id)

        for recipient in clients:
            # print('---')
            # print(obj.metadata)
            # print(self.should_route_to(obj, sender, recipient), recipient, recipient.routing_id)
            # print('---')

            if self.should_route_to(obj, sender, recipient)[0]:
                recipient.send(obj, sender)

    def should_route_to(self, obj, sender, recipient):
        if not recipient.subscribed:
            return False, 'recipient not yet subscribed'

        if 'route' in obj.metadata:
            if isinstance(recipient, RoutedSystemClient) and \
                   recipient.routing_id in obj.metadata['route'] and \
                   len(obj.metadata['route']) > 2:
                return False, 'recipient.routing_id in route'

            if isinstance(sender, RoutedSystemClient) and \
                   obj.event is not None and obj.event.startswith('routing/'):
                if sender.routing_id in obj.metadata['route']:
                    return False, 'routing/* and sender.routing_id in route'

        if recipient.server:
            return True, 'recipient is server'

        if obj.metadata.get('event', '').startswith('routing/announcement/'):
            return False, 'not routing routing announcements to non-servers'

        if 'to' in obj.metadata:
            if not recipient.has_routing_id(obj.metadata['to']):
                return False, "recipient doesn't have routing id for to field"

        if sender is recipient and recipient.echo is True:
            return False, "echo false"

        decision = routing_decision(obj, recipient.subscriptions)
        return decision, "decision made by rule_engine.routing_decision"


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
        if obj.event == 'routing/subscribe' and \
           ('receive-mode' in obj.metadata or 'receive_mode' in obj.metadata):
            return self.handle_legacy_subscription(obj, sender, clients)

        if obj.event == 'clients/register':
            return self.handle_legacy_registration(obj, sender, clients)
        return obj

    def handle_legacy_registration(self, obj, sender, clients):
        client = sender
        RoutedSystemClient.promote(client, obj=obj)

        if 'route' in obj.metadata and len(obj.metadata['route']) > 1:
            return obj

        if 'routing-ids' in obj.metadata:
            routing_ids = obj.metadata['routing-ids']
            if isinstance(routing_ids, basestring):
                logger.error(u"Got {0} as routing-ids from {1}".format(routing_ids, client))
            else:
                for routing_id in routing_ids:
                    client.extra_routing_ids.append(routing_id)

        client.receive_mode = obj.metadata.get('receive', 'all')
        client.types = obj.metadata.get('subscriptions', 'all')
        client.subscribed = True
        client.legacy = True

        logger.info(u"Legacy client {0} subscribed (registered)!".format(client))

        if client.receive_mode != "none" and client.types != "none":
            client.send(BusinessObject({ 'event': 'clients/register/reply',
                                         'routing-id': sender.routing_id }, None), None)

        notification = BusinessObject({ 'event': 'routing/subscribe/notification',
                                  'routing-id': client.routing_id }, None)
        for c in clients:
            if c != client:
                c.send(notification, None)

        return BusinessObject({ 'event': 'services/request',
                                'name': 'clients',
                                'request': 'join',
                                'client': obj.metadata.get('name', 'no-client'),
                                'user': obj.metadata.get('user', 'no-user'),
                                'route': obj.metadata.get('route', []) }, None)

    def handle_legacy_subscription(self, obj, sender, clients):
        client = sender
        RoutedSystemClient.promote(client, obj=obj)

        if 'route' in obj.metadata and len(obj.metadata['route']) > 1:
            return obj

        if 'routing-ids' in obj.metadata:
            routing_ids = obj.metadata['routing-ids']
            if isinstance(routing_ids, basestring):
                logger.error(u"Got {0} as routing-ids from {1}".format(routing_ids, client))
            else:
                for routing_id in routing_ids:
                    client.extra_routing_ids.append(routing_id)

        # receive-mode handling
        receive_mode = obj.metadata.get('receive-mode', obj.metadata.get('receive_mode', 'none'))

        if receive_mode == "no_echo":
            client.echo = False
        else:
            client.echo = True

        client.subscriptions = []
        if receive_mode == "events_only":
            client.subscriptions = ['@*']
        else:
            client.subscriptions = ['*']

        client.legacy = True
        client.subscribed = True

        logger.info(u"Legacy client {0} subscribed!".format(client))

        if receive_mode != "none" and obj.metadata['types'] != "none":
            client.send(BusinessObject({ 'event': 'routing/subscribe/reply',
                                         'routing-id': client.routing_id,
                                         'in-reply-to': obj.id }, None), None)

        notification = BusinessObject({ 'event': 'routing/subscribe/notification',
                                        'routing-id': client.routing_id }, None)

        for c in clients:
            if c != client:
                c.send(notification, None)

        return None


class PingPongMiddleware(Middleware):
    def handle(self, obj, sender, *args, **kwargs):
        if obj.event == 'ping' and isinstance(sender, RoutedSystemClient) and \
           sender.subscribed:
            sender.send(BusinessObject({ 'event': 'pong',
                                         'routing-id': sender.routing_id,
                                         'in-reply-to': obj.id }, None), None)
        else:
            return obj
