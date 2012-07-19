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

    @classmethod
    def promote(cls, instance, obj=None):
        if instance.__class__ == cls:
            return

        instance.__class__ = cls
        instance.routing_id = make_routing_id(registration_object=obj)
        instance.extra_routing_ids = []
        instance.receive_mode = "none"
        instance.types = "all"
        instance.service = None
        instance.subscribed = False
        instance.subscribed_to = False

def make_server_subscription(obj=None):
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

    def connect(self, client, clients):
        RoutedSystemClient.promote(client, None)

        if client.server:
            self.subscribe(None, client, clients)
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

        if obj.event == 'routing/subscribe':
            self.subscribe(obj, sender, clients)
        elif obj.event == 'services/register':
            return self.register_service(obj, sender, clients)
        else:
            return self.route(obj, sender, clients)

    def subscribe(self, obj, client, clients):
        if obj is None and client.server:
            subscription = make_server_subscription()
            subscription.metadata['routing-id'] = self.routing_id
            client.send(subscription, None)
            return

        client.extra_routing_ids = RoutingMiddleware.extra_routing_ids(obj)
        client.receive_mode = obj.metadata.get('receive-mode', obj.metadata.get('receive_mode', 'none'))
        client.types = obj.metadata.get('types', 'all')
        client.server = RoutingMiddleware.is_server(obj)
        client.subscribed = True

        notify = BusinessObject({ 'event': 'routing/subscribe/notify',
                                  'routing-id': client.routing_id }, None)
        # Send a registration reply
        if client.server:
            notify.metadata['role'] = 'server'
            subscription = make_server_subscription()
            subscription.metadata['routing-id'] = self.routing_id
            if not client.subscribed_to:
                client.send(subscription, None)
                client.subscribed_to = True
            client.send(BusinessObject({ 'event': 'routing/subscribe/reply',
                                         'routing-id': client.routing_id,
                                         'in-reply-to': obj.id,
                                         'role': 'server' }, None), None)
            logger.info(u"Server {0} subscribed!".format(client))
        else:
            client.send(BusinessObject({ 'event': 'routing/subscribe/reply',
                                         'routing-id': client.routing_id,
                                         'in-reply-to': obj.id }, None), None)
            logger.info(u"Client {0} subscribed!".format(client))

        for c in clients:
            if c != client:
                c.send(notify, None)


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

    def register_service(self, obj, client, clients):
        if client.subscribed is False:
            client.send(BusinessObject({ 'event': 'services/register/reply',
                                         'error': 'routing subscription required' }, None), None)
            logger.warning(u"Client {0} tried to register as a service without a routing subscription!".format(client))
            return

        if 'route' in obj.metadata:
            routing_id = obj.metadata['route'][0]
            if len(route) > 1:
                return
        else:
            routing_id = client.routing_id

        if 'name' not in obj.metadata:
            logger.warning(u"services/register without 'name' from {0}".format(client))
            client.send(BusinessObject({ 'event': 'services/register/reply',
                                         'error': 'no name specified',
                                         'in-reply-to': obj.id,
                                         'to': routing_id }, None), None)

        client.service = obj.metadata['name']
        client.send(BusinessObject({ 'event': 'services/register/reply',
                                     'in-reply-to': obj.id,
                                     'to': routing_id }, None), None)

        logger.info(u"Service {0} registered".format(client))
        return self.route(BusinessObject({ 'event': 'services/register/notify',
                                           'name': client.service }, None), None, clients)


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
        if not recipient.subscribed:
            return False, 'recipient not yet subscribed'

        if 'route' in obj.metadata:
            if isinstance(recipient, RoutedSystemClient) and \
                   recipient.routing_id in obj.metadata['route']:
                return False, 'recipient.routing_id in route'

            if isinstance(sender, RoutedSystemClient) and \
                   obj.event is not None and obj.event.startswith('routing/'):
                if sender.routing_id in obj.metadata['route']:
                    return False, 'routing/* and sender.routing_id in route'
                

        if recipient.server:
            return True, 'recipient is server'

        if 'to' in obj.metadata:
            if not recipient.has_routing_id(obj.metadata['to']):
                return False, "recipient doesn't have routing id for to field"

        receive_mode = recipient.receive_mode
        types = recipient.types

        reason = []
        should = None

        if receive_mode == "none":
            should = False
            reason.append('receive_mode is none')

        elif receive_mode == "no_echo":
            if sender is recipient:
                should = False
                reason.append('receive_mode is no_echo and sender is recipient')
            else:
                should = True
                reason.append("receive_mode is no_echo and sender isn't recipient")

        elif receive_mode == "events_only":
            if obj.event is not None:
                should = True
                reason.append("receive_mode is events_only and this is an event")
            else:
                should = False
                reason.append("receive_mode is events_only and this is not an event")

        elif receive_mode == "all":
            reason.append("receive_mode is all")
            should = True

        if should:
            if types == "none":
                should = False
                reason.append("types is none")
            elif types == "all":
                should = True
                reason.append("types is all")

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

        logger.info(u"Legacy client {0} subscribed!".format(client))

        if client.receive_mode != "none" and client.types != "none":
            client.send(BusinessObject({ 'event': 'clients/register/reply',
                                         'routing-id': sender.routing_id }, None), None)

        notify = BusinessObject({ 'event': 'routing/subscribe/notify',
                                  'routing-id': client.routing_id }, None)
        for c in clients:
            if c != client:
                c.send(notify, None)

        return BusinessObject({ 'event': 'services/request',
                                'name': 'clients',
                                'request': 'join',
                                'client': obj.metadata.get('name', 'no-client'),
                                'user': obj.metadata.get('user', 'no-user'),
                                'route': obj.metadata.get('route', []) }, None)
