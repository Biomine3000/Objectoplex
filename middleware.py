# -*- coding: utf-8 -*-
import hashlib
import logging

from datetime import datetime
from collections import defaultdict

from system import BusinessObject

logger = logging.getLogger('middleware')

class Middleware(object):
    def handle(self, message, client, clients):
        """
        Return None to signify that this message is handled and doesn't need
        to be passed on to the next middleware.
        """
        return message

    def register(self, client):
        pass

    def unregister(self, client):
        pass


class ChecksumMiddleware(Middleware):
    def handle(self, message, *args):
        if 'sha1' not in message.metadata and message.size > 0:
            message.metadata['sha1'] = hashlib.sha1(message.payload).hexdigest()
        return message

class MultiplexingMiddleware(Middleware):
    def handle(self, message, sender, clients):
        for client in clients:
            client.send(message, sender)
        return None

class StatisticsMiddleware(Middleware):
    def __init__(self):
        self.received_objects = 0
        self.clients_connected_total = 0
        self.clients_disconnected_total = 0
        self.messages_by_type = defaultdict(int)
        self.events_by_type = defaultdict(int)
        self.started = datetime.now()

    def handle(self, message, sender, clients):
        self.received_objects += 1

        if message.content_type is not None:
            self.messages_by_type[str(message.content_type)] += 1
        else:
            self.messages_by_type[""] += 1

        if message.event is not None:
            self.events_by_type[str(message.event)] += 1

        if message.event == 'services/request' and \
               message.metadata.get('name', None) == 'statistics':
            self.send_statistics(sender, message.id)
            return None

        return message

    def register(self, client):
        self.clients_connected_total += 1

    def unregister(self, client):
        self.clients_disconnected_total += 1

    def send_statistics(self, client, original_id):
        metadata = {
            'event': 'services/reply',
            'in-reply-to': original_id,
            'statistics': {
                'received objects': self.received_objects,
                'clients connected total': self.clients_connected_total,
                'clients disconnected total': self.clients_disconnected_total,
                'messages by type': self.messages_by_type,
                'events by type': self.events_by_type
                }
            }
        client.send(BusinessObject(metadata, None), None)
