# -*- coding: utf-8 -*-
from system import BusinessObject
from service import Service


class Client(object):
    def __init__(self, obj):
        self.name = obj.metadata.get('name', 'no-name')
        self.user = obj.metadata.get('user', 'no-user')
        self.routing_id = None
        if 'route' in obj.metadata:
            self.routing_id = obj.metadata['route'][0]

    def __unicode__(self):
        return u'{0}-{1}'.format(self.name, self.user)

    def __str__(self):
        return unicode(self).encode('ASCII', 'backslashreplace')

    def __repr__(self):
        return '<%s %s (routing-id: %s)>' % (self.__class__.__name__, str(self), self.routing_id)

class ClientRegistry(Service):
    __service__ = 'client_registry'

    def __init__(self, *args, **kwargs):
        super(ClientRegistry, self).__init__(*args, **kwargs)
        self.clients = []

    def should_handle(self, obj):
        if obj.event is not None and \
               obj.event.startswith('clients/'):
            return True

        return False

    def client_for_sender(self, obj):
        if 'route' in obj.metadata:
            sender = obj.metadata['route'][0]
            for client in self.clients:
                if client.routing_id is not None and \
                       client.routing_id == sender:
                    return client

    def handle_list(self, obj):
        requesting_client = self.client_for_sender(obj)

        if requesting_client is None:
            self.logger.warning(u"Couldn't find client object for {0}".format(obj.metadata))
            return None

        others = []
        for client in self.clients:
            if requesting_client is not None and client != requesting_client:
                others.append(unicode(client))
            elif requesting_client is None:
                others.append(unicode(client))

        metadata = { 'event': 'clients/list/reply',
                     'others': others,
                     'in-reply-to': obj.id,
                     'to': requesting_client.routing_id,
                     'you': unicode(requesting_client) }

        return BusinessObject(metadata, None)

    def handle(self, obj):
        _clients, event = obj.event.split('/', 1)

        if event == 'register':
            client = Client(obj)
            self.clients.append(client)
            self.logger.info(u"{0} registered!".format(repr(client)))
            return self.handle_list(obj)
        elif event == 'list':
            return self.handle_list(obj)

service = ClientRegistry


# "clients/list/reply"      - reply listing clients connected to the server; server is free to send this 
#                            also when not requested, e.g. when a client connects to or parts from the server.
#     • "others": names of other clients connected to the server
#     • "you": name of the receiving client, as seen by the server
