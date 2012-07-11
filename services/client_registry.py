# -*- coding: utf-8 -*-
from system import BusinessObject
from service import Service


class Client(object):
    def __init__(self, obj):
        self.client = obj.metadata.get('client', 'no-client')
        self.user = obj.metadata.get('user', 'no-user')
        self.routing_id = None
        if 'route' in obj.metadata:
            self.routing_id = obj.metadata['route'][0]

    def __unicode__(self):
        return u'{0}-{1}'.format(self.client, self.user)

    def __str__(self):
        return unicode(self).encode('ASCII', 'backslashreplace')

    def __repr__(self):
        return '<%s %s (routing-id: %s)>' % (self.__class__.__name__, str(self), self.routing_id)

class ClientRegistry(Service):
    __service__ = 'clients'

    def __init__(self, *args, **kwargs):
        super(ClientRegistry, self).__init__(*args, **kwargs)
        self.clients = []

    def client_for_sender(self, obj):
        if 'route' in obj.metadata:
            sender = obj.metadata['route'][0]
            for client in self.clients:
                if client.routing_id is not None and \
                       client.routing_id == sender:
                    return client

    def handle_list(self, obj):
        requesting_client = self.client_for_sender(obj)

        clients = []
        for client in self.clients:
            clients.append({ 'user': client.user,
                             'client': client.client,
                             'routing-id': client.routing_id })

        metadata = { 'event': 'services/reply',
                     'clients': clients,
                     'in-reply-to': obj.id,
                     'to': requesting_client.routing_id }

        return BusinessObject(metadata, None)

    def remove_client(self, obj):
        if 'routing-id' not in obj.metadata:
            self.logger.warning(u"Received leave with no routing-id: {0}".format(obj.metadata))
            return

        removable = None
        for client in self.clients:
            if client.routing_id == obj.metadata['routing-id']:
                removable = client

        if removable is not None:
            self.clients.remove(removable)

        self.logger.info(u"{0} removed from registry!".format(removable))

    def add_client(self, obj):
        client = Client(obj)
        self.clients.append(client)
        self.logger.info(u"{0} registered!".format(repr(client)))

    def handle_connect(self, obj):
        self.add_client(obj)

    def handle_subscribe(self, obj):
        self.add_client(obj)

    def handle_disconnect(self, obj):
        self.remove_client(obj)

    def handle(self, obj):
        if obj.event == 'routing/connect':
            return self.handle_connect(obj)
        elif obj.event == 'routing/subscribe':
            return self.handle_subscribe(obj)
        elif obj.event == 'routing/disconnect':
            return self.handle_disconnect(obj)

        if 'request' not in obj.metadata:
            return

        request = obj.metadata['request']

        if request == 'join':
            self.add_client(obj)
        elif request == 'leave':
            self.remove_client(obj)
        elif request == 'list':
            return self.handle_list(obj)

    def should_handle(self, obj):
        if obj.event == 'routing/connect' or \
           obj.event == 'routing/subscribe' or \
           obj.event == 'routing/disconnect':
            return True
        elif obj.event == 'services/request' and \
                 obj.metadata.get('name', None) == self.__class__.__service__:
            return True

        return False
                      

service = ClientRegistry


# "clients/list/reply"      - reply listing clients connected to the server; server is free to send this 
#                            also when not requested, e.g. when a client connects to or parts from the server.
#     • "others": names of other clients connected to the server
#     • "you": name of the receiving client, as seen by the server
